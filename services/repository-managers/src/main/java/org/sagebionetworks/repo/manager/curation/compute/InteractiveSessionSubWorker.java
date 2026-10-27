package org.sagebionetworks.repo.manager.curation.compute;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.repo.manager.EntityManager;
import org.sagebionetworks.repo.manager.UserManager;
import org.sagebionetworks.repo.manager.curation.compute.session.LaunchedSession;
import org.sagebionetworks.repo.manager.curation.compute.session.SessionInput;
import org.sagebionetworks.repo.manager.curation.compute.session.SessionLaunchRequest;
import org.sagebionetworks.repo.manager.curation.compute.session.SessionLauncher;
import org.sagebionetworks.repo.manager.curation.compute.session.SessionOutput;
import org.sagebionetworks.repo.manager.entity.EntityAuthorizationManager;
import org.sagebionetworks.repo.manager.entity.decider.UsersEntityAccessInfo;
import org.sagebionetworks.repo.manager.file.FileHandleManager;
import org.sagebionetworks.repo.manager.file.FileHandleUrlRequest;
import org.sagebionetworks.repo.model.ACCESS_TYPE;
import org.sagebionetworks.repo.model.EntityChildrenRequest;
import org.sagebionetworks.repo.model.EntityChildrenResponse;
import org.sagebionetworks.repo.model.EntityHeader;
import org.sagebionetworks.repo.model.EntityType;
import org.sagebionetworks.repo.model.UnauthorizedException;
import org.sagebionetworks.repo.model.UserInfo;
import org.sagebionetworks.repo.model.curation.CurationTask;
import org.sagebionetworks.repo.model.curation.TaskState;
import org.sagebionetworks.repo.model.curation.TaskStatus;
import org.sagebionetworks.repo.model.curation.execution.InteractiveSessionExecutionDetails;
import org.sagebionetworks.repo.model.curation.execution.InteractiveSessionExecutionProperties;
import org.sagebionetworks.repo.model.curation.execution.InteractiveSessionState;
import org.sagebionetworks.repo.model.curation.execution.SessionInputFile;
import org.sagebionetworks.repo.model.curation.execution.SessionOutputFile;
import org.sagebionetworks.repo.model.dao.asynch.AsyncJobProgressCallback;
import org.sagebionetworks.repo.model.dbo.curation.CurationTaskDao;
import org.sagebionetworks.repo.model.file.FileHandleAssociateType;
import org.sagebionetworks.repo.model.jdo.KeyFactory;
import org.sagebionetworks.util.ValidateArgument;
import org.springframework.stereotype.Service;

/**
 * Starts and ends the network-isolated session of an interactive session task.
 * <p>
 * Starting copies the task's inputs into the session read-only: the direct-child files of its input
 * Folder, and the approved outputs of the tasks it chains from. Every input must be downloadable by the
 * user starting the session, and its pre-signed URL is issued through the same check as a download. A
 * session that shares another task's /workspace only starts if everyone who has run a session in that
 * workspace can download every input of every session in it, since a file copied into /workspace is
 * visible to all of them.
 * <p>
 * The session keeps running after {@link #execute} returns, so the task stays EXECUTING until
 * {@link #end} stops it and collects what it wrote to /outputs for review.
 */
@Service
public class InteractiveSessionSubWorker implements ComputeTaskSubWorker<InteractiveSessionExecutionDetails> {

	private static final Logger LOG = LogManager.getLogger(InteractiveSessionSubWorker.class);

	/**
	 * Every input is downloaded into the session before it starts, within the start job, so their number
	 * is bounded.
	 */
	public static final int MAX_INPUT_FILES = 100;

	public static final long MAX_DURATION_MINUTES = 720L;

	static final String FOLDER_INPUT_PREFIX = "files/";
	static final String TASK_INPUT_PREFIX = "tasks/";

	private static final long TASK_PAGE_SIZE = 100L;

	private final CurationTaskDao curationTaskDao;
	private final EntityManager entityManager;
	private final FileHandleManager fileHandleManager;
	private final EntityAuthorizationManager entityAuthorizationManager;
	private final UserManager userManager;
	private final SessionLauncher sessionLauncher;

	public InteractiveSessionSubWorker(CurationTaskDao curationTaskDao, EntityManager entityManager,
			FileHandleManager fileHandleManager, EntityAuthorizationManager entityAuthorizationManager,
			UserManager userManager, SessionLauncher sessionLauncher) {
		this.curationTaskDao = curationTaskDao;
		this.entityManager = entityManager;
		this.fileHandleManager = fileHandleManager;
		this.entityAuthorizationManager = entityAuthorizationManager;
		this.userManager = userManager;
		this.sessionLauncher = sessionLauncher;
	}

	@Override
	public Class<InteractiveSessionExecutionDetails> getExecutionDetailsType() {
		return InteractiveSessionExecutionDetails.class;
	}

	@Override
	public InteractiveSessionExecutionDetails execute(UserInfo user, CurationTask task,
			InteractiveSessionExecutionDetails details, AsyncJobProgressCallback callback) {
		InteractiveSessionExecutionProperties properties = getProperties(task);
		ValidateArgument.required(properties.getEnvironmentId(), "environmentId");
		validateMaxDurationMinutes(properties.getMaxDurationMinutes());

		callback.updateProgress("Checking the session's inputs", 0L, 100L);
		List<PlannedInput> planned = planInputs(user, task, properties);
		Workspace workspace = resolveWorkspace(task, properties);
		checkEveryoneInWorkspaceCanDownload(user, workspace, planned);
		List<StagedInput> staged = planned.stream().map(input -> stage(user, input)).toList();

		callback.updateProgress("Starting the session", 50L, 100L);
		LaunchedSession launched = sessionLauncher.start(new SessionLaunchRequest(task.getTaskId(), workspace.id(),
				properties.getEnvironmentId(), user.getId(), properties.getMaxDurationMinutes(),
				staged.stream().map(StagedInput::launchInput).toList()));
		LOG.info("Started session {} for task {} (user {}, workspace {}, {} inputs)", launched.sessionId(),
				task.getTaskId(), user.getId(), workspace.id(), staged.size());

		return details.setSessionId(launched.sessionId())
				.setSessionState(InteractiveSessionState.RUNNING)
				.setWorkspaceId(workspace.id())
				.setEndsOn(launched.endsOn())
				.setInputFiles(staged.stream().map(StagedInput::file).toList())
				.setOutputFiles(null)
				.setOutputFolderId(null);
	}

	@Override
	public boolean isComplete(InteractiveSessionExecutionDetails details) {
		return details.getSessionState() != InteractiveSessionState.RUNNING;
	}

	@Override
	public InteractiveSessionExecutionDetails end(UserInfo user, CurationTask task,
			InteractiveSessionExecutionDetails details, AsyncJobProgressCallback callback) {
		callback.updateProgress("Stopping the session and collecting its outputs", 0L, 100L);
		List<SessionOutput> outputs = sessionLauncher.stop(details.getSessionId());
		LOG.info("Ended session {} of task {} (user {}): {} outputs for review", details.getSessionId(),
				task.getTaskId(), user.getId(), outputs.size());
		return details.setSessionState(InteractiveSessionState.ENDED)
				.setOutputFiles(outputs.stream().map(output -> new SessionOutputFile().setPath(output.path())
						.setContentSize(output.contentSize()).setContentMd5(output.contentMd5())).toList());
	}

	/**
	 * Validates a session's maximum duration: required, and between 1 and {@link #MAX_DURATION_MINUTES}.
	 */
	public static void validateMaxDurationMinutes(Long maxDurationMinutes) {
		ValidateArgument.required(maxDurationMinutes, "maxDurationMinutes");
		ValidateArgument.requirement(maxDurationMinutes >= 1 && maxDurationMinutes <= MAX_DURATION_MINUTES,
				"maxDurationMinutes must be between 1 and " + MAX_DURATION_MINUTES + ".");
	}

	record PlannedInput(EntityHeader file, String path, Long sourceTaskId) {
	}

	record StagedInput(SessionInputFile file, String url) {
		SessionInput launchInput() {
			return new SessionInput(file.getPath(), url);
		}
	}

	/**
	 * @param id            The ID of the workspace: the ID of the task that owns it.
	 * @param otherTaskIds  The other interactive session tasks that mount it.
	 */
	record Workspace(String id, List<Long> otherTaskIds) {
	}

	static InteractiveSessionExecutionProperties getProperties(CurationTask task) {
		if (!(task.getTaskProperties() instanceof InteractiveSessionExecutionProperties properties)) {
			throw new IllegalArgumentException(
					"Task " + task.getTaskId() + " must have InteractiveSessionExecutionProperties.");
		}
		return properties;
	}

	List<PlannedInput> planInputs(UserInfo user, CurationTask task, InteractiveSessionExecutionProperties properties) {
		List<PlannedInput> inputs = new ArrayList<>();
		if (properties.getInputFolderId() != null) {
			for (EntityHeader file : listFiles(user, properties.getInputFolderId())) {
				inputs.add(new PlannedInput(file, FOLDER_INPUT_PREFIX + file.getName(), null));
			}
		}
		if (properties.getInputTaskIds() != null) {
			for (Long inputTaskId : properties.getInputTaskIds()) {
				for (EntityHeader file : listFiles(user, getApprovedOutputFolderId(inputTaskId))) {
					inputs.add(new PlannedInput(file, TASK_INPUT_PREFIX + inputTaskId + "/" + file.getName(), inputTaskId));
				}
			}
		}
		ValidateArgument.requirement(inputs.size() <= MAX_INPUT_FILES, "The session has " + inputs.size()
				+ " input files, which exceeds the maximum of " + MAX_INPUT_FILES + ".");
		return inputs;
	}

	List<EntityHeader> listFiles(UserInfo user, String folderId) {
		List<EntityHeader> files = new ArrayList<>();
		String nextPageToken = null;
		do {
			EntityChildrenResponse page = entityManager.getChildren(user, new EntityChildrenRequest()
					.setParentId(folderId)
					.setIncludeTypes(List.of(EntityType.file))
					.setNextPageToken(nextPageToken));
			files.addAll(page.getPage());
			// Stop listing as soon as the limit is passed; planInputs reports it.
			if (files.size() > MAX_INPUT_FILES) {
				break;
			}
			nextPageToken = page.getNextPageToken();
		} while (nextPageToken != null);
		return files;
	}

	String getApprovedOutputFolderId(Long inputTaskId) {
		TaskStatus status = curationTaskDao.getTaskStatus(inputTaskId);
		if (status.getState() == TaskState.COMPLETED
				&& status.getExecutionDetails() instanceof InteractiveSessionExecutionDetails details
				&& details.getOutputFolderId() != null) {
			return details.getOutputFolderId();
		}
		throw new IllegalArgumentException("Input task " + inputTaskId + " has no approved outputs yet.");
	}

	Workspace resolveWorkspace(CurationTask task, InteractiveSessionExecutionProperties properties) {
		Long ownerTaskId = properties.getWorkspaceTaskId() == null ? task.getTaskId() : properties.getWorkspaceTaskId();
		if (!ownerTaskId.equals(task.getTaskId())) {
			CurationTask owner = curationTaskDao.getCurationTask(ownerTaskId).orElseThrow(
					() -> new IllegalArgumentException("The workspaceTaskId task does not exist: " + ownerTaskId));
			ValidateArgument.requirement(
					owner.getTaskProperties() instanceof InteractiveSessionExecutionProperties ownerProperties
							&& ownerProperties.getWorkspaceTaskId() == null,
					"The workspaceTaskId must reference an interactive session task that does not itself share another task's workspace.");
		}
		List<Long> otherTaskIds = new ArrayList<>();
		for (CurationTask other : listProjectTasks(task.getProjectId())) {
			if (other.getTaskId().equals(task.getTaskId())) {
				continue;
			}
			if (other.getTaskProperties() instanceof InteractiveSessionExecutionProperties otherProperties
					&& (other.getTaskId().equals(ownerTaskId) || ownerTaskId.equals(otherProperties.getWorkspaceTaskId()))) {
				otherTaskIds.add(other.getTaskId());
			}
		}
		return new Workspace(ownerTaskId.toString(), otherTaskIds);
	}

	List<CurationTask> listProjectTasks(String projectId) {
		Long projectKey = KeyFactory.stringToKey(projectId);
		List<CurationTask> tasks = new ArrayList<>();
		List<CurationTask> page;
		long offset = 0;
		do {
			page = curationTaskDao.getCurationTasks(projectKey, TASK_PAGE_SIZE, offset);
			tasks.addAll(page);
			offset += TASK_PAGE_SIZE;
		} while (page.size() == TASK_PAGE_SIZE);
		return tasks;
	}

	void checkEveryoneInWorkspaceCanDownload(UserInfo user, Workspace workspace, List<PlannedInput> planned) {
		// Files from any session that mounted the workspace may have been copied into it, so the people
		// who have worked in it and the inputs of their sessions are pooled.
		Set<Long> participantIds = new LinkedHashSet<>();
		participantIds.add(user.getId());
		Set<Long> entityIds = new LinkedHashSet<>();
		planned.forEach(input -> entityIds.add(KeyFactory.stringToKey(input.file().getId())));
		for (Long otherTaskId : workspace.otherTaskIds()) {
			if (curationTaskDao.getTaskStatus(otherTaskId).getExecutionDetails() instanceof InteractiveSessionExecutionDetails other
					&& other.getSessionId() != null) {
				participantIds.add(Long.parseLong(other.getStartedBy()));
				if (other.getInputFiles() != null) {
					other.getInputFiles().forEach(file -> entityIds.add(KeyFactory.stringToKey(file.getEntityId())));
				}
			}
		}
		if (entityIds.isEmpty()) {
			return;
		}
		List<String> denials = new ArrayList<>();
		for (Long participantId : participantIds) {
			UserInfo participant = participantId.equals(user.getId()) ? user : userManager.getUserInfo(participantId);
			for (UsersEntityAccessInfo info : entityAuthorizationManager.batchHasAccess(participant,
					new ArrayList<>(entityIds), ACCESS_TYPE.DOWNLOAD)) {
				if (!info.getAuthorizationStatus().isAuthorized()) {
					denials.add((participantId.equals(user.getId()) ? "you" : "user " + participantId)
							+ " cannot download " + KeyFactory.keyToString(info.getEntityId()));
				}
			}
		}
		if (!denials.isEmpty()) {
			throw new UnauthorizedException("The session cannot start: everyone working in workspace "
					+ workspace.id() + " must be able to download every input in it, and " + String.join("; ", denials)
					+ ".");
		}
	}

	StagedInput stage(UserInfo user, PlannedInput input) {
		EntityHeader file = input.file();
		String fileHandleId = entityManager.getFileHandleIdForVersion(user, file.getId(), file.getVersionNumber());
		String url = fileHandleManager.getRedirectURLForFileHandle(new FileHandleUrlRequest(user, fileHandleId)
				.withAssociation(FileHandleAssociateType.FileEntity, file.getId()));
		return new StagedInput(new SessionInputFile().setEntityId(file.getId()).setVersionNumber(file.getVersionNumber())
				.setPath(input.path()).setSourceTaskId(input.sourceTaskId()), url);
	}
}
