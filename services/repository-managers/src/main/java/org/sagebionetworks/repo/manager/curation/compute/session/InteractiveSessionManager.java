package org.sagebionetworks.repo.manager.curation.compute.session;

import java.util.List;

import org.sagebionetworks.repo.manager.curation.CurationTaskManager;
import org.sagebionetworks.repo.manager.entity.EntityAuthorizationManager;
import org.sagebionetworks.repo.manager.entity.decider.UsersEntityAccessInfo;
import org.sagebionetworks.repo.model.ACCESS_TYPE;
import org.sagebionetworks.repo.model.AuthorizationUtils;
import org.sagebionetworks.repo.model.UnauthorizedException;
import org.sagebionetworks.repo.model.UserInfo;
import org.sagebionetworks.repo.model.curation.TaskState;
import org.sagebionetworks.repo.model.curation.TaskStatus;
import org.sagebionetworks.repo.model.curation.execution.InteractiveSessionAccess;
import org.sagebionetworks.repo.model.curation.execution.InteractiveSessionExecutionDetails;
import org.sagebionetworks.repo.model.curation.execution.InteractiveSessionState;
import org.sagebionetworks.repo.model.curation.execution.ListSessionEnvironmentsResponse;
import org.sagebionetworks.repo.model.curation.execution.SessionInputFile;
import org.sagebionetworks.repo.model.jdo.KeyFactory;
import org.sagebionetworks.util.ValidateArgument;
import org.springframework.stereotype.Service;

/**
 * The parts of interactive session tasks that are not task execution jobs: the approved environments a
 * session can run in, and links that open a running session.
 */
@Service
public class InteractiveSessionManager {

	private final CurationTaskManager curationTaskManager;
	private final EntityAuthorizationManager entityAuthorizationManager;
	private final SessionLauncher sessionLauncher;

	public InteractiveSessionManager(CurationTaskManager curationTaskManager,
			EntityAuthorizationManager entityAuthorizationManager, SessionLauncher sessionLauncher) {
		this.curationTaskManager = curationTaskManager;
		this.entityAuthorizationManager = entityAuthorizationManager;
		this.sessionLauncher = sessionLauncher;
	}

	/**
	 * @return The approved environments a session can run in; empty when this stack offers no sessions.
	 */
	public ListSessionEnvironmentsResponse listEnvironments(UserInfo user) {
		ValidateArgument.required(user, "user");
		AuthorizationUtils.disallowAnonymous(user);
		return new ListSessionEnvironmentsResponse().setEnvironments(sessionLauncher.listEnvironments());
	}

	/**
	 * Issue a single-use link that opens the running session of the given task. Only the user who started
	 * the session can open it, and only while they can still download every one of its inputs.
	 */
	public InteractiveSessionAccess openSession(UserInfo user, Long taskId) {
		ValidateArgument.required(user, "user");
		ValidateArgument.required(taskId, "taskId");
		curationTaskManager.getCurationTask(user, taskId);
		TaskStatus status = curationTaskManager.getTaskStatus(user, taskId);

		if (!(status.getExecutionDetails() instanceof InteractiveSessionExecutionDetails details)
				|| status.getState() != TaskState.EXECUTING
				|| details.getSessionState() != InteractiveSessionState.RUNNING
				|| details.getAsyncJobId() != null) {
			throw new IllegalArgumentException("Task " + taskId + " has no running session to open.");
		}
		if (!user.getId().toString().equals(details.getStartedBy())) {
			throw new UnauthorizedException("Only the person who started this session can open it.");
		}
		checkCanStillDownload(user, details.getInputFiles());

		SessionAccessGrant grant = sessionLauncher.grantAccess(details.getSessionId(), user.getId());
		return new InteractiveSessionAccess().setSessionUrl(grant.url()).setExpiresOn(grant.expiresOn());
	}

	void checkCanStillDownload(UserInfo user, List<SessionInputFile> inputFiles) {
		if (inputFiles == null || inputFiles.isEmpty()) {
			return;
		}
		List<Long> entityIds = inputFiles.stream().map(file -> KeyFactory.stringToKey(file.getEntityId())).distinct()
				.toList();
		List<String> denied = entityAuthorizationManager.batchHasAccess(user, entityIds, ACCESS_TYPE.DOWNLOAD)
				.stream().filter(info -> !info.getAuthorizationStatus().isAuthorized())
				.map(UsersEntityAccessInfo::getEntityId).map(KeyFactory::keyToString).toList();
		if (!denied.isEmpty()) {
			throw new UnauthorizedException("You can no longer download " + String.join(", ", denied)
					+ ", which this session uses, so it cannot be opened.");
		}
	}
}
