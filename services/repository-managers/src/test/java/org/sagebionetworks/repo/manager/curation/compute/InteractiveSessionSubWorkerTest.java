package org.sagebionetworks.repo.manager.curation.compute;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
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
import org.sagebionetworks.repo.model.AuthorizationConstants;
import org.sagebionetworks.repo.model.EntityChildrenRequest;
import org.sagebionetworks.repo.model.EntityChildrenResponse;
import org.sagebionetworks.repo.model.EntityHeader;
import org.sagebionetworks.repo.model.EntityType;
import org.sagebionetworks.repo.model.UnauthorizedException;
import org.sagebionetworks.repo.model.UserInfo;
import org.sagebionetworks.repo.model.auth.AuthorizationStatus;
import org.sagebionetworks.repo.model.curation.CurationTask;
import org.sagebionetworks.repo.model.curation.TaskState;
import org.sagebionetworks.repo.model.curation.TaskStatus;
import org.sagebionetworks.repo.model.curation.execution.InteractiveSessionExecutionDetails;
import org.sagebionetworks.repo.model.curation.execution.InteractiveSessionExecutionProperties;
import org.sagebionetworks.repo.model.curation.execution.InteractiveSessionState;
import org.sagebionetworks.repo.model.curation.execution.SampleSheetGenerationExecutionProperties;
import org.sagebionetworks.repo.model.curation.execution.SessionInputFile;
import org.sagebionetworks.repo.model.curation.execution.SessionOutputFile;
import org.sagebionetworks.repo.model.dao.asynch.AsyncJobProgressCallback;
import org.sagebionetworks.repo.model.dbo.curation.CurationTaskDao;
import org.sagebionetworks.repo.model.file.FileHandleAssociateType;

@ExtendWith(MockitoExtension.class)
public class InteractiveSessionSubWorkerTest {

	@Mock
	private CurationTaskDao mockCurationTaskDao;
	@Mock
	private EntityManager mockEntityManager;
	@Mock
	private FileHandleManager mockFileHandleManager;
	@Mock
	private EntityAuthorizationManager mockEntityAuthorizationManager;
	@Mock
	private UserManager mockUserManager;
	@Mock
	private SessionLauncher mockSessionLauncher;
	@Mock
	private AsyncJobProgressCallback mockCallback;

	private InteractiveSessionSubWorker subWorker;

	private UserInfo user;
	private UserInfo collaborator;
	private Long userId = 101L;
	private Long collaboratorId = 202L;
	private Long taskId = 987L;
	private Long chainedTaskId = 986L;
	private Long ownerTaskId = 985L;
	private String projectId = "syn100";
	private String inputFolderId = "syn200";
	private String chainedOutputFolderId = "syn300";
	private Date endsOn = new Date(1_800_000_000_000L);

	@BeforeEach
	public void setup() {
		user = new UserInfo(false, userId, AuthorizationConstants.DEFAULT_REALM_ID, new HashSet<>(Set.of(userId)));
		collaborator = new UserInfo(false, collaboratorId, AuthorizationConstants.DEFAULT_REALM_ID,
				new HashSet<>(Set.of(collaboratorId)));
		subWorker = new InteractiveSessionSubWorker(mockCurationTaskDao, mockEntityManager, mockFileHandleManager,
				mockEntityAuthorizationManager, mockUserManager, mockSessionLauncher);
	}

	private InteractiveSessionExecutionProperties properties() {
		return new InteractiveSessionExecutionProperties().setInputFolderId(inputFolderId)
				.setEnvironmentId("python-3.12").setMaxDurationMinutes(120L);
	}

	private CurationTask task(Long id, InteractiveSessionExecutionProperties properties) {
		return new CurationTask().setTaskId(id).setProjectId(projectId).setTaskProperties(properties);
	}

	private EntityHeader file(String id, String name, Long version) {
		return new EntityHeader().setId(id).setName(name).setVersionNumber(version);
	}

	private void setupFolder(String folderId, List<EntityHeader> files) {
		when(mockEntityManager.getChildren(user, new EntityChildrenRequest().setParentId(folderId)
				.setIncludeTypes(List.of(EntityType.file)))).thenReturn(new EntityChildrenResponse().setPage(files));
	}

	private void setupDownload(EntityHeader file, String fileHandleId) {
		when(mockEntityManager.getFileHandleIdForVersion(user, file.getId(), file.getVersionNumber()))
				.thenReturn(fileHandleId);
		when(mockFileHandleManager.getRedirectURLForFileHandle(new FileHandleUrlRequest(user, fileHandleId)
				.withAssociation(FileHandleAssociateType.FileEntity, file.getId())))
				.thenReturn("https://s3/" + fileHandleId);
	}

	private UsersEntityAccessInfo access(Long entityId, AuthorizationStatus status) {
		return new UsersEntityAccessInfo().withEntityId(entityId).withAuthorizationStatus(status);
	}

	@Test
	public void testExecuteWithFolderAndChainedInputs() {
		InteractiveSessionExecutionProperties properties = properties().setInputTaskIds(List.of(chainedTaskId));
		CurationTask task = task(taskId, properties);

		EntityHeader folderFile = file("syn201", "cohort.csv", 2L);
		EntityHeader chainedFile = file("syn301", "summary.csv", 1L);
		setupFolder(inputFolderId, List.of(folderFile));
		setupFolder(chainedOutputFolderId, List.of(chainedFile));
		when(mockCurationTaskDao.getTaskStatus(chainedTaskId)).thenReturn(new TaskStatus().setState(TaskState.COMPLETED)
				.setExecutionDetails(new InteractiveSessionExecutionDetails().setOutputFolderId(chainedOutputFolderId)));
		when(mockCurationTaskDao.getCurationTasks(100L, 100L, 0L)).thenReturn(List.of(task));
		when(mockEntityAuthorizationManager.batchHasAccess(user, List.of(201L, 301L), ACCESS_TYPE.DOWNLOAD))
				.thenReturn(List.of(access(201L, AuthorizationStatus.authorized()),
						access(301L, AuthorizationStatus.authorized())));
		setupDownload(folderFile, "fh1");
		setupDownload(chainedFile, "fh2");
		when(mockSessionLauncher.start(any())).thenReturn(new LaunchedSession("session-1", endsOn));

		// call under test
		InteractiveSessionExecutionDetails result = subWorker.execute(user, task,
				new InteractiveSessionExecutionDetails().setStartedBy(userId.toString()), mockCallback);

		verify(mockSessionLauncher).start(new SessionLaunchRequest(taskId, taskId.toString(), "python-3.12", userId, 120L,
				List.of(new SessionInput("files/cohort.csv", "https://s3/fh1"),
						new SessionInput("tasks/986/summary.csv", "https://s3/fh2"))));
		assertEquals(new InteractiveSessionExecutionDetails().setStartedBy(userId.toString()).setSessionId("session-1")
				.setSessionState(InteractiveSessionState.RUNNING).setWorkspaceId(taskId.toString()).setEndsOn(endsOn)
				.setInputFiles(List.of(
						new SessionInputFile().setEntityId("syn201").setVersionNumber(2L).setPath("files/cohort.csv"),
						new SessionInputFile().setEntityId("syn301").setVersionNumber(1L)
								.setPath("tasks/986/summary.csv").setSourceTaskId(chainedTaskId))),
				result);
		assertFalse(subWorker.isComplete(result));
	}

	@Test
	public void testExecuteWithoutDownloadAccess() {
		CurationTask task = task(taskId, properties());
		setupFolder(inputFolderId, List.of(file("syn201", "cohort.csv", 2L), file("syn202", "genotypes.vcf", 1L)));
		when(mockCurationTaskDao.getCurationTasks(100L, 100L, 0L)).thenReturn(List.of(task));
		// Aggregate-only access does not let data into a session.
		when(mockEntityAuthorizationManager.batchHasAccess(user, List.of(201L, 202L), ACCESS_TYPE.DOWNLOAD))
				.thenReturn(List.of(access(201L, AuthorizationStatus.authorized()),
						access(202L, AuthorizationStatus.accessDeniedButAggregateAllowed("aggregate only", "syn9"))));

		// call under test
		String message = assertThrows(UnauthorizedException.class,
				() -> subWorker.execute(user, task, new InteractiveSessionExecutionDetails(), mockCallback)).getMessage();

		assertEquals("The session cannot start: everyone working in workspace 987 must be able to download every input"
				+ " in it, and you cannot download syn202.", message);
		verify(mockFileHandleManager, never()).getRedirectURLForFileHandle(any());
		verifyNoInteractions(mockSessionLauncher);
	}

	@Test
	public void testExecuteWithTooManyInputs() {
		CurationTask task = task(taskId, properties());
		List<EntityHeader> files = new ArrayList<>();
		for (int i = 0; i <= InteractiveSessionSubWorker.MAX_INPUT_FILES; i++) {
			files.add(file("syn" + (1000 + i), "file" + i + ".csv", 1L));
		}
		setupFolder(inputFolderId, files);

		// call under test
		String message = assertThrows(IllegalArgumentException.class,
				() -> subWorker.execute(user, task, new InteractiveSessionExecutionDetails(), mockCallback)).getMessage();

		assertEquals("The session has 101 input files, which exceeds the maximum of 100.", message);
		verifyNoInteractions(mockSessionLauncher);
	}

	@Test
	public void testExecuteWithChainedTaskNotApproved() {
		CurationTask task = task(taskId, properties().setInputFolderId(null).setInputTaskIds(List.of(chainedTaskId)));
		when(mockCurationTaskDao.getTaskStatus(chainedTaskId)).thenReturn(new TaskStatus().setState(TaskState.IN_REVIEW)
				.setExecutionDetails(new InteractiveSessionExecutionDetails()));

		// call under test
		String message = assertThrows(IllegalArgumentException.class,
				() -> subWorker.execute(user, task, new InteractiveSessionExecutionDetails(), mockCallback)).getMessage();

		assertEquals("Input task 986 has no approved outputs yet.", message);
		verifyNoInteractions(mockSessionLauncher);
	}

	@Test
	public void testExecuteWithMaxDurationOverLimit() {
		CurationTask task = task(taskId, properties().setMaxDurationMinutes(721L));

		// call under test
		String message = assertThrows(IllegalArgumentException.class,
				() -> subWorker.execute(user, task, new InteractiveSessionExecutionDetails(), mockCallback)).getMessage();

		assertEquals("maxDurationMinutes must be between 1 and 720.", message);
		verifyNoInteractions(mockEntityManager, mockSessionLauncher);
	}

	@Test
	public void testExecuteWithWrongProperties() {
		CurationTask task = new CurationTask().setTaskId(taskId).setTaskProperties(new SampleSheetGenerationExecutionProperties());

		// call under test
		assertThrows(IllegalArgumentException.class,
				() -> subWorker.execute(user, task, new InteractiveSessionExecutionDetails(), mockCallback));

		verifyNoInteractions(mockSessionLauncher);
	}

	@Test
	public void testExecuteWithSharedWorkspace() {
		CurationTask owner = task(ownerTaskId, properties().setInputFolderId("syn400"));
		CurationTask task = task(taskId, properties().setWorkspaceTaskId(ownerTaskId));
		CurationTask unrelated = task(984L, properties());
		EntityHeader folderFile = file("syn201", "cohort.csv", 2L);
		setupFolder(inputFolderId, List.of(folderFile));
		when(mockCurationTaskDao.getCurationTask(ownerTaskId)).thenReturn(Optional.of(owner));
		when(mockCurationTaskDao.getCurationTasks(100L, 100L, 0L)).thenReturn(List.of(unrelated, owner, task));
		// The collaborator ran the owner's session over syn401.
		when(mockCurationTaskDao.getTaskStatus(ownerTaskId)).thenReturn(new TaskStatus().setState(TaskState.EXECUTING)
				.setExecutionDetails(new InteractiveSessionExecutionDetails().setSessionId("session-0")
						.setStartedBy(collaboratorId.toString())
						.setInputFiles(List.of(new SessionInputFile().setEntityId("syn401").setPath("files/a.csv")))));
		when(mockUserManager.getUserInfo(collaboratorId)).thenReturn(collaborator);
		when(mockEntityAuthorizationManager.batchHasAccess(user, List.of(201L, 401L), ACCESS_TYPE.DOWNLOAD))
				.thenReturn(List.of(access(201L, AuthorizationStatus.authorized()),
						access(401L, AuthorizationStatus.authorized())));
		when(mockEntityAuthorizationManager.batchHasAccess(collaborator, List.of(201L, 401L), ACCESS_TYPE.DOWNLOAD))
				.thenReturn(List.of(access(201L, AuthorizationStatus.authorized()),
						access(401L, AuthorizationStatus.authorized())));
		setupDownload(folderFile, "fh1");
		when(mockSessionLauncher.start(any())).thenReturn(new LaunchedSession("session-1", endsOn));

		// call under test
		InteractiveSessionExecutionDetails result = subWorker.execute(user, task, new InteractiveSessionExecutionDetails(),
				mockCallback);

		verify(mockSessionLauncher).start(new SessionLaunchRequest(taskId, ownerTaskId.toString(), "python-3.12", userId,
				120L, List.of(new SessionInput("files/cohort.csv", "https://s3/fh1"))));
		assertEquals(ownerTaskId.toString(), result.getWorkspaceId());
		verify(mockCurationTaskDao, never()).getTaskStatus(984L);
	}

	@Test
	public void testExecuteWithSharedWorkspaceAndCollaboratorWithoutAccess() {
		CurationTask owner = task(ownerTaskId, properties().setInputFolderId("syn400"));
		CurationTask task = task(taskId, properties().setWorkspaceTaskId(ownerTaskId));
		setupFolder(inputFolderId, List.of(file("syn201", "cohort.csv", 2L)));
		when(mockCurationTaskDao.getCurationTask(ownerTaskId)).thenReturn(Optional.of(owner));
		when(mockCurationTaskDao.getCurationTasks(100L, 100L, 0L)).thenReturn(List.of(owner, task));
		when(mockCurationTaskDao.getTaskStatus(ownerTaskId)).thenReturn(new TaskStatus().setState(TaskState.EXECUTING)
				.setExecutionDetails(new InteractiveSessionExecutionDetails().setSessionId("session-0")
						.setStartedBy(collaboratorId.toString())
						.setInputFiles(List.of(new SessionInputFile().setEntityId("syn401").setPath("files/a.csv")))));
		when(mockUserManager.getUserInfo(collaboratorId)).thenReturn(collaborator);
		when(mockEntityAuthorizationManager.batchHasAccess(user, List.of(201L, 401L), ACCESS_TYPE.DOWNLOAD))
				.thenReturn(List.of(access(201L, AuthorizationStatus.authorized()),
						access(401L, AuthorizationStatus.authorized())));
		// The collaborator could otherwise read this session's inputs through the shared /workspace.
		when(mockEntityAuthorizationManager.batchHasAccess(collaborator, List.of(201L, 401L), ACCESS_TYPE.DOWNLOAD))
				.thenReturn(List.of(access(201L, AuthorizationStatus.accessDenied("no")),
						access(401L, AuthorizationStatus.authorized())));

		// call under test
		String message = assertThrows(UnauthorizedException.class,
				() -> subWorker.execute(user, task, new InteractiveSessionExecutionDetails(), mockCallback)).getMessage();

		assertEquals("The session cannot start: everyone working in workspace 985 must be able to download every input"
				+ " in it, and user 202 cannot download syn201.", message);
		verifyNoInteractions(mockSessionLauncher);
	}

	@Test
	public void testExecuteWithNestedWorkspace() {
		CurationTask owner = task(ownerTaskId, properties().setWorkspaceTaskId(984L));
		CurationTask task = task(taskId, properties().setWorkspaceTaskId(ownerTaskId));
		setupFolder(inputFolderId, List.of());
		when(mockCurationTaskDao.getCurationTask(ownerTaskId)).thenReturn(Optional.of(owner));

		// call under test
		assertThrows(IllegalArgumentException.class,
				() -> subWorker.execute(user, task, new InteractiveSessionExecutionDetails(), mockCallback));

		verifyNoInteractions(mockSessionLauncher);
	}

	@Test
	public void testEnd() {
		CurationTask task = task(taskId, properties());
		InteractiveSessionExecutionDetails details = new InteractiveSessionExecutionDetails().setSessionId("session-1")
				.setSessionState(InteractiveSessionState.RUNNING).setStartedBy(userId.toString());
		when(mockSessionLauncher.stop("session-1"))
				.thenReturn(List.of(new SessionOutput("results/table1.csv", 2048L, "abc123")));

		// call under test
		InteractiveSessionExecutionDetails result = subWorker.end(user, task, details, mockCallback);

		assertEquals(new InteractiveSessionExecutionDetails().setSessionId("session-1")
				.setSessionState(InteractiveSessionState.ENDED).setStartedBy(userId.toString())
				.setOutputFiles(List.of(new SessionOutputFile().setPath("results/table1.csv").setContentSize(2048L)
						.setContentMd5("abc123"))),
				result);
		assertTrue(subWorker.isComplete(result));
	}
}
