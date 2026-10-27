package org.sagebionetworks.repo.manager.curation.compute.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sagebionetworks.repo.manager.curation.CurationTaskManager;
import org.sagebionetworks.repo.manager.entity.EntityAuthorizationManager;
import org.sagebionetworks.repo.manager.entity.decider.UsersEntityAccessInfo;
import org.sagebionetworks.repo.model.ACCESS_TYPE;
import org.sagebionetworks.repo.model.AuthorizationConstants;
import org.sagebionetworks.repo.model.AuthorizationConstants.BOOTSTRAP_PRINCIPAL;
import org.sagebionetworks.repo.model.UnauthorizedException;
import org.sagebionetworks.repo.model.UserInfo;
import org.sagebionetworks.repo.model.auth.AuthorizationStatus;
import org.sagebionetworks.repo.model.curation.TaskState;
import org.sagebionetworks.repo.model.curation.TaskStatus;
import org.sagebionetworks.repo.model.curation.execution.InteractiveSessionAccess;
import org.sagebionetworks.repo.model.curation.execution.InteractiveSessionExecutionDetails;
import org.sagebionetworks.repo.model.curation.execution.InteractiveSessionState;
import org.sagebionetworks.repo.model.curation.execution.ListSessionEnvironmentsResponse;
import org.sagebionetworks.repo.model.curation.execution.SessionEnvironment;
import org.sagebionetworks.repo.model.curation.execution.SessionInputFile;
import org.sagebionetworks.repo.model.curation.execution.SessionPackage;

@ExtendWith(MockitoExtension.class)
public class InteractiveSessionManagerTest {

	@Mock
	private CurationTaskManager mockCurationTaskManager;
	@Mock
	private EntityAuthorizationManager mockEntityAuthorizationManager;
	@Mock
	private SessionLauncher mockSessionLauncher;

	private InteractiveSessionManager manager;

	private UserInfo user;
	private Long userId = 101L;
	private Long taskId = 987L;
	private Date expiresOn = new Date(1_800_000_000_000L);

	@BeforeEach
	public void setup() {
		user = new UserInfo(false, userId, AuthorizationConstants.DEFAULT_REALM_ID, new HashSet<>(Set.of(userId)));
		manager = new InteractiveSessionManager(mockCurationTaskManager, mockEntityAuthorizationManager, mockSessionLauncher);
	}

	private InteractiveSessionExecutionDetails running() {
		return new InteractiveSessionExecutionDetails().setSessionId("session-1")
				.setSessionState(InteractiveSessionState.RUNNING).setStartedBy(userId.toString())
				.setInputFiles(List.of(new SessionInputFile().setEntityId("syn201").setPath("files/cohort.csv")));
	}

	@Test
	public void testListEnvironments() {
		SessionEnvironment environment = new SessionEnvironment().setId("python-3.12").setName("Python 3.12")
				.setImageDigest("sha256:abc").setPackages(List.of(new SessionPackage().setName("pandas").setVersion("2.2.3")));
		when(mockSessionLauncher.listEnvironments()).thenReturn(List.of(environment));

		// call under test
		ListSessionEnvironmentsResponse response = manager.listEnvironments(user);

		assertEquals(new ListSessionEnvironmentsResponse().setEnvironments(List.of(environment)), response);
	}

	@Test
	public void testListEnvironmentsWithAnonymous() {
		UserInfo anonymous = new UserInfo(false, BOOTSTRAP_PRINCIPAL.ANONYMOUS_USER.getPrincipalId(),
				AuthorizationConstants.DEFAULT_REALM_ID);
		anonymous.setRealmAnonymousUserId(BOOTSTRAP_PRINCIPAL.ANONYMOUS_USER.getPrincipalId());

		// call under test
		assertThrows(UnauthorizedException.class, () -> manager.listEnvironments(anonymous));

		verifyNoInteractions(mockSessionLauncher);
	}

	@Test
	public void testOpenSession() {
		when(mockCurationTaskManager.getTaskStatus(user, taskId))
				.thenReturn(new TaskStatus().setState(TaskState.EXECUTING).setExecutionDetails(running()));
		when(mockEntityAuthorizationManager.batchHasAccess(user, List.of(201L), ACCESS_TYPE.DOWNLOAD)).thenReturn(
				List.of(new UsersEntityAccessInfo().withEntityId(201L).withAuthorizationStatus(AuthorizationStatus.authorized())));
		when(mockSessionLauncher.grantAccess("session-1", userId))
				.thenReturn(new SessionAccessGrant("http://127.0.0.1:3020/s/session-1?token=t", expiresOn));

		// call under test
		InteractiveSessionAccess access = manager.openSession(user, taskId);

		assertEquals(new InteractiveSessionAccess().setSessionUrl("http://127.0.0.1:3020/s/session-1?token=t")
				.setExpiresOn(expiresOn), access);
	}

	@Test
	public void testOpenSessionWithAnotherUsersSession() {
		when(mockCurationTaskManager.getTaskStatus(user, taskId)).thenReturn(new TaskStatus().setState(TaskState.EXECUTING)
				.setExecutionDetails(running().setStartedBy("202")));

		// call under test
		String message = assertThrows(UnauthorizedException.class, () -> manager.openSession(user, taskId)).getMessage();

		assertEquals("Only the person who started this session can open it.", message);
		verifyNoInteractions(mockSessionLauncher);
	}

	@Test
	public void testOpenSessionAfterLosingDownloadAccess() {
		when(mockCurationTaskManager.getTaskStatus(user, taskId))
				.thenReturn(new TaskStatus().setState(TaskState.EXECUTING).setExecutionDetails(running()));
		when(mockEntityAuthorizationManager.batchHasAccess(user, List.of(201L), ACCESS_TYPE.DOWNLOAD)).thenReturn(List.of(
				new UsersEntityAccessInfo().withEntityId(201L).withAuthorizationStatus(AuthorizationStatus.accessDenied("revoked"))));

		// call under test
		String message = assertThrows(UnauthorizedException.class, () -> manager.openSession(user, taskId)).getMessage();

		assertEquals("You can no longer download syn201, which this session uses, so it cannot be opened.", message);
		verifyNoInteractions(mockSessionLauncher);
	}

	@Test
	public void testOpenSessionWhileEnding() {
		when(mockCurationTaskManager.getTaskStatus(user, taskId)).thenReturn(new TaskStatus().setState(TaskState.EXECUTING)
				.setExecutionDetails(running().setAsyncJobId("end-job")));

		// call under test
		assertThrows(IllegalArgumentException.class, () -> manager.openSession(user, taskId));

		verifyNoInteractions(mockSessionLauncher, mockEntityAuthorizationManager);
	}

	@Test
	public void testOpenSessionInReview() {
		when(mockCurationTaskManager.getTaskStatus(user, taskId)).thenReturn(new TaskStatus().setState(TaskState.IN_REVIEW)
				.setExecutionDetails(running().setSessionState(InteractiveSessionState.ENDED)));

		// call under test
		String message = assertThrows(IllegalArgumentException.class, () -> manager.openSession(user, taskId)).getMessage();

		assertEquals("Task 987 has no running session to open.", message);
		verifyNoInteractions(mockSessionLauncher);
	}
}
