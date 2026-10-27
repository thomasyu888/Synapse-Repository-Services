package org.sagebionetworks.repo.manager.curation.compute;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.Mockito;
import org.sagebionetworks.repo.manager.curation.CurationTaskManager;
import org.sagebionetworks.repo.model.AuthorizationConstants;
import org.sagebionetworks.repo.model.UnauthorizedException;
import org.sagebionetworks.repo.model.UserInfo;
import org.sagebionetworks.repo.model.curation.ComputeTaskAction;
import org.sagebionetworks.repo.model.curation.ComputeTaskExecutionRequest;
import org.sagebionetworks.repo.model.curation.ComputeTaskExecutionResponse;
import org.sagebionetworks.repo.model.curation.CurationTask;
import org.sagebionetworks.repo.model.curation.TaskState;
import org.sagebionetworks.repo.model.curation.TaskStatus;
import org.sagebionetworks.repo.model.curation.execution.ExecutableTaskExecutionDetails;
import org.sagebionetworks.repo.model.curation.execution.SampleSheetGenerationExecutionDetails;
import org.sagebionetworks.repo.model.dao.asynch.AsyncJobProgressCallback;
import org.sagebionetworks.repo.model.curation.execution.GridExecutionDetails;
import org.sagebionetworks.repo.model.dbo.curation.CurationTaskDao;
import org.sagebionetworks.repo.web.NotFoundException;

@ExtendWith(MockitoExtension.class)
public class ComputeTaskDispatcherImplTest {

	@Mock
	private CurationTaskDao mockCurationTaskDao;

	@Mock
	private CurationTaskManager mockCurationTaskManager;

	@Mock
	private ComputeTaskSubWorker<SampleSheetGenerationExecutionDetails> mockSubWorker;

	@Mock
	private AsyncJobProgressCallback mockCallback;

	private ComputeTaskDispatcherImpl dispatcher;

	private UserInfo userInfo;
	private Long userId = 101L;
	private Long taskId = 987L;
	private String projectId = "syn123";
	private String jobId = "job-abc-123";

	@BeforeEach
	public void setup() {
		userInfo = new UserInfo(false, userId, AuthorizationConstants.DEFAULT_REALM_ID, new HashSet<>(Set.of(userId)));

		when(mockSubWorker.getExecutionDetailsType()).thenReturn(SampleSheetGenerationExecutionDetails.class);
		dispatcher = new ComputeTaskDispatcherImpl(mockCurationTaskDao, mockCurationTaskManager, List.of(mockSubWorker));
	}

	@Test
	public void testDispatchWithNullRequest() {
		// call under test
		assertThrows(IllegalArgumentException.class,
				() -> dispatcher.dispatch(jobId, userInfo, null, mockCallback));

		verifyNoMoreInteractions(mockCurationTaskDao);
	}

	@Test
	public void testDispatchWithNullTaskId() {
		ComputeTaskExecutionRequest request = new ComputeTaskExecutionRequest();

		// call under test
		assertThrows(IllegalArgumentException.class,
				() -> dispatcher.dispatch(jobId, userInfo, request, mockCallback));

		verifyNoMoreInteractions(mockCurationTaskDao);
	}

	@Test
	public void testDispatchWithTaskNotFound() {
		ComputeTaskExecutionRequest request = new ComputeTaskExecutionRequest().setTaskId(taskId);
		when(mockCurationTaskDao.getCurationTask(taskId)).thenReturn(Optional.empty());

		// call under test
		assertThrows(NotFoundException.class,
				() -> dispatcher.dispatch(jobId, userInfo, request, mockCallback));

		verify(mockCurationTaskDao, never()).updateTaskStatus(any(), any(), any());
	}

	@Test
	public void testDispatchWithUnauthorizedUser() {
		ComputeTaskExecutionRequest request = new ComputeTaskExecutionRequest().setTaskId(taskId);
		CurationTask task = new CurationTask().setProjectId(projectId).setAssigneePrincipalId("999");
		when(mockCurationTaskDao.getCurationTask(taskId)).thenReturn(Optional.of(task));
		Mockito.doThrow(new UnauthorizedException("no access"))
				.when(mockCurationTaskManager).validateUpdateTaskStatus(userInfo, task);

		// call under test
		assertThrows(UnauthorizedException.class,
				() -> dispatcher.dispatch(jobId, userInfo, request, mockCallback));

		verify(mockCurationTaskDao, never()).updateTaskStatus(any(), any(), any());
	}

	@ParameterizedTest
	@EnumSource(value = TaskState.class, names = {"NOT_STARTED"}, mode = EnumSource.Mode.EXCLUDE)
	public void testDispatchWithWrongState(TaskState state) {
		ComputeTaskExecutionRequest request = new ComputeTaskExecutionRequest().setTaskId(taskId);
		CurationTask task = new CurationTask().setProjectId(projectId).setAssigneePrincipalId(userId.toString());
		TaskStatus status = new TaskStatus().setState(state)
				.setExecutionDetails(new SampleSheetGenerationExecutionDetails());

		when(mockCurationTaskDao.getCurationTask(taskId)).thenReturn(Optional.of(task));
		when(mockCurationTaskDao.getTaskStatus(taskId)).thenReturn(status);

		// call under test
		assertThrows(IllegalArgumentException.class,
				() -> dispatcher.dispatch(jobId, userInfo, request, mockCallback));

		verify(mockCurationTaskDao, never()).updateTaskStatus(any(), any(), any());
	}

	@Test
	public void testDispatchWithNonExecutableDetails() {
		ComputeTaskExecutionRequest request = new ComputeTaskExecutionRequest().setTaskId(taskId);
		CurationTask task = new CurationTask().setProjectId(projectId).setAssigneePrincipalId(userId.toString());
		TaskStatus status = new TaskStatus().setState(TaskState.NOT_STARTED)
				.setExecutionDetails(new GridExecutionDetails());

		when(mockCurationTaskDao.getCurationTask(taskId)).thenReturn(Optional.of(task));
		when(mockCurationTaskDao.getTaskStatus(taskId)).thenReturn(status);

		// call under test
		assertThrows(IllegalArgumentException.class,
				() -> dispatcher.dispatch(jobId, userInfo, request, mockCallback));

		verify(mockCurationTaskDao, never()).updateTaskStatus(any(), any(), any());
	}

	@Test
	public void testDispatchWithNoSubWorkerRegistered() {
		dispatcher = new ComputeTaskDispatcherImpl(mockCurationTaskDao, mockCurationTaskManager, Collections.emptyList());

		ComputeTaskExecutionRequest request = new ComputeTaskExecutionRequest().setTaskId(taskId);
		CurationTask task = new CurationTask().setProjectId(projectId).setAssigneePrincipalId(userId.toString());

		SampleSheetGenerationExecutionDetails details = new SampleSheetGenerationExecutionDetails();
		TaskStatus status = new TaskStatus().setState(TaskState.NOT_STARTED).setExecutionDetails(details).setEtag("etag1");

		when(mockCurationTaskDao.getCurationTask(taskId)).thenReturn(Optional.of(task));
		when(mockCurationTaskDao.getTaskStatus(taskId)).thenReturn(status);

		// call under test
		assertThrows(IllegalArgumentException.class,
				() -> dispatcher.dispatch(jobId, userInfo, request, mockCallback));

		verify(mockCurationTaskDao, never()).updateTaskStatus(any(), any(), any());
	}

	@Test
	public void testDispatchWithSuccess() throws Exception {
		ComputeTaskExecutionRequest request = new ComputeTaskExecutionRequest().setTaskId(taskId);
		CurationTask task = new CurationTask().setProjectId(projectId).setAssigneePrincipalId(userId.toString());

		SampleSheetGenerationExecutionDetails details = new SampleSheetGenerationExecutionDetails();
		TaskStatus status = new TaskStatus().setState(TaskState.NOT_STARTED).setExecutionDetails(details).setEtag("etag1");
		TaskStatus freshStatus = new TaskStatus().setState(TaskState.EXECUTING).setExecutionDetails(details).setEtag("etag2");

		when(mockCurationTaskDao.getCurationTask(taskId)).thenReturn(Optional.of(task));
		when(mockCurationTaskDao.getTaskStatus(taskId)).thenReturn(status, freshStatus);
		when(mockCurationTaskDao.updateTaskStatus(eq(userId), eq(taskId), any())).thenReturn(status, freshStatus);

		SampleSheetGenerationExecutionDetails resultDetails = new SampleSheetGenerationExecutionDetails();
		when(mockSubWorker.execute(eq(userInfo), eq(task), any(), eq(mockCallback))).thenReturn(resultDetails);
		when(mockSubWorker.isComplete(resultDetails)).thenReturn(true);

		// call under test
		ComputeTaskExecutionResponse response = dispatcher.dispatch(jobId, userInfo, request, mockCallback);

		assertEquals(taskId, response.getTaskId());
		assertEquals(resultDetails, response.getExecutionDetails());

		// Verify both state transitions
		ArgumentCaptor<TaskStatus> statusCaptor = ArgumentCaptor.forClass(TaskStatus.class);
		verify(mockCurationTaskDao, org.mockito.Mockito.times(2)).updateTaskStatus(eq(userId), eq(taskId), statusCaptor.capture());

		TaskStatus executingStatus = statusCaptor.getAllValues().get(0);
		assertEquals(TaskState.EXECUTING, executingStatus.getState());
		ExecutableTaskExecutionDetails executingDetails = (ExecutableTaskExecutionDetails) executingStatus.getExecutionDetails();
		assertEquals(jobId, executingDetails.getAsyncJobId());
		assertEquals(userId.toString(), executingDetails.getStartedBy());

		TaskStatus inReviewStatus = statusCaptor.getAllValues().get(1);
		assertEquals(TaskState.IN_REVIEW, inReviewStatus.getState());
	}

	@Test
	public void testDispatchWithSubWorkerFailure() throws Exception {
		ComputeTaskExecutionRequest request = new ComputeTaskExecutionRequest().setTaskId(taskId);
		CurationTask task = new CurationTask().setProjectId(projectId).setAssigneePrincipalId(userId.toString());

		SampleSheetGenerationExecutionDetails details = new SampleSheetGenerationExecutionDetails();
		TaskStatus status = new TaskStatus().setState(TaskState.NOT_STARTED).setExecutionDetails(details).setEtag("etag1");
		TaskStatus freshStatus = new TaskStatus().setState(TaskState.EXECUTING).setExecutionDetails(details).setEtag("etag2");

		when(mockCurationTaskDao.getCurationTask(taskId)).thenReturn(Optional.of(task));
		when(mockCurationTaskDao.getTaskStatus(taskId)).thenReturn(status, freshStatus);
		when(mockCurationTaskDao.updateTaskStatus(eq(userId), eq(taskId), any())).thenReturn(status, freshStatus);

		RuntimeException subWorkerError = new RuntimeException("AI agent failed");
		when(mockSubWorker.execute(eq(userInfo), eq(task), any(), eq(mockCallback))).thenThrow(subWorkerError);

		// call under test
		RuntimeException thrown = assertThrows(RuntimeException.class,
				() -> dispatcher.dispatch(jobId, userInfo, request, mockCallback));

		assertEquals("AI agent failed", thrown.getMessage());

		// Verify error state transition: second updateTaskStatus call should set NOT_STARTED with error
		ArgumentCaptor<TaskStatus> statusCaptor = ArgumentCaptor.forClass(TaskStatus.class);
		verify(mockCurationTaskDao, org.mockito.Mockito.atLeast(2)).updateTaskStatus(eq(userId), eq(taskId), statusCaptor.capture());

		TaskStatus errorStatus = statusCaptor.getAllValues().get(1);
		assertEquals(TaskState.NOT_STARTED, errorStatus.getState());
		ExecutableTaskExecutionDetails errorDetails = (ExecutableTaskExecutionDetails) errorStatus.getExecutionDetails();
		assertEquals("AI agent failed", errorDetails.getErrorMessage());
		assertEquals("java.lang.RuntimeException", errorDetails.getErrorDetails());
	}

	@Test
	public void testDispatchWithExecutionLeftRunning() throws Exception {
		ComputeTaskExecutionRequest request = new ComputeTaskExecutionRequest().setTaskId(taskId);
		CurationTask task = new CurationTask().setProjectId(projectId).setAssigneePrincipalId(userId.toString());

		SampleSheetGenerationExecutionDetails details = new SampleSheetGenerationExecutionDetails();
		TaskStatus status = new TaskStatus().setState(TaskState.NOT_STARTED).setExecutionDetails(details).setEtag("etag1");
		TaskStatus freshStatus = new TaskStatus().setState(TaskState.EXECUTING)
				.setExecutionDetails(new SampleSheetGenerationExecutionDetails().setAsyncJobId(jobId)).setEtag("etag2");

		when(mockCurationTaskDao.getCurationTask(taskId)).thenReturn(Optional.of(task));
		when(mockCurationTaskDao.getTaskStatus(taskId)).thenReturn(status, freshStatus);

		SampleSheetGenerationExecutionDetails resultDetails = new SampleSheetGenerationExecutionDetails()
				.setAsyncJobId(jobId).setStartedBy(userId.toString());
		when(mockSubWorker.execute(eq(userInfo), eq(task), any(), eq(mockCallback))).thenReturn(resultDetails);
		when(mockSubWorker.isComplete(resultDetails)).thenReturn(false);

		// call under test
		ComputeTaskExecutionResponse response = dispatcher.dispatch(jobId, userInfo, request, mockCallback);

		assertEquals(new ComputeTaskExecutionResponse().setTaskId(taskId).setExecutionDetails(resultDetails), response);

		ArgumentCaptor<TaskStatus> statusCaptor = ArgumentCaptor.forClass(TaskStatus.class);
		verify(mockCurationTaskDao, times(2)).updateTaskStatus(eq(userId), eq(taskId), statusCaptor.capture());

		// The task stays EXECUTING, with no job attached, until its session is ended.
		TaskStatus runningStatus = statusCaptor.getAllValues().get(1);
		assertEquals(TaskState.EXECUTING, runningStatus.getState());
		assertEquals(new SampleSheetGenerationExecutionDetails().setStartedBy(userId.toString()),
				runningStatus.getExecutionDetails());
		verify(mockSubWorker, never()).end(any(), any(), any(), any());
	}

	@ParameterizedTest
	@EnumSource(value = TaskState.class, names = {"EXECUTING"}, mode = EnumSource.Mode.EXCLUDE)
	public void testDispatchEndSessionWithWrongState(TaskState state) {
		ComputeTaskExecutionRequest request = new ComputeTaskExecutionRequest().setTaskId(taskId)
				.setAction(ComputeTaskAction.END_SESSION);
		CurationTask task = new CurationTask().setProjectId(projectId).setAssigneePrincipalId(userId.toString());
		TaskStatus status = new TaskStatus().setState(state)
				.setExecutionDetails(new SampleSheetGenerationExecutionDetails());

		when(mockCurationTaskDao.getCurationTask(taskId)).thenReturn(Optional.of(task));
		when(mockCurationTaskDao.getTaskStatus(taskId)).thenReturn(status);

		// call under test
		String message = assertThrows(IllegalArgumentException.class,
				() -> dispatcher.dispatch(jobId, userInfo, request, mockCallback)).getMessage();

		assertEquals("Task must be in EXECUTING state to end its session. Current state: " + state, message);
		verify(mockCurationTaskDao, never()).updateTaskStatus(any(), any(), any());
	}

	@Test
	public void testDispatchEndSessionWithJobInProgress() throws Exception {
		ComputeTaskExecutionRequest request = new ComputeTaskExecutionRequest().setTaskId(taskId)
				.setAction(ComputeTaskAction.END_SESSION);
		CurationTask task = new CurationTask().setProjectId(projectId).setAssigneePrincipalId(userId.toString());
		TaskStatus status = new TaskStatus().setState(TaskState.EXECUTING)
				.setExecutionDetails(new SampleSheetGenerationExecutionDetails().setAsyncJobId("other-job"));

		when(mockCurationTaskDao.getCurationTask(taskId)).thenReturn(Optional.of(task));
		when(mockCurationTaskDao.getTaskStatus(taskId)).thenReturn(status);

		// call under test
		String message = assertThrows(IllegalArgumentException.class,
				() -> dispatcher.dispatch(jobId, userInfo, request, mockCallback)).getMessage();

		assertEquals("Task 987 has no running session to end.", message);
		verify(mockCurationTaskDao, never()).updateTaskStatus(any(), any(), any());
		verify(mockSubWorker, never()).end(any(), any(), any(), any());
	}

	@Test
	public void testDispatchEndSessionWithNothingRunning() throws Exception {
		ComputeTaskExecutionRequest request = new ComputeTaskExecutionRequest().setTaskId(taskId)
				.setAction(ComputeTaskAction.END_SESSION);
		CurationTask task = new CurationTask().setProjectId(projectId).setAssigneePrincipalId(userId.toString());
		SampleSheetGenerationExecutionDetails details = new SampleSheetGenerationExecutionDetails();
		TaskStatus status = new TaskStatus().setState(TaskState.EXECUTING).setExecutionDetails(details);

		when(mockCurationTaskDao.getCurationTask(taskId)).thenReturn(Optional.of(task));
		when(mockCurationTaskDao.getTaskStatus(taskId)).thenReturn(status);
		when(mockSubWorker.isComplete(details)).thenReturn(true);

		// call under test
		assertThrows(IllegalArgumentException.class,
				() -> dispatcher.dispatch(jobId, userInfo, request, mockCallback));

		verify(mockCurationTaskDao, never()).updateTaskStatus(any(), any(), any());
		verify(mockSubWorker, never()).end(any(), any(), any(), any());
	}

	@Test
	public void testDispatchEndSessionWithSuccess() throws Exception {
		ComputeTaskExecutionRequest request = new ComputeTaskExecutionRequest().setTaskId(taskId)
				.setAction(ComputeTaskAction.END_SESSION);
		CurationTask task = new CurationTask().setProjectId(projectId).setAssigneePrincipalId(userId.toString());

		SampleSheetGenerationExecutionDetails details = new SampleSheetGenerationExecutionDetails()
				.setStartedBy(userId.toString()).setErrorMessage("an earlier attempt failed");
		TaskStatus status = new TaskStatus().setState(TaskState.EXECUTING).setExecutionDetails(details).setEtag("etag1");
		TaskStatus freshStatus = new TaskStatus().setState(TaskState.EXECUTING)
				.setExecutionDetails(new SampleSheetGenerationExecutionDetails()).setEtag("etag2");

		when(mockCurationTaskDao.getCurationTask(taskId)).thenReturn(Optional.of(task));
		when(mockCurationTaskDao.getTaskStatus(taskId)).thenReturn(status, freshStatus);
		when(mockSubWorker.isComplete(details)).thenReturn(false);

		SampleSheetGenerationExecutionDetails resultDetails = new SampleSheetGenerationExecutionDetails()
				.setStartedBy(userId.toString()).setAsyncJobId(jobId);
		when(mockSubWorker.end(eq(userInfo), eq(task), any(), eq(mockCallback))).thenReturn(resultDetails);

		// call under test
		ComputeTaskExecutionResponse response = dispatcher.dispatch(jobId, userInfo, request, mockCallback);

		assertEquals(new ComputeTaskExecutionResponse().setTaskId(taskId).setExecutionDetails(resultDetails), response);

		ArgumentCaptor<TaskStatus> statusCaptor = ArgumentCaptor.forClass(TaskStatus.class);
		verify(mockCurationTaskDao, times(2)).updateTaskStatus(eq(userId), eq(taskId), statusCaptor.capture());

		TaskStatus endingStatus = statusCaptor.getAllValues().get(0);
		assertEquals(new TaskStatus().setState(TaskState.EXECUTING).setEtag("etag1").setExecutionDetails(
				new SampleSheetGenerationExecutionDetails().setStartedBy(userId.toString()).setAsyncJobId(jobId)),
				endingStatus);

		TaskStatus inReviewStatus = statusCaptor.getAllValues().get(1);
		assertEquals(new TaskStatus().setState(TaskState.IN_REVIEW).setEtag("etag2").setExecutionDetails(
				new SampleSheetGenerationExecutionDetails().setStartedBy(userId.toString())), inReviewStatus);
		verify(mockSubWorker, never()).execute(any(), any(), any(), any());
	}

	@Test
	public void testDispatchEndSessionWithFailure() throws Exception {
		ComputeTaskExecutionRequest request = new ComputeTaskExecutionRequest().setTaskId(taskId)
				.setAction(ComputeTaskAction.END_SESSION);
		CurationTask task = new CurationTask().setProjectId(projectId).setAssigneePrincipalId(userId.toString());

		SampleSheetGenerationExecutionDetails details = new SampleSheetGenerationExecutionDetails()
				.setStartedBy(userId.toString());
		TaskStatus status = new TaskStatus().setState(TaskState.EXECUTING).setExecutionDetails(details).setEtag("etag1");
		TaskStatus freshStatus = new TaskStatus().setState(TaskState.EXECUTING).setEtag("etag2").setExecutionDetails(
				new SampleSheetGenerationExecutionDetails().setStartedBy(userId.toString()).setAsyncJobId(jobId));

		when(mockCurationTaskDao.getCurationTask(taskId)).thenReturn(Optional.of(task));
		when(mockCurationTaskDao.getTaskStatus(taskId)).thenReturn(status, freshStatus);
		when(mockSubWorker.isComplete(details)).thenReturn(false);
		when(mockSubWorker.end(eq(userInfo), eq(task), any(), eq(mockCallback)))
				.thenThrow(new IllegalStateException("launcher unreachable"));

		// call under test
		assertThrows(IllegalStateException.class, () -> dispatcher.dispatch(jobId, userInfo, request, mockCallback));

		ArgumentCaptor<TaskStatus> statusCaptor = ArgumentCaptor.forClass(TaskStatus.class);
		verify(mockCurationTaskDao, times(2)).updateTaskStatus(eq(userId), eq(taskId), statusCaptor.capture());

		// The session may still hold the user's work, so the task stays EXECUTING and can be ended again.
		TaskStatus errorStatus = statusCaptor.getAllValues().get(1);
		assertEquals(new TaskStatus().setState(TaskState.EXECUTING).setEtag("etag2").setExecutionDetails(
				new SampleSheetGenerationExecutionDetails().setStartedBy(userId.toString())
						.setErrorMessage("launcher unreachable").setErrorDetails("java.lang.IllegalStateException")),
				errorStatus);
	}
}
