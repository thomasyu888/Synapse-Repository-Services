package org.sagebionetworks.repo.manager.curation.compute;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.sagebionetworks.repo.manager.curation.CurationTaskManager;
import org.sagebionetworks.repo.model.UserInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.repo.model.curation.ComputeTaskAction;
import org.sagebionetworks.repo.model.curation.ComputeTaskExecutionRequest;
import org.sagebionetworks.repo.model.curation.ComputeTaskExecutionResponse;
import org.sagebionetworks.repo.model.curation.CurationTask;
import org.sagebionetworks.repo.model.curation.TaskState;
import org.sagebionetworks.repo.model.curation.TaskStatus;
import org.sagebionetworks.repo.model.curation.execution.ExecutableTaskExecutionDetails;
import org.sagebionetworks.repo.model.dao.asynch.AsyncJobProgressCallback;
import org.sagebionetworks.repo.model.dbo.curation.CurationTaskDao;
import org.sagebionetworks.repo.web.NotFoundException;
import org.sagebionetworks.util.ValidateArgument;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ComputeTaskDispatcherImpl implements ComputeTaskDispatcher {

	private static final Logger LOG = LogManager.getLogger(ComputeTaskDispatcherImpl.class);

	private final CurationTaskDao curationTaskDao;
	private final CurationTaskManager curationTaskManager;
	private final Map<Class<? extends ExecutableTaskExecutionDetails>, ComputeTaskSubWorker<?>> subWorkerMap;

	@Autowired
	public ComputeTaskDispatcherImpl(CurationTaskDao curationTaskDao, CurationTaskManager curationTaskManager,
			@Autowired(required = false) List<ComputeTaskSubWorker<?>> subWorkers) {
		this.curationTaskDao = curationTaskDao;
		this.curationTaskManager = curationTaskManager;
		this.subWorkerMap = subWorkers == null ? Collections.emptyMap()
				: subWorkers.stream().collect(
						Collectors.toMap(ComputeTaskSubWorker::getExecutionDetailsType, Function.identity()));
	}

	@Override
	public ComputeTaskExecutionResponse dispatch(String jobId, UserInfo user, ComputeTaskExecutionRequest request,
			AsyncJobProgressCallback callback) throws Exception {
		ValidateArgument.required(request, "request");
		ValidateArgument.required(request.getTaskId(), "request.taskId");

		Long taskId = request.getTaskId();

		CurationTask task = curationTaskDao.getCurationTask(taskId)
				.orElseThrow(() -> new NotFoundException("CurationTask not found: " + taskId));

		curationTaskManager.validateUpdateTaskStatus(user, task);

		TaskStatus currentStatus = curationTaskDao.getTaskStatus(taskId);

		if (ComputeTaskAction.END_SESSION == request.getAction()) {
			return endSession(jobId, user, taskId, task, currentStatus, callback);
		}
		return start(jobId, user, taskId, task, currentStatus, callback);
	}

	private ComputeTaskExecutionResponse start(String jobId, UserInfo user, Long taskId, CurationTask task,
			TaskStatus currentStatus, AsyncJobProgressCallback callback) throws Exception {
		if (currentStatus.getState() != TaskState.NOT_STARTED) {
			throw new IllegalArgumentException(
					"Task must be in NOT_STARTED state to execute. Current state: " + currentStatus.getState());
		}

		ExecutableTaskExecutionDetails details = getExecutableDetails(currentStatus);
		ComputeTaskSubWorker<ExecutableTaskExecutionDetails> subWorker = getSubWorker(details);

		transitionToExecuting(user, taskId, jobId, currentStatus);

		try {
			ExecutableTaskExecutionDetails result = subWorker.execute(user, task, details, callback);

			if (subWorker.isComplete(result)) {
				transitionToInReview(user, taskId, result);
			} else {
				transitionToRunning(user, taskId, result);
			}

			return new ComputeTaskExecutionResponse().setTaskId(taskId).setExecutionDetails(result);
		} catch (Exception e) {
			transitionToNotStartedOnError(user, taskId, e);
			throw e;
		}
	}

	private ComputeTaskExecutionResponse endSession(String jobId, UserInfo user, Long taskId, CurationTask task,
			TaskStatus currentStatus, AsyncJobProgressCallback callback) throws Exception {
		if (currentStatus.getState() != TaskState.EXECUTING) {
			throw new IllegalArgumentException(
					"Task must be in EXECUTING state to end its session. Current state: " + currentStatus.getState());
		}

		ExecutableTaskExecutionDetails details = getExecutableDetails(currentStatus);
		ComputeTaskSubWorker<ExecutableTaskExecutionDetails> subWorker = getSubWorker(details);

		// An asyncJobId means another job is still starting the session or is already ending it.
		if (details.getAsyncJobId() != null || subWorker.isComplete(details)) {
			throw new IllegalArgumentException("Task " + taskId + " has no running session to end.");
		}

		transitionToEnding(user, taskId, jobId, currentStatus);

		try {
			ExecutableTaskExecutionDetails result = subWorker.end(user, task, details, callback);

			transitionToInReview(user, taskId, result);

			return new ComputeTaskExecutionResponse().setTaskId(taskId).setExecutionDetails(result);
		} catch (Exception e) {
			transitionToRunningOnError(user, taskId, e);
			throw e;
		}
	}

	private static ExecutableTaskExecutionDetails getExecutableDetails(TaskStatus status) {
		if (!(status.getExecutionDetails() instanceof ExecutableTaskExecutionDetails details)) {
			throw new IllegalArgumentException(
					"Task does not have ExecutableTaskExecutionDetails. Cannot dispatch for execution.");
		}
		return details;
	}

	private ComputeTaskSubWorker<ExecutableTaskExecutionDetails> getSubWorker(ExecutableTaskExecutionDetails details) {
		@SuppressWarnings("unchecked")
		ComputeTaskSubWorker<ExecutableTaskExecutionDetails> subWorker =
				(ComputeTaskSubWorker<ExecutableTaskExecutionDetails>) subWorkerMap.get(details.getClass());

		if (subWorker == null) {
			throw new IllegalArgumentException(
					"No sub-worker registered for execution details type: " + details.getClass().getName());
		}
		return subWorker;
	}

	private void transitionToExecuting(UserInfo user, Long taskId, String jobId, TaskStatus currentStatus) {
		ExecutableTaskExecutionDetails details = (ExecutableTaskExecutionDetails) currentStatus.getExecutionDetails();
		details.setAsyncJobId(jobId);
		details.setStartedBy(user.getId().toString());
		details.setStartedOn(new Date());
		details.setErrorMessage(null);
		details.setErrorDetails(null);

		currentStatus.setState(TaskState.EXECUTING);
		currentStatus.setExecutionDetails(details);

		curationTaskDao.updateTaskStatus(user.getId(), taskId, currentStatus);
	}

	private void transitionToInReview(UserInfo user, Long taskId, ExecutableTaskExecutionDetails updatedDetails) {
		TaskStatus freshStatus = curationTaskDao.getTaskStatus(taskId);

		updatedDetails.setAsyncJobId(null);
		freshStatus.setState(TaskState.IN_REVIEW);
		freshStatus.setExecutionDetails(updatedDetails);

		curationTaskDao.updateTaskStatus(user.getId(), taskId, freshStatus);
	}

	private void transitionToRunning(UserInfo user, Long taskId, ExecutableTaskExecutionDetails updatedDetails) {
		TaskStatus freshStatus = curationTaskDao.getTaskStatus(taskId);

		updatedDetails.setAsyncJobId(null);
		freshStatus.setState(TaskState.EXECUTING);
		freshStatus.setExecutionDetails(updatedDetails);

		curationTaskDao.updateTaskStatus(user.getId(), taskId, freshStatus);
	}

	private void transitionToEnding(UserInfo user, Long taskId, String jobId, TaskStatus currentStatus) {
		ExecutableTaskExecutionDetails details = (ExecutableTaskExecutionDetails) currentStatus.getExecutionDetails();
		details.setAsyncJobId(jobId);
		details.setErrorMessage(null);
		details.setErrorDetails(null);

		curationTaskDao.updateTaskStatus(user.getId(), taskId, currentStatus);
	}

	/**
	 * A session that failed to end may still hold the user's work, so the task stays EXECUTING with the
	 * error recorded, and ending it can be tried again.
	 */
	private void transitionToRunningOnError(UserInfo user, Long taskId, Exception e) {
		try {
			TaskStatus freshStatus = curationTaskDao.getTaskStatus(taskId);

			ExecutableTaskExecutionDetails details =
					(ExecutableTaskExecutionDetails) freshStatus.getExecutionDetails();
			details.setAsyncJobId(null);
			details.setErrorMessage(e.getMessage());
			details.setErrorDetails(e.getClass().getName());

			freshStatus.setExecutionDetails(details);

			curationTaskDao.updateTaskStatus(user.getId(), taskId, freshStatus);
		} catch (Exception updateException) {
			LOG.error("Failed to record the error ending the session of task {}", taskId, updateException);
		}
	}

	private void transitionToNotStartedOnError(UserInfo user, Long taskId, Exception e) {
		try {
			TaskStatus freshStatus = curationTaskDao.getTaskStatus(taskId);

			ExecutableTaskExecutionDetails details =
					(ExecutableTaskExecutionDetails) freshStatus.getExecutionDetails();
			details.setAsyncJobId(null);
			details.setErrorMessage(e.getMessage());
			details.setErrorDetails(e.getClass().getName());

			freshStatus.setState(TaskState.NOT_STARTED);
			freshStatus.setExecutionDetails(details);

			curationTaskDao.updateTaskStatus(user.getId(), taskId, freshStatus);
		} catch (Exception updateException) {
			LOG.error("Failed to transition task {} back to NOT_STARTED after error", taskId, updateException);
		}
	}
}
