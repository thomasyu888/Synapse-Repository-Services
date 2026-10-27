package org.sagebionetworks.repo.manager.curation.compute;

import org.sagebionetworks.repo.model.UserInfo;
import org.sagebionetworks.repo.model.curation.CurationTask;
import org.sagebionetworks.repo.model.curation.execution.ExecutableTaskExecutionDetails;
import org.sagebionetworks.repo.model.dao.asynch.AsyncJobProgressCallback;

/**
 * Interface for type-specific sub-workers that execute curation task computations.
 * Each implementation handles a specific concrete type of {@link ExecutableTaskExecutionDetails}.
 *
 * @param <T> The concrete execution details type this sub-worker handles.
 */
public interface ComputeTaskSubWorker<T extends ExecutableTaskExecutionDetails> {

	/**
	 * @return The class of ExecutableTaskExecutionDetails this sub-worker handles.
	 */
	Class<T> getExecutionDetailsType();

	/**
	 * Execute the computation for the given task.
	 *
	 * @param user The user who initiated the execution
	 * @param task The curation task to execute
	 * @param executionDetails The typed execution details
	 * @param callback Progress callback for the async job
	 * @return The updated execution details after successful execution
	 */
	T execute(UserInfo user, CurationTask task, T executionDetails, AsyncJobProgressCallback callback) throws Exception;

	/**
	 * @param executionDetails The execution details returned by {@link #execute}.
	 * @return False if the execution left something running, such as an interactive session, that stays
	 *         EXECUTING until {@link #end} is called; true if the task is ready for review.
	 */
	default boolean isComplete(T executionDetails) {
		return true;
	}

	/**
	 * End what {@link #execute} left running, so that the task can go to review. Only sub-workers whose
	 * executions can stay running support this.
	 *
	 * @return The updated execution details.
	 */
	default T end(UserInfo user, CurationTask task, T executionDetails, AsyncJobProgressCallback callback)
			throws Exception {
		throw new IllegalArgumentException("Task " + task.getTaskId() + " has nothing running to end.");
	}
}
