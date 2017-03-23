package org.sagebionetworks.repo.manager.dataaccess;

import org.sagebionetworks.repo.model.UserInfo;
import org.sagebionetworks.repo.model.dataaccess.AccessApprovalStatusRequest;
import org.sagebionetworks.repo.model.dataaccess.AccessApprovalStatusResults;
import org.sagebionetworks.repo.model.dataaccess.DataAccessSubmission;
import org.sagebionetworks.repo.model.dataaccess.DataAccessSubmissionOrder;
import org.sagebionetworks.repo.model.dataaccess.DataAccessSubmissionPage;
import org.sagebionetworks.repo.model.dataaccess.DataAccessSubmissionState;
import org.sagebionetworks.repo.model.dataaccess.DataAccessSubmissionStatus;
import org.sagebionetworks.repo.model.dataaccess.SubmissionStateChangeRequest;

public interface DataAccessSubmissionManager {

	/**
	 * Create a submission
	 * 
	 * @param userInfo
	 * @param requestId
	 * @param etag 
	 * @return
	 */
	public DataAccessSubmissionStatus create(UserInfo userInfo, String requestId, String etag);

	/**
	 * Retrieve a submission status that the user owns or is an accessor.
	 * 
	 * @param userInfo
	 * @param accessRequirementId
	 * @return
	 */
	public DataAccessSubmissionStatus getSubmissionStatus(UserInfo userInfo, String accessRequirementId);

	/**
	 * Cancel a submission.
	 * 
	 * @param userInfo
	 * @param submissionId
	 * @return
	 */
	public DataAccessSubmissionStatus cancel(UserInfo userInfo, String submissionId);

	/**
	 * Update the state of a submission.
	 * 
	 * @param userInfo
	 * @param request
	 * @return
	 */
	public DataAccessSubmission updateStatus(UserInfo userInfo, SubmissionStateChangeRequest request);

	/**
	 * List a page of submissions for a given access requirement.
	 * 
	 * @param userInfo
	 * @param accessRequirementId
	 * @param nextPageToken
	 * @param filterBy
	 * @param orderBy
	 * @param isAscending
	 * @return
	 */
	public DataAccessSubmissionPage listSubmission(UserInfo userInfo, String accessRequirementId, String nextPageToken,
			DataAccessSubmissionState filterBy, DataAccessSubmissionOrder orderBy, boolean isAscending);

	/**
	 * Retrieve approval status for a list of access requirements.
	 * 
	 * @param userInfo
	 * @param request
	 * @return
	 */
	public AccessApprovalStatusResults getAccessApprovalStatus(UserInfo userInfo, AccessApprovalStatusRequest request);

}
