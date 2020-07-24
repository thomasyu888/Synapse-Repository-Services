package org.sagebionetworks.repo.manager.dataaccess;

import org.sagebionetworks.repo.model.ObjectType;
import org.sagebionetworks.repo.model.message.ChangeMessage;
import org.sagebionetworks.repo.model.message.ChangeType;
import org.sagebionetworks.workers.util.aws.message.RecoverableMessageException;

/**
 * Manager to process revocation and renewal notifications for access approvals
 * 
 * @author Marco Marasca
 */
public interface AccessApprovalNotificationManager {

	// Do not re-send another notification if one was sent already within the last 7
	// days
	long REVOKE_RESEND_TIMEOUT_DAYS = 7;

	// Fake id for messages to that are not actually created, e.g. in staging we do not send the messages
	// to avoid having duplicate messages sent out
	long NO_MESSAGE_TO_USER = -1;

	// Do not process a change message if it's older than 24 hours
	long CHANGE_TIMEOUT_HOURS = 24;

	/**
	 * Process a change message for an access approval to check if a revocation notification should be sent out to the
	 * accessor
	 * 
	 * @param message The change message, only {@link ChangeType#UPDATE} changes on {@link ObjectType#ACCESS_APPROVAL}
	 *                are processed
	 * @throws RecoverableMessageException If the message cannot be processed at this time
	 */
	void processAccessApprovalChange(ChangeMessage message) throws RecoverableMessageException;

}