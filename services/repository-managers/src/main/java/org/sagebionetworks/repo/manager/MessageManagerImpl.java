package org.sagebionetworks.repo.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;
import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.repo.manager.file.FileHandleManager;
import org.sagebionetworks.repo.manager.principal.SynapseEmailService;
import org.sagebionetworks.repo.manager.team.TeamConstants;
import org.sagebionetworks.repo.model.ACCESS_TYPE;
import org.sagebionetworks.repo.model.ACLInheritanceException;
import org.sagebionetworks.repo.model.AccessControlList;
import org.sagebionetworks.repo.model.AuthorizationConstants.BOOTSTRAP_PRINCIPAL;
import org.sagebionetworks.repo.model.AuthorizationUtils;
import org.sagebionetworks.repo.model.DatastoreException;
import org.sagebionetworks.repo.model.DomainType;
import org.sagebionetworks.repo.model.GroupMembersDAO;
import org.sagebionetworks.repo.model.MessageDAO;
import org.sagebionetworks.repo.model.Node;
import org.sagebionetworks.repo.model.NodeDAO;
import org.sagebionetworks.repo.model.ObjectType;
import org.sagebionetworks.repo.model.QueryResults;
import org.sagebionetworks.repo.model.ResourceAccess;
import org.sagebionetworks.repo.model.TooManyRequestsException;
import org.sagebionetworks.repo.model.UnauthorizedException;
import org.sagebionetworks.repo.model.UserGroup;
import org.sagebionetworks.repo.model.UserGroupDAO;
import org.sagebionetworks.repo.model.UserInfo;
import org.sagebionetworks.repo.model.UserProfile;
import org.sagebionetworks.repo.model.dao.FileHandleDao;
import org.sagebionetworks.repo.model.dao.NotificationEmailDAO;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.sagebionetworks.repo.model.message.MessageBundle;
import org.sagebionetworks.repo.model.message.MessageRecipientSet;
import org.sagebionetworks.repo.model.message.MessageSortBy;
import org.sagebionetworks.repo.model.message.MessageStatus;
import org.sagebionetworks.repo.model.message.MessageStatusType;
import org.sagebionetworks.repo.model.message.MessageToUser;
import org.sagebionetworks.repo.model.message.Settings;
import org.sagebionetworks.repo.model.principal.PrincipalAliasDAO;
import org.sagebionetworks.repo.transactions.WriteTransaction;
import org.sagebionetworks.repo.web.NotFoundException;
import org.springframework.beans.factory.annotation.Autowired;

import com.amazonaws.services.simpleemail.model.SendEmailRequest;
import com.amazonaws.services.simpleemail.model.SendRawEmailRequest;
import com.google.common.collect.Lists;


public class MessageManagerImpl implements MessageManager {
	
	static private Logger log = LogManager.getLogger(MessageManagerImpl.class);
	
	/**
	 * The maximum number of messages a user can create within a given interval
	 * The interval is defined by {@link #MESSAGE_CREATION_INTERVAL}
	 */
	private static final long MAX_NUMBER_OF_NEW_MESSAGES = 10L;
	
	/**
	 * The span of the interval, in milliseconds, in which created messages are counted
	 * See {@link #MAX_NUMBER_OF_NEW_MESSAGES}  
	 */
	private static final long MESSAGE_CREATION_INTERVAL_MILLISECONDS = 60000L;
	
	/**
	 * The maximum number of targets of a message
	 */
	protected static final long MAX_NUMBER_OF_RECIPIENTS = 50L;
	
	@Autowired
	private MessageDAO messageDAO;
	
	@Autowired
	private UserGroupDAO userGroupDAO;
	
	@Autowired
	private GroupMembersDAO groupMembersDAO;
	
	@Autowired
	private UserManager userManager;
	
	@Autowired
	private UserProfileManager userProfileManager;
	
	@Autowired
	private NotificationEmailDAO notificationEmailDao;
	
	@Autowired
	private PrincipalAliasDAO principalAliasDAO;
	
	@Autowired
	private AuthorizationManager authorizationManager;
	
	@Autowired
	private SynapseEmailService sesClient;
	
	@Autowired
	private FileHandleManager fileHandleManager;
	
	@Autowired
	FileHandleDao fileHandleDao;
	
	@Autowired
	private NodeDAO nodeDAO;
	
	@Autowired
	private EntityPermissionsManager entityPermissionsManager;
	
	public MessageManagerImpl() { };
	
	/**
	 * Used for testing
	 */
	public MessageManagerImpl(MessageDAO messageDAO, UserGroupDAO userGroupDAO,
			GroupMembersDAO groupMembersDAO, UserManager userManager,
			UserProfileManager userProfileManager,
			NotificationEmailDAO notificationEmailDao,
			PrincipalAliasDAO principalAliasDAO,
			AuthorizationManager authorizationManager,
			SynapseEmailService sesClient,
			FileHandleManager fileHandleManager, NodeDAO nodeDAO,
			EntityPermissionsManager entityPermissionsManager,
			FileHandleDao fileHandleDao) {
		this.messageDAO = messageDAO;
		this.userGroupDAO = userGroupDAO;
		this.groupMembersDAO = groupMembersDAO;
		this.userManager = userManager;
		this.userProfileManager = userProfileManager;
		this.notificationEmailDao = notificationEmailDao;
		this.principalAliasDAO = principalAliasDAO;
		this.authorizationManager = authorizationManager;
		this.sesClient = sesClient;
		this.fileHandleManager = fileHandleManager;
		this.nodeDAO = nodeDAO;
		this.entityPermissionsManager = entityPermissionsManager;
		this.fileHandleDao = fileHandleDao;
	}
	
	@Override
	public MessageToUser getMessage(UserInfo userInfo, String messageId) throws NotFoundException {
		MessageToUser message = messageDAO.getMessage(messageId);
		
		// Get the user's ID and the user's groups' IDs
		String userId = userInfo.getId().toString();
		Set<String> userGroups = new HashSet<String>();
		for (Long ug : userInfo.getGroups()) {
			userGroups.add(ug.toString());
		}
		userGroups.add(userId);
		
		// Is the user the sender?
		if (!message.getCreatedBy().equals(userId)) {
			// Is the user an intended recipient?
			boolean isRecipient = false;
			for (String recipient : message.getRecipients()) {
				if (userGroups.contains(recipient)) {
					isRecipient = true;
					break;
				}
			}
			
			// Not allowed to get the message
			if (!isRecipient) {
				throw new UnauthorizedException("You are not the sender or receiver of this message (" + messageId + ")");
			}
		}
		return message;
	}
	
	@Override
	public String getMessageFileRedirectURL(UserInfo userInfo, String messageId) throws NotFoundException {
		// If the user can get the message metadata (permission checking by the manager)
		// then the user can download the file
		MessageToUser dto = getMessage(userInfo, messageId);
		return fileHandleManager.getRedirectURLForFileHandle(dto.getFileHandleId());
	}

	@Override
	@WriteTransaction
	public MessageToUser createMessage(UserInfo userInfo, MessageToUser dto) throws NotFoundException {
		// Make sure the sender is correct
		dto.setCreatedBy(userInfo.getId().toString());
		
		if (!userInfo.isAdmin()) {
			// Can't be anonymous
			if (AuthorizationUtils.isUserAnonymous(userInfo)) {
				throw new UnauthorizedException("Anonymous user may not send messages.");
			}
			boolean userIsTrustedMessageSender = userInfo.getGroups().contains(TeamConstants.TRUSTED_MESSAGE_SENDER_TEAM_ID);
			// Throttle message creation
			if (!userIsTrustedMessageSender && !messageDAO.canCreateMessage(userInfo.getId().toString(), 
						MAX_NUMBER_OF_NEW_MESSAGES,
						MESSAGE_CREATION_INTERVAL_MILLISECONDS)) {
				throw new TooManyRequestsException(
						"Please slow down.  You may send a maximum of "
								+ MAX_NUMBER_OF_NEW_MESSAGES + " message(s) every "
								+ (MESSAGE_CREATION_INTERVAL_MILLISECONDS / 1000) + " second(s)");
			}
			
			// Limit the number of recipients
			if (!userIsTrustedMessageSender && dto.getRecipients() != null && dto.getRecipients().size() > MAX_NUMBER_OF_RECIPIENTS) {
				throw new IllegalArgumentException(
						"May not message more than "
								+ MAX_NUMBER_OF_RECIPIENTS
								+ " at once.  Consider grouping the recipients in a Team if possible.");
			}
		}
		
		if (!authorizationManager.canAccessRawFileHandleById(userInfo, dto.getFileHandleId()).getAuthorized()
				&& !messageDAO.canSeeMessagesUsingFileHandle(userInfo.getGroups(), dto.getFileHandleId())) {
			throw new UnauthorizedException("Invalid file handle given");
		}
		
		// Make sure the recipients all exist
		if (dto.getRecipients() != null) {
			List<UserGroup> ugs = userGroupDAO.get(Lists.newArrayList(dto.getRecipients()));
			if (ugs.size() != dto.getRecipients().size()) {
				throw new IllegalArgumentException("One or more of the following IDs are not recognized: " + dto.getRecipients());
			}
		}
		
		dto = messageDAO.createMessage(dto);
		
		// If the recipient list is only one element long, 
		// process and send the message in this transaction 
		if (dto.getRecipients().size() == 1) {
			UserGroup ug;
			try {
				ug = userGroupDAO.get(Long.parseLong(dto.getRecipients().iterator().next()));
			} catch (NotFoundException e) {
				throw new DatastoreException("Could not get a user group that satisfied message creation constraints");
			}
			
			// Defer the sending of messages to non-individuals 
			// since there could be more than one actual recipient after finding the members
			if (ug.getIsIndividual()) {
				List<String> errors;
				try {
					errors = processMessage(dto.getId(), true);
				} catch (NotFoundException e) {
					throw new DatastoreException("Could not find a message that was created in the same transaction");
				}
				if (errors.size() > 0) {
					throw new IllegalArgumentException(StringUtils.join(errors, "\n"));
				}
			}
		}
		return dto;
	}
	
	@Override
	@WriteTransaction
	public MessageToUser createMessageToEntityOwner(UserInfo userInfo,
			String entityId, MessageToUser toCreate) throws NotFoundException, ACLInheritanceException {
		// No permission checks since we only need to find the IDs of the creator of the node
		//   (or anyone with CHANGE_PERMISSIONS access)
		Node entity = nodeDAO.getNode(entityId);
		AccessControlList acl = entityPermissionsManager.getACL(entityId, userInfo);
		
		// Find all users with permission to change permissions
		Set<Long> sharers = new HashSet<Long>();
		for (ResourceAccess ra : acl.getResourceAccess()) {
			if (ra.getAccessType().contains(ACCESS_TYPE.CHANGE_PERMISSIONS)) {
				sharers.add(ra.getPrincipalId());
			}
		}

		if (toCreate.getRecipients() == null) {
			toCreate.setRecipients(new HashSet<String>());
		}
		
		// If the creator has permission, just message the creator
		if (sharers.contains(entity.getCreatedByPrincipalId())) {
			toCreate.getRecipients().add(entity.getCreatedByPrincipalId().toString());
			
		// Otherwise message everyone else
		} else {
			if (sharers.size() <= 0) {
				throw new UnauthorizedException("Unable to find a user with access to this entity.  Please contact a Synapse Administrator at synapseInfo@sagebase.org");
			}
			for (Long sharer : sharers) {
				toCreate.getRecipients().add(sharer.toString());
			}
		}
		
		// Create the message like normal
		return createMessage(userInfo, toCreate);
	}
	
	@Override
	@WriteTransaction
	public MessageToUser forwardMessage(UserInfo userInfo, String messageId,
			MessageRecipientSet recipients) throws NotFoundException {
		MessageToUser message = getMessage(userInfo, messageId);
		message.setRecipients(recipients.getRecipients());
		message.setInReplyTo(messageId);
		return createMessage(userInfo, message);
	}

	@Override
	public QueryResults<MessageToUser> getConversation(UserInfo userInfo, String associatedMessageId, 
			MessageSortBy sortBy, boolean descending, long limit, long offset) throws NotFoundException {
		MessageToUser dto = messageDAO.getMessage(associatedMessageId);
		String rootMessageId = dto.getInReplyToRoot();
		
		List<MessageToUser> dtos = messageDAO.getConversation(rootMessageId, userInfo.getId().toString(), 
				sortBy, descending, limit, offset);
		long totalMessages = messageDAO.getConversationSize(rootMessageId, userInfo.getId().toString());
		return new QueryResults<MessageToUser>(dtos, totalMessages);
	}

	@Override
	public QueryResults<MessageBundle> getInbox(UserInfo userInfo, 
			List<MessageStatusType> included, MessageSortBy sortBy, boolean descending, long limit, long offset) {
		List<MessageBundle> dtos = messageDAO.getReceivedMessages(userInfo.getId().toString(), 
				included, sortBy, descending, limit, offset);
		long totalMessages = messageDAO.getNumReceivedMessages(userInfo.getId().toString(), included);
		return new QueryResults<MessageBundle>(dtos, totalMessages);
	}

	@Override
	public QueryResults<MessageToUser> getOutbox(UserInfo userInfo, 
			MessageSortBy sortBy, boolean descending, long limit, long offset) {
		List<MessageToUser> dtos = messageDAO.getSentMessages(userInfo.getId().toString(), 
				sortBy, descending, limit, offset);
		long totalMessages = messageDAO.getNumSentMessages(userInfo.getId().toString());
		return new QueryResults<MessageToUser>(dtos, totalMessages);
	}

	@Override
	@WriteTransaction
	public void markMessageStatus(UserInfo userInfo, MessageStatus status) throws NotFoundException {
		// Check to see if the user can see the message being updated
		getMessage(userInfo, status.getMessageId());
		
		// Update the message
		status.setRecipientId(userInfo.getId().toString());
		boolean succeeded = messageDAO.updateMessageStatus_SameTransaction(status);
		
		if (!succeeded) {
			throw new UnauthorizedException("Cannot change status of message (" + status.getMessageId() + ")");
		}
	}

	@Override
	@WriteTransaction
	public List<String> processMessage(String messageId) throws NotFoundException {
		return processMessage(messageId, false);
	}
	
	/**
	 * See {@link #processMessage(String)}
	 * Also used by {@link #createMessage(UserInfo, MessageToUser)}
	 * 
	 * @param singleTransaction Should the sending be done in one transaction or one transaction per recipient?
	 *    This allows the sending of messages during creation to complete without deadlock. 
	 *    Note: It is crucial to pass in "true" when creating *and* sending a message in the same operation together. 
	 *      Otherwise the 'sending step' waits forever for the lock obtained by the 'creation step' to be released.
	 * General usage of this method sets this parameter to false.
	 * @throws  
	 */
	private List<String> processMessage(String messageId, boolean singleTransaction) throws NotFoundException {
		MessageToUser dto = messageDAO.getMessage(messageId);
		FileHandle fileHandle = fileHandleDao.get(dto.getFileHandleId());
		ContentType contentType = ContentType.parse(fileHandle.getContentType());
		String mimeType = contentType.getMimeType().trim().toLowerCase();

		try {
			String messageBody = fileHandleManager.downloadFileToString(dto.getFileHandleId());
			return processMessage(dto, singleTransaction, messageBody, mimeType);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * See {@link #processMessage(String, boolean)}
	 * Also used by {@link #sendTemplateEmail(String, String, String, boolean)}
	 * 
	 * @param messageBody The body of any email(s) that get sent as a result of processing this message
	 *    Note: This parameter is provided so that templated messages do not need to be uploaded then downloaded before sending
	 */
	private List<String> processMessage(
			MessageToUser dto, boolean singleTransaction, 
			String messageBody, String mimeType) throws NotFoundException {
		List<String> errors = new ArrayList<String>();
		
		// Check to see if the message has already been sent
		// If so, nothing else needs to be done
		if (messageDAO.getMessageSent(dto.getId())) {
			return errors;
		}

		UserInfo userInfo = userManager.getUserInfo(Long.parseLong(dto.getCreatedBy()));
		
		// in practice there is always a user name, but strictly speaking it is allowed to be missing
		String senderUserName = null;
		try {
			senderUserName = principalAliasDAO.getUserName(userInfo.getId());
		} catch (NotFoundException e) {
			senderUserName = null;
		}
		String senderDisplayName = null;
		try {
			UserProfile senderUserProfile = userProfileManager.getUserProfile(userInfo.getId().toString());
			senderDisplayName = EmailUtils.getDisplayName(senderUserProfile);
		} catch (NotFoundException e) {
			senderDisplayName = null;
		}
		
		// Get the individual recipients
		Set<String> recipients = expandRecipientSet(userInfo, dto.getRecipients(), errors);
		
		// Make sure the caller set the boolean correctly
		if (recipients.size() > 1 && singleTransaction) {
			throw new IllegalArgumentException("A message sent to multiple recipients must be done in separate transactions");
		}
		
		// Now that the recipients list has been expanded, begin processing the message
		for (String userId : recipients) {
			// Try to send messages to each user individually
			try {
				// Get the user's settings
				Settings settings = null;
				UserProfile profile = userProfileManager.getUserProfile(userId);
				settings = profile.getNotificationSettings();
				if(settings == null){
					settings = new Settings();
				}
				MessageStatusType userMessageStatus = null; // setting to null tells the DAO to use the default value
				// This marks a user as a recipient of the message
				if (singleTransaction) {
					messageDAO.createMessageStatus_SameTransaction(dto.getId(), userId, userMessageStatus);
				} else {
					messageDAO.createMessageStatus_NewTransaction(dto.getId(), userId, userMessageStatus);
				}
				
				// Should emails be sent?
				if (settings.getSendEmailNotifications() == null || settings.getSendEmailNotifications()) {
					String email = getEmailForUser(Long.parseLong(userId));
					if (ContentType.APPLICATION_JSON.getMimeType().equals(mimeType)) {
						SendRawEmailRequest sendRawEmailRequest = (new SendRawEmailRequestBuilder())
								.withRecipientEmail(email)
								.withSubject(dto.getSubject())
								.withBody(messageBody)
								.withSenderUserName(senderUserName)
								.withSenderDisplayName(senderDisplayName)
								.withUserId(userId)
								.withNotificationUnsubscribeEndpoint(dto.getNotificationUnsubscribeEndpoint())
								.build();
						sesClient.sendRawEmail(sendRawEmailRequest);
					} else {
						boolean isHtml= ContentType.TEXT_HTML.getMimeType().equals(mimeType);
						SendEmailRequest sendEmailRequest = (new SendEmailRequestBuilder())
								.withRecipientEmail(email)
								.withSubject(dto.getSubject())
								.withBody(messageBody)
								.withIsHtml(isHtml)
								.withSenderUserName(senderUserName)
								.withSenderDisplayName(senderDisplayName)
								.withUserId(userId)
								.withNotificationUnsubscribeEndpoint(dto.getNotificationUnsubscribeEndpoint())
								.build();
						sesClient.sendEmail(sendEmailRequest);
					}						
					// Should the message be marked as READ?
					if (settings.getMarkEmailedMessagesAsRead() != null && settings.getMarkEmailedMessagesAsRead()) {
						userMessageStatus = MessageStatusType.READ;
					}
				}
				
				if (userMessageStatus!=null) {
					// the status has been changed, so we have to update it again
					MessageStatus messageStatus = new MessageStatus();
					messageStatus.setMessageId(dto.getId());
					messageStatus.setRecipientId(userId);
					messageStatus.setStatus(userMessageStatus);
					if (singleTransaction) {
						messageDAO.updateMessageStatus_SameTransaction(messageStatus);
					} else {
						messageDAO.updateMessageStatus_NewTransaction(messageStatus);
					}
				}
			} catch (Exception e) {
				log.info("Error caught while processing message", e);
				errors.add("Failed while processing message for recipient (" + userId + "): " + e.getMessage());
			}
		}
		
		messageDAO.updateMessageTransmissionAsComplete(dto.getId());
		
		return errors;
	}
	
	/**
	 * Helper for {@link #processMessage(String, boolean, String)}
	 * 
	 * Takes a set of user IDs and expands it into a set of individuals that the user is permitted to message
	 */
	private Set<String> expandRecipientSet(UserInfo userInfo, Set<String> intendedRecipients, List<String> errors) throws NotFoundException {
		// From the list of intended recipients, filter out the un-permitted recipients
		Set<String> recipients = new HashSet<String>();
		for (String principalId : intendedRecipients) {
			Long principalIdLong = Long.parseLong(principalId);
			UserGroup ug;
			try {
				ug = userGroupDAO.get(principalIdLong);
			} catch (NotFoundException e) {
				errors.add(e.getMessage());
				continue;
			}
			
			boolean userIsTrustedMessageSender = userInfo.getGroups().contains(TeamConstants.TRUSTED_MESSAGE_SENDER_TEAM_ID);
			// Check permissions to send to non-individuals
			if (!userIsTrustedMessageSender &&
					!ug.getIsIndividual() &&
					!authorizationManager.canAccess(userInfo, principalId, ObjectType.TEAM, ACCESS_TYPE.SEND_MESSAGE).getAuthorized()) {
				errors.add(userInfo.getId()
						+ " may not send messages to the group (" + principalId + ")");
				continue;
			}
			
			// The principal is a valid recipient
			if (ug.getIsIndividual()) {
				recipients.add(principalId);
			} else {
				// Handle the implicit group that contains all users
				// Note: only admins can pass the authorization check to reach this
				if (BOOTSTRAP_PRINCIPAL.AUTHENTICATED_USERS_GROUP.getPrincipalId().toString().equals(principalId)) {
					for (UserGroup member : userGroupDAO.getAllPrincipals()) {
						if (member.getIsIndividual() && !isNotEmailRecipient(member.getId())) {
							recipients.add(member.getId());
						}
					}
				}
				
				// Expand non-individuals into individuals
				for (UserGroup member : groupMembersDAO.getMembers(principalId)) {
					recipients.add(member.getId());
				}
			}
		}
		
		return recipients;
	}
	
	// this filters out bootstrapped principals, who are NOT email recipients
	private static boolean isNotEmailRecipient(String principalId) {
		for (BOOTSTRAP_PRINCIPAL pb : BOOTSTRAP_PRINCIPAL.values()) {
			if (pb.getPrincipalId().toString().equals(principalId)) return true;
		}
		return false;
	}
	
	@Override
	@WriteTransaction
	public void deleteMessage(UserInfo userInfo, String messageId) {
		if (!userInfo.isAdmin()) {
			throw new UnauthorizedException("Only admins may delete messages");
		}
		
		messageDAO.deleteMessage(messageId);
	}
	
	
	@Override
	@WriteTransaction
	public void sendPasswordResetEmail(Long recipientId, DomainType domain, String sessionToken) throws NotFoundException {
		// Build the subject and body of the message
		String domainString = WordUtils.capitalizeFully(domain.name());
		String subject = "Set " + domain + " Password";
		Map<String,String> fieldValues = new HashMap<String,String>();
		fieldValues.put(EmailUtils.TEMPLATE_KEY_ORIGIN_CLIENT, domainString);
		
		String alias = principalAliasDAO.getUserName(recipientId);
		UserProfile userProfile = userProfileManager.getUserProfile(recipientId.toString());
		String displayName = EmailUtils.getDisplayName(userProfile);

		fieldValues.put(EmailUtils.TEMPLATE_KEY_DISPLAY_NAME, alias);
		
		fieldValues.put(EmailUtils.TEMPLATE_KEY_USERNAME, alias);
		String webLink;
		switch (domain) {
		case SYNAPSE:
			webLink = "https://www.synapse.org/Portal.html#!PasswordReset:" + sessionToken;
			break;
		default:
			throw new IllegalArgumentException("Unknown origin client type: " + domain);
		}
		fieldValues.put(EmailUtils.TEMPLATE_KEY_WEB_LINK, webLink);
		String messageBody = EmailUtils.readMailTemplate("message/PasswordResetTemplate.txt", fieldValues);
		String email = getEmailForUser(recipientId);
		SendEmailRequest sendEmailRequest = (new SendEmailRequestBuilder())
				.withRecipientEmail(email)
				.withSubject(subject)
				.withBody(messageBody)
				.withIsHtml(false)
				.withSenderUserName(alias)
				.withSenderDisplayName(displayName)
				.withUserId(recipientId.toString())
				.build();
		sesClient.sendEmail(sendEmailRequest);
	}
	
	@Override
	@WriteTransaction
	public void sendWelcomeEmail(Long recipientId, DomainType domain, String notificationUnsubscribeEndpoint) throws NotFoundException {
		// Build the subject and body of the message
		String domainString = WordUtils.capitalizeFully(domain.name());
		String subject = "Welcome to " + domain + "!";
		Map<String,String> fieldValues = new HashMap<String,String>();
		fieldValues.put(EmailUtils.TEMPLATE_KEY_ORIGIN_CLIENT, domainString);
		
		String alias = principalAliasDAO.getUserName(recipientId);
		fieldValues.put(EmailUtils.TEMPLATE_KEY_DISPLAY_NAME, alias);
		
		fieldValues.put(EmailUtils.TEMPLATE_KEY_USERNAME, alias);
		String messageBody = EmailUtils.readMailTemplate("message/WelcomeTemplate.txt", fieldValues);
		String email = getEmailForUser(recipientId);
		SendEmailRequest sendEmailRequest = (new SendEmailRequestBuilder())
				.withRecipientEmail(email)
				.withSubject(subject)
				.withBody(messageBody)
				.withIsHtml(false)
				.withUserId(recipientId.toString())
				.withNotificationUnsubscribeEndpoint(notificationUnsubscribeEndpoint)
				.build();
		sesClient.sendEmail(sendEmailRequest);
	}
	
	@Override
	@WriteTransaction
	public void sendDeliveryFailureEmail(String messageId, List<String> errors) throws NotFoundException {
		// Build the subject and body of the message
		MessageToUser dto = messageDAO.getMessage(messageId);
		UserInfo sender = userManager.getUserInfo(Long.parseLong(dto.getCreatedBy()));
		String subject = "Message " + messageId + " Delivery Failure(s)";
		
		String alias = principalAliasDAO.getUserName(sender.getId());

		Map<String,String> fieldValues = new HashMap<String,String>();
		fieldValues.put(EmailUtils.TEMPLATE_KEY_DISPLAY_NAME, alias);
		
		fieldValues.put(EmailUtils.TEMPLATE_KEY_MESSAGE_ID, messageId);
		fieldValues.put(EmailUtils.TEMPLATE_KEY_DETAILS, "- " + StringUtils.join(errors, "\n- "));
		String email = getEmailForUser(sender.getId());
		String messageBody = EmailUtils.readMailTemplate("message/DeliveryFailureTemplate.txt", fieldValues);
		
		SendEmailRequest sendEmailRequest = (new SendEmailRequestBuilder())
				.withRecipientEmail(email)
				.withSubject(subject)
				.withBody(messageBody)
				.withIsHtml(false)
				.withNotificationUnsubscribeEndpoint(dto.getNotificationUnsubscribeEndpoint())
				.withUserId(sender.getId().toString())
				.build();
		sesClient.sendEmail(sendEmailRequest);

	}
	
	
	private String getEmailForUser(Long principalId) throws NotFoundException {
		return notificationEmailDao.getNotificationEmailForPrincipal(principalId);
	}

}
