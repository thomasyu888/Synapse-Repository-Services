package org.sagebionetworks.repo.manager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.sagebionetworks.evaluation.manager.EvaluationPermissionsManager;
import org.sagebionetworks.evaluation.model.Evaluation;
import org.sagebionetworks.evaluation.model.Submission;
import org.sagebionetworks.reflection.model.PaginatedResults;
import org.sagebionetworks.repo.manager.file.FileHandleAuthorizationStatus;
import org.sagebionetworks.repo.manager.team.TeamConstants;
import org.sagebionetworks.repo.manager.trash.EntityInTrashCanException;
import org.sagebionetworks.repo.model.ACCESS_TYPE;
import org.sagebionetworks.repo.model.AccessApproval;
import org.sagebionetworks.repo.model.AccessApprovalDAO;
import org.sagebionetworks.repo.model.AccessControlListDAO;
import org.sagebionetworks.repo.model.AccessRequirement;
import org.sagebionetworks.repo.model.AccessRequirementDAO;
import org.sagebionetworks.repo.model.ActivityDAO;
import org.sagebionetworks.repo.model.DatastoreException;
import org.sagebionetworks.repo.model.EntityHeader;
import org.sagebionetworks.repo.model.NodeDAO;
import org.sagebionetworks.repo.model.ObjectType;
import org.sagebionetworks.repo.model.Reference;
import org.sagebionetworks.repo.model.RestrictableObjectDescriptor;
import org.sagebionetworks.repo.model.RestrictableObjectType;
import org.sagebionetworks.repo.model.TermsOfUseAccessApproval;
import org.sagebionetworks.repo.model.TermsOfUseAccessRequirement;
import org.sagebionetworks.repo.model.UnauthorizedException;
import org.sagebionetworks.repo.model.UserInfo;
import org.sagebionetworks.repo.model.VerificationDAO;
import org.sagebionetworks.repo.model.AuthorizationConstants.BOOTSTRAP_PRINCIPAL;
import org.sagebionetworks.repo.model.dao.FileHandleDao;
import org.sagebionetworks.repo.model.dao.WikiPageKey;
import org.sagebionetworks.repo.model.dao.discussion.DiscussionThreadDAO;
import org.sagebionetworks.repo.model.dao.discussion.ForumDAO;
import org.sagebionetworks.repo.model.discussion.DiscussionFilter;
import org.sagebionetworks.repo.model.discussion.DiscussionThreadBundle;
import org.sagebionetworks.repo.model.discussion.Forum;
import org.sagebionetworks.repo.model.evaluation.SubmissionDAO;
import org.sagebionetworks.repo.model.file.FileHandleAssociateType;
import org.sagebionetworks.repo.model.file.FileHandleAssociationManager;
import org.sagebionetworks.repo.model.provenance.Activity;
import org.sagebionetworks.repo.model.subscription.SubscriptionObjectType;
import org.sagebionetworks.repo.model.v2.dao.V2WikiPageDao;
import org.sagebionetworks.repo.web.NotFoundException;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class AuthorizationManagerImplUnitTest {

	@Mock
	private AccessRequirementDAO  mockAccessRequirementDAO;
	@Mock
	private AccessApprovalDAO mockAccessApprovalDAO;
	@Mock
	private ActivityDAO mockActivityDAO;
	@Mock
	private FileHandleDao mockFileHandleDao;
	@Mock
	private EntityPermissionsManager mockEntityPermissionsManager;
	@Mock
	private AccessControlListDAO mockAclDAO;
	@Mock
	private FileHandleAssociationManager mockFileHandleAssociationManager;
	@Mock
	private VerificationDAO mockVerificationDao;
	@Mock
	private NodeDAO mockNodeDao;
	@Mock
	private DiscussionThreadDAO mockThreadDao;
	@Mock
	private ForumDAO mockForumDao;
	@Mock
	private V2WikiPageDao mockWikiPageDaoV2;
	@Mock
	private SubmissionDAO mockSubmissionDAO;
	@Mock
	private Submission mockSubmission;
	@Mock
	private EvaluationPermissionsManager mockEvaluationPermissionsManager;
	@Mock
	private MessageManager mockMessageManager;

	private static String USER_PRINCIPAL_ID = "123";
	private static String EVAL_OWNER_PRINCIPAL_ID = "987";
	private static String EVAL_ID = "1234567";

	private AuthorizationManagerImpl authorizationManager;
	private UserInfo userInfo;
	private UserInfo adminUser;
	private Evaluation evaluation;
	private String threadId;
	private String forumId;
	private String projectId;
	private DiscussionThreadBundle bundle;
	private Forum forum;

	@Before
	public void setUp() throws Exception {

		MockitoAnnotations.initMocks(this);

		authorizationManager = new AuthorizationManagerImpl();
		ReflectionTestUtils.setField(authorizationManager, "accessRequirementDAO", mockAccessRequirementDAO);
		ReflectionTestUtils.setField(authorizationManager, "accessApprovalDAO", mockAccessApprovalDAO);
		ReflectionTestUtils.setField(authorizationManager, "activityDAO", mockActivityDAO);
		ReflectionTestUtils.setField(authorizationManager, "entityPermissionsManager", mockEntityPermissionsManager);
		ReflectionTestUtils.setField(authorizationManager, "fileHandleDao", mockFileHandleDao);
		ReflectionTestUtils.setField(authorizationManager, "aclDAO", mockAclDAO);
		ReflectionTestUtils.setField(authorizationManager, "nodeDao", mockNodeDao);
		ReflectionTestUtils.setField(authorizationManager, "fileHandleAssociationSwitch", mockFileHandleAssociationManager);
		ReflectionTestUtils.setField(authorizationManager, "verificationDao", mockVerificationDao);
		ReflectionTestUtils.setField(authorizationManager, "threadDao", mockThreadDao);
		ReflectionTestUtils.setField(authorizationManager, "forumDao", mockForumDao);
		ReflectionTestUtils.setField(authorizationManager, "wikiPageDaoV2", mockWikiPageDaoV2);
		ReflectionTestUtils.setField(authorizationManager, "submissionDAO", mockSubmissionDAO);
		ReflectionTestUtils.setField(authorizationManager, "evaluationPermissionsManager", mockEvaluationPermissionsManager);
		ReflectionTestUtils.setField(authorizationManager, "messageManager", mockMessageManager);

		userInfo = new UserInfo(false, USER_PRINCIPAL_ID);
		adminUser = new UserInfo(true, 456L);

		evaluation = new Evaluation();
		evaluation.setId(EVAL_ID);
		evaluation.setOwnerId(EVAL_OWNER_PRINCIPAL_ID);

		List<ACCESS_TYPE> participateAndDownload = new ArrayList<ACCESS_TYPE>();
		participateAndDownload.add(ACCESS_TYPE.DOWNLOAD);
		participateAndDownload.add(ACCESS_TYPE.PARTICIPATE);

		when(mockAccessRequirementDAO.getAllUnmetAccessRequirements(
				any(List.class),
				any(RestrictableObjectType.class), any(Collection.class), eq(participateAndDownload))).
				thenReturn(new ArrayList<Long>());
		
		when(mockFileHandleAssociationManager.getAuthorizationObjectTypeForAssociatedObjectType(FileHandleAssociateType.FileEntity)).thenReturn(ObjectType.ENTITY);
		when(mockFileHandleAssociationManager.getAuthorizationObjectTypeForAssociatedObjectType(FileHandleAssociateType.TableEntity)).thenReturn(ObjectType.ENTITY);
		when(mockFileHandleAssociationManager.getAuthorizationObjectTypeForAssociatedObjectType(FileHandleAssociateType.WikiAttachment)).thenReturn(ObjectType.WIKI);

		threadId = "0";
		forumId = "1";
		projectId = "syn123";
		bundle = new DiscussionThreadBundle();
		bundle.setForumId(forumId);
		bundle.setProjectId(projectId);
		forum = new Forum();
		forum.setId(forumId);
		forum.setProjectId(projectId);
		when(mockThreadDao.getThread(Mockito.anyLong(), Mockito.any(DiscussionFilter.class))).thenReturn(bundle);
	}

	private PaginatedResults<Reference> generateQueryResults(int numResults, int total) {
		PaginatedResults<Reference> results = new PaginatedResults<Reference>();
		List<Reference> resultList = new ArrayList<Reference>();		
		for(int i=0; i<numResults; i++) {
			Reference ref = new Reference();
			ref.setTargetId("nodeId");
			resultList.add(ref);
		}
		results.setResults(resultList);
		results.setTotalNumberOfResults(total);
		return results;
	}

	@Test
	public void testCanAccessActivityPagination() throws Exception {		 
		Activity act = new Activity();
		String actId = "1";
		int limit = 1000;
		int total = 2001;
		int offset = 0;
		// create as admin, try to access as user so fails access and tests pagination
		act.setId(actId);
		act.setCreatedBy(adminUser.getId().toString());
		when(mockActivityDAO.get(actId)).thenReturn(act);
		PaginatedResults<Reference> results1 = generateQueryResults(limit, total);
		PaginatedResults<Reference> results2 = generateQueryResults(total-limit, total);		
		PaginatedResults<Reference> results3 = generateQueryResults(total-(2*limit), total);
		when(mockActivityDAO.getEntitiesGeneratedBy(actId, limit, offset)).thenReturn(results1);
		when(mockActivityDAO.getEntitiesGeneratedBy(actId, limit, offset+limit)).thenReturn(results2);		
		when(mockActivityDAO.getEntitiesGeneratedBy(actId, limit, offset+(2*limit))).thenReturn(results3);

		boolean canAccess = authorizationManager.canAccessActivity(userInfo, actId).getAuthorized();
		verify(mockActivityDAO).getEntitiesGeneratedBy(actId, limit, offset);
		verify(mockActivityDAO).getEntitiesGeneratedBy(actId, limit, offset+limit);
		verify(mockActivityDAO).getEntitiesGeneratedBy(actId, limit, offset+(2*limit));
		assertFalse(canAccess);
	}

	@Test
	public void testCanAccessActivityPaginationSmallResultSet() throws Exception {		 
		Activity act = new Activity();
		String actId = "1";
		int limit = 1000;
		int offset = 0;
		// create as admin, try to access as user so fails access and tests pagination
		act.setId(actId);
		act.setCreatedBy(adminUser.getId().toString());
		when(mockActivityDAO.get(actId)).thenReturn(act);
		PaginatedResults<Reference> results1 = generateQueryResults(1, 1);		
		when(mockActivityDAO.getEntitiesGeneratedBy(actId, limit, offset)).thenReturn(results1);		

		boolean canAccess = authorizationManager.canAccessActivity(userInfo, actId).getAuthorized();
		verify(mockActivityDAO).getEntitiesGeneratedBy(actId, limit, offset);
		assertFalse(canAccess);
	}

	@Test
	public void testCanAccessRawFileHandleByCreator(){
		// The admin can access anything
		String creator = userInfo.getId().toString();
		assertTrue("Admin should have access to all FileHandles",authorizationManager.canAccessRawFileHandleByCreator(adminUser, "101", creator).getAuthorized());
		assertTrue("Creator should have access to their own FileHandles", authorizationManager.canAccessRawFileHandleByCreator(userInfo, "101", creator).getAuthorized());
		// Set the creator to be the admin this time.
		creator = adminUser.getId().toString();
		assertFalse("Only the creator (or admin) should have access a FileHandle", authorizationManager.canAccessRawFileHandleByCreator(userInfo, "101", creator).getAuthorized());
	}

	@Test
	public void testCanAccessRawFileHandleById() throws NotFoundException{
		// The admin can access anything
		String creator = userInfo.getId().toString();
		String fileHandlId = "3333";
		when(mockFileHandleDao.getHandleCreator(fileHandlId)).thenReturn(creator);
		assertTrue("Admin should have access to all FileHandles",authorizationManager.canAccessRawFileHandleById(adminUser, fileHandlId).getAuthorized());
		assertTrue("Creator should have access to their own FileHandles", authorizationManager.canAccessRawFileHandleById(userInfo, fileHandlId).getAuthorized());
		// change the users id
		UserInfo notTheCreatoro = new UserInfo(false, "999999");
		assertFalse("Only the creator (or admin) should have access a FileHandle", authorizationManager.canAccessRawFileHandleById(notTheCreatoro, fileHandlId).getAuthorized());
		verify(mockFileHandleDao, times(2)).getHandleCreator(fileHandlId);
	}

	@Test
	public void testCanAccessRawFileHandlesByIds() throws NotFoundException {
		// The admin can access anything
		Multimap<String, String> creators = ArrayListMultimap.create();
		creators.put(userInfo.getId().toString(), "3333");
		creators.put(userInfo.getId().toString(), "4444");
		List<String> fileHandlIds = Lists.newArrayList("3333", "4444");
		when(mockFileHandleDao.getHandleCreators(fileHandlIds)).thenReturn(creators);
		Set<String> allowed = Sets.newHashSet();
		Set<String> disallowed = Sets.newHashSet();
		authorizationManager.canAccessRawFileHandlesByIds(adminUser, fileHandlIds, allowed, disallowed);
		assertEquals("Admin should have access to all FileHandles", 2, allowed.size());
		assertEquals("Admin should have access to all FileHandles", 0, disallowed.size());

		allowed.clear();
		disallowed.clear();
		authorizationManager.canAccessRawFileHandlesByIds(userInfo, fileHandlIds, allowed, disallowed);
		assertEquals("Creator should have access to their own FileHandles", 2, allowed.size());
		assertEquals("Creator should have access to their own FileHandles", 0, disallowed.size());

		// change the users id
		UserInfo notTheCreator = new UserInfo(false, "999999");
		allowed.clear();
		disallowed.clear();
		authorizationManager.canAccessRawFileHandlesByIds(notTheCreator, fileHandlIds, allowed, disallowed);
		assertEquals("Only the creator (or admin) should have access a FileHandle", 0, allowed.size());
		assertEquals("Only the creator (or admin) should have access a FileHandle", 2, disallowed.size());

		verify(mockFileHandleDao, times(2)).getHandleCreators(fileHandlIds);
	}

	@Test
	public void testCanAccessRawFileHandlesByIdsEmptyList() throws NotFoundException {
		authorizationManager.canAccessRawFileHandlesByIds(adminUser, Lists.<String> newArrayList(), null, null);
		verifyZeroInteractions(mockFileHandleDao);
	}

	@Test
	public void testCanAccessWithObjectTypeEntityAllow() throws DatastoreException, NotFoundException{
		String entityId = "syn123";
		when(mockEntityPermissionsManager.hasAccess(any(String.class), any(ACCESS_TYPE.class), eq(userInfo))).thenReturn(AuthorizationManagerUtil.AUTHORIZED);
		assertTrue("User should have acces to do anything with this entity", authorizationManager.canAccess(userInfo, entityId, ObjectType.ENTITY, ACCESS_TYPE.DELETE).getAuthorized());
	}

	@Test
	public void testCanAccessWithObjectTypeEntityDeny() throws DatastoreException, NotFoundException{
		String entityId = "syn123";
		when(mockEntityPermissionsManager.hasAccess(any(String.class), any(ACCESS_TYPE.class), eq(userInfo))).thenReturn(AuthorizationManagerUtil.ACCESS_DENIED);
		assertFalse("User should not have acces to do anything with this entity", authorizationManager.canAccess(userInfo, entityId, ObjectType.ENTITY, ACCESS_TYPE.DELETE).getAuthorized());
	}

	@Test(expected=EntityInTrashCanException.class)
	public void testCanAccessWithTrashCanException() throws DatastoreException, NotFoundException{
		when(mockEntityPermissionsManager.hasAccess(eq("syn123"), any(ACCESS_TYPE.class), eq(userInfo))).thenThrow(new EntityInTrashCanException(""));
		authorizationManager.canAccess(userInfo, "syn123", ObjectType.ENTITY, ACCESS_TYPE.READ);
	}

	@Test
	public void testVerifyACTTeamMembershipOrIsAdmin_Admin() {
		UserInfo adminInfo = new UserInfo(true);
		assertTrue(authorizationManager.isACTTeamMemberOrAdmin(adminInfo));
	}

	@Test
	public void testVerifyACTTeamMembershipOrIsAdmin_ACT() {
		userInfo.getGroups().add(TeamConstants.ACT_TEAM_ID);
		assertTrue(authorizationManager.isACTTeamMemberOrAdmin(userInfo));
	}

	@Test
	public void testVerifyACTTeamMembershipOrIsAdmin_NONE() {
		assertFalse(authorizationManager.isACTTeamMemberOrAdmin(userInfo));
	}

	private static RestrictableObjectDescriptor createEntitySubjectId() {
		RestrictableObjectDescriptor subjectId = new RestrictableObjectDescriptor();
		subjectId.setType(RestrictableObjectType.ENTITY);
		subjectId.setId("101");
		return subjectId;
	}

	private AccessRequirement createEntityAccessRequirement() throws Exception {
		TermsOfUseAccessRequirement ar = new TermsOfUseAccessRequirement();
		ar.setSubjectIds(Arrays.asList(new RestrictableObjectDescriptor[]{createEntitySubjectId()}));
		ar.setId(1234L);
		when(mockAccessRequirementDAO.get(ar.getId().toString())).thenReturn(ar);
		return ar;
	}

	private AccessApproval createEntityAccessApproval() throws Exception {
		AccessRequirement ar = createEntityAccessRequirement();
		TermsOfUseAccessApproval aa = new TermsOfUseAccessApproval();
		aa.setAccessorId(userInfo.getId().toString());
		aa.setId(656L);
		aa.setRequirementId(ar.getId());
		when(mockAccessApprovalDAO.get(aa.getId().toString())).thenReturn(aa);
		return aa;
	}

	private static RestrictableObjectDescriptor createEvaluationSubjectId() {
		RestrictableObjectDescriptor subjectId = new RestrictableObjectDescriptor();
		subjectId.setType(RestrictableObjectType.EVALUATION);
		subjectId.setId(EVAL_ID);
		return subjectId;
	}

	private AccessRequirement createEvaluationAccessRequirement() throws Exception {
		TermsOfUseAccessRequirement ar = new TermsOfUseAccessRequirement();
		ar.setSubjectIds(Arrays.asList(new RestrictableObjectDescriptor[]{createEvaluationSubjectId()}));
		ar.setId(1234L);
		when(mockAccessRequirementDAO.get(ar.getId().toString())).thenReturn(ar);
		return ar;
	}

	private AccessApproval createEvaluationAccessApproval() throws Exception {
		AccessRequirement ar = createEvaluationAccessRequirement();
		TermsOfUseAccessApproval aa = new TermsOfUseAccessApproval();
		aa.setAccessorId(userInfo.getId().toString());
		aa.setId(656L);
		aa.setRequirementId(ar.getId());
		when(mockAccessApprovalDAO.get(aa.getId().toString())).thenReturn(aa);
		return aa;
	}

	@Test
	public void testCanAccessEntityAccessRequirement() throws Exception {
		AccessRequirement ar = createEntityAccessRequirement();
		assertFalse(authorizationManager.canAccess(userInfo, ar.getId().toString(), ObjectType.ACCESS_REQUIREMENT, ACCESS_TYPE.UPDATE).getAuthorized());
		userInfo.getGroups().add(TeamConstants.ACT_TEAM_ID);
		assertTrue(authorizationManager.canAccess(userInfo, "1234", ObjectType.ACCESS_REQUIREMENT, ACCESS_TYPE.UPDATE).getAuthorized());
	}

	@Test
	public void testCanAccessEvaluationAccessRequirement() throws Exception {
		AccessRequirement ar = createEvaluationAccessRequirement();
		assertFalse(authorizationManager.canAccess(userInfo, ar.getId().toString(), ObjectType.ACCESS_REQUIREMENT, ACCESS_TYPE.UPDATE).getAuthorized());
		userInfo.setId(Long.parseLong(EVAL_OWNER_PRINCIPAL_ID));
		// only ACT may update an access requirement
		assertFalse(authorizationManager.canAccess(userInfo, ar.getId().toString(), ObjectType.ACCESS_REQUIREMENT, ACCESS_TYPE.UPDATE).getAuthorized());
	}

	@Test
	public void testCanAccessEntityAccessApproval() throws Exception {
		AccessApproval aa = createEntityAccessApproval();
		assertFalse(authorizationManager.canAccess(userInfo, aa.getId().toString(), ObjectType.ACCESS_APPROVAL, ACCESS_TYPE.READ).getAuthorized());
		userInfo.getGroups().add(TeamConstants.ACT_TEAM_ID);
		assertTrue(authorizationManager.canAccess(userInfo, aa.getId().toString(), ObjectType.ACCESS_APPROVAL, ACCESS_TYPE.READ).getAuthorized());
	}

	@Test
	public void testCanAccessEvaluationAccessApproval() throws Exception {
		AccessApproval aa = createEvaluationAccessApproval();
		assertFalse(authorizationManager.canAccess(userInfo, aa.getId().toString(), ObjectType.ACCESS_APPROVAL, ACCESS_TYPE.READ).getAuthorized());
		userInfo.setId(Long.parseLong(EVAL_OWNER_PRINCIPAL_ID));
		// only ACT may review access approvals
		assertFalse(authorizationManager.canAccess(userInfo, aa.getId().toString(), ObjectType.ACCESS_APPROVAL, ACCESS_TYPE.READ).getAuthorized());
	}

	@Test
	public void testCanCreateEntityAccessRequirement() throws Exception {
		AccessRequirement ar = createEntityAccessRequirement();
		assertFalse(authorizationManager.canCreateAccessRequirement(userInfo, ar).getAuthorized());
		userInfo.getGroups().add(TeamConstants.ACT_TEAM_ID); 
		assertTrue(authorizationManager.canCreateAccessRequirement(userInfo, ar).getAuthorized());
		userInfo.getGroups().remove(TeamConstants.ACT_TEAM_ID);
		assertFalse(authorizationManager.canCreateAccessRequirement(userInfo, ar).getAuthorized());
		// give user edit ability on entity 101
		when(mockEntityPermissionsManager.hasAccess(eq("101"), any(ACCESS_TYPE.class), eq(userInfo))).thenReturn(AuthorizationManagerUtil.AUTHORIZED);
		// only ACT may create access requirements
		assertFalse(authorizationManager.canCreateAccessRequirement(userInfo, ar).getAuthorized());
	}

	@Test
	public void testCanAccessEntityAccessApprovalsForSubject() throws Exception {
		assertFalse(authorizationManager.canAccessAccessApprovalsForSubject(userInfo, createEntitySubjectId(), ACCESS_TYPE.READ).getAuthorized());
		userInfo.getGroups().add(TeamConstants.ACT_TEAM_ID);
		assertTrue(authorizationManager.canAccessAccessApprovalsForSubject(userInfo, createEntitySubjectId(), ACCESS_TYPE.READ).getAuthorized());
	}

	@Test
	public void testCanAccessEvaluationAccessApprovalsForSubject() throws Exception {
		assertFalse(authorizationManager.canAccessAccessApprovalsForSubject(userInfo, createEvaluationSubjectId(), ACCESS_TYPE.READ).getAuthorized());
		userInfo.setId(Long.parseLong(EVAL_OWNER_PRINCIPAL_ID));
		// only ACT may review access approvals
		assertFalse(authorizationManager.canAccessAccessApprovalsForSubject(userInfo, createEvaluationSubjectId(), ACCESS_TYPE.READ).getAuthorized());
	}
	
	@Test
	public void testCanAccessTeam() throws Exception {
		String teamId = "123";
		ACCESS_TYPE accessType = ACCESS_TYPE.TEAM_MEMBERSHIP_UPDATE;
		// admin can always access
		assertTrue(authorizationManager.canAccess(adminUser, teamId, ObjectType.TEAM, accessType).getAuthorized());
		// non admin can access if acl says so
		when(mockAclDAO.canAccess(userInfo.getGroups(), teamId, ObjectType.TEAM, accessType)).thenReturn(true);
		assertTrue(authorizationManager.canAccess(userInfo, teamId, ObjectType.TEAM, accessType).getAuthorized());
		// otherwise not
		when(mockAclDAO.canAccess(userInfo.getGroups(), teamId, ObjectType.TEAM, accessType)).thenReturn(false);
		assertFalse(authorizationManager.canAccess(userInfo, teamId, ObjectType.TEAM, accessType).getAuthorized());
	}

	@Test
	public void testCanDownloadTeamIcon() throws Exception {
		assertTrue(authorizationManager.canAccess(userInfo, "123", ObjectType.TEAM, ACCESS_TYPE.DOWNLOAD).getAuthorized());
	}

	@Test
	public void testCanAccessWiki() throws Exception {
		String wikiId = "1";
		String entityId = "syn123";
		WikiPageKey key = new WikiPageKey();
		key.setOwnerObjectId(entityId);
		when(mockWikiPageDaoV2.lookupWikiKey(wikiId)).thenReturn(key);
		when(mockEntityPermissionsManager.hasAccess(entityId, ACCESS_TYPE.READ, userInfo))
				.thenReturn(AuthorizationManagerUtil.AUTHORIZED);
		assertTrue(authorizationManager.canAccess(userInfo, wikiId, ObjectType.WIKI, ACCESS_TYPE.DOWNLOAD).getAuthorized());
	}

	@Test
	public void testCanAccessUserProfile() throws Exception {
		assertTrue(authorizationManager.canAccess(userInfo, userInfo.getId().toString(), ObjectType.USER_PROFILE, ACCESS_TYPE.DOWNLOAD).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, userInfo.getId().toString(), ObjectType.USER_PROFILE, ACCESS_TYPE.CHANGE_PERMISSIONS).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, userInfo.getId().toString(), ObjectType.USER_PROFILE, ACCESS_TYPE.CHANGE_SETTINGS).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, userInfo.getId().toString(), ObjectType.USER_PROFILE, ACCESS_TYPE.CREATE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, userInfo.getId().toString(), ObjectType.USER_PROFILE, ACCESS_TYPE.DELETE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, userInfo.getId().toString(), ObjectType.USER_PROFILE, ACCESS_TYPE.DELETE_SUBMISSION).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, userInfo.getId().toString(), ObjectType.USER_PROFILE, ACCESS_TYPE.MODERATE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, userInfo.getId().toString(), ObjectType.USER_PROFILE, ACCESS_TYPE.PARTICIPATE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, userInfo.getId().toString(), ObjectType.USER_PROFILE, ACCESS_TYPE.READ).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, userInfo.getId().toString(), ObjectType.USER_PROFILE, ACCESS_TYPE.READ_PRIVATE_SUBMISSION).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, userInfo.getId().toString(), ObjectType.USER_PROFILE, ACCESS_TYPE.SEND_MESSAGE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, userInfo.getId().toString(), ObjectType.USER_PROFILE, ACCESS_TYPE.SUBMIT).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, userInfo.getId().toString(), ObjectType.USER_PROFILE, ACCESS_TYPE.TEAM_MEMBERSHIP_UPDATE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, userInfo.getId().toString(), ObjectType.USER_PROFILE, ACCESS_TYPE.UPDATE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, userInfo.getId().toString(), ObjectType.USER_PROFILE, ACCESS_TYPE.UPDATE_SUBMISSION).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, userInfo.getId().toString(), ObjectType.USER_PROFILE, ACCESS_TYPE.UPLOAD).getAuthorized());
	}

	@Test
	public void testCanAccessEvaluationSubmissionAccessDenied() throws Exception {
		String submissionId = "1";
		String evalId = "2";
		when(mockSubmissionDAO.get(submissionId)).thenReturn(mockSubmission);
		when(mockSubmission.getEvaluationId()).thenReturn(evalId);
		when(mockEvaluationPermissionsManager.hasAccess(userInfo, evalId, ACCESS_TYPE.READ_PRIVATE_SUBMISSION)).thenReturn(AuthorizationManagerUtil.ACCESS_DENIED);
		assertFalse(authorizationManager.canAccess(userInfo, submissionId, ObjectType.EVALUATION_SUBMISSIONS, ACCESS_TYPE.DOWNLOAD).getAuthorized());
	}

	@Test
	public void testCanAccessEvaluationSubmissionAuthorized() throws Exception {
		String submissionId = "1";
		String evalId = "2";
		when(mockSubmissionDAO.get(submissionId)).thenReturn(mockSubmission);
		when(mockSubmission.getEvaluationId()).thenReturn(evalId);
		when(mockEvaluationPermissionsManager.hasAccess(userInfo, evalId, ACCESS_TYPE.READ_PRIVATE_SUBMISSION)).thenReturn(AuthorizationManagerUtil.AUTHORIZED);
		assertTrue(authorizationManager.canAccess(userInfo, submissionId, ObjectType.EVALUATION_SUBMISSIONS, ACCESS_TYPE.DOWNLOAD).getAuthorized());
	}

	@Test
	public void testCanAccessEvaluationSubmissionNonDownload() throws Exception {
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.EVALUATION_SUBMISSIONS, ACCESS_TYPE.CHANGE_PERMISSIONS).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.EVALUATION_SUBMISSIONS, ACCESS_TYPE.CHANGE_SETTINGS).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.EVALUATION_SUBMISSIONS, ACCESS_TYPE.CREATE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.EVALUATION_SUBMISSIONS, ACCESS_TYPE.DELETE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.EVALUATION_SUBMISSIONS, ACCESS_TYPE.DELETE_SUBMISSION).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.EVALUATION_SUBMISSIONS, ACCESS_TYPE.MODERATE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.EVALUATION_SUBMISSIONS, ACCESS_TYPE.PARTICIPATE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.EVALUATION_SUBMISSIONS, ACCESS_TYPE.READ).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.EVALUATION_SUBMISSIONS, ACCESS_TYPE.READ_PRIVATE_SUBMISSION).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.EVALUATION_SUBMISSIONS, ACCESS_TYPE.SEND_MESSAGE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.EVALUATION_SUBMISSIONS, ACCESS_TYPE.SUBMIT).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.EVALUATION_SUBMISSIONS, ACCESS_TYPE.TEAM_MEMBERSHIP_UPDATE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.EVALUATION_SUBMISSIONS, ACCESS_TYPE.UPDATE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.EVALUATION_SUBMISSIONS, ACCESS_TYPE.UPDATE_SUBMISSION).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.EVALUATION_SUBMISSIONS, ACCESS_TYPE.UPLOAD).getAuthorized());
	}

	@Test
	public void testCanAccessMessageAccessDenied() throws Exception {
		String messageId = "1";
		when(mockMessageManager.getMessage(userInfo, messageId)).thenThrow(new UnauthorizedException());
		assertFalse(authorizationManager.canAccess(userInfo, messageId, ObjectType.MESSAGE, ACCESS_TYPE.DOWNLOAD).getAuthorized());
	}

	@Test
	public void testCanAccessMessageAuthorized() throws Exception {
		String messageId = "1";
		assertTrue(authorizationManager.canAccess(userInfo, messageId, ObjectType.MESSAGE, ACCESS_TYPE.DOWNLOAD).getAuthorized());
	}

	@Test
	public void testCanAccessMessageNonDownload() throws Exception {
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.MESSAGE, ACCESS_TYPE.CHANGE_PERMISSIONS).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.MESSAGE, ACCESS_TYPE.CHANGE_SETTINGS).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.MESSAGE, ACCESS_TYPE.CREATE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.MESSAGE, ACCESS_TYPE.DELETE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.MESSAGE, ACCESS_TYPE.DELETE_SUBMISSION).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.MESSAGE, ACCESS_TYPE.MODERATE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.MESSAGE, ACCESS_TYPE.PARTICIPATE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.MESSAGE, ACCESS_TYPE.READ).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.MESSAGE, ACCESS_TYPE.READ_PRIVATE_SUBMISSION).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.MESSAGE, ACCESS_TYPE.SEND_MESSAGE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.MESSAGE, ACCESS_TYPE.SUBMIT).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.MESSAGE, ACCESS_TYPE.TEAM_MEMBERSHIP_UPDATE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.MESSAGE, ACCESS_TYPE.UPDATE).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.MESSAGE, ACCESS_TYPE.UPDATE_SUBMISSION).getAuthorized());
		assertFalse(authorizationManager.canAccess(userInfo, "1", ObjectType.MESSAGE, ACCESS_TYPE.UPLOAD).getAuthorized());
	}
	
	@Test
	public void testCanDownloadVerificationSubmission() throws Exception {
		String verificationId = "123";
		long verificationIdLong = Long.parseLong(verificationId);
		ACCESS_TYPE accessType = ACCESS_TYPE.DOWNLOAD;
		ObjectType ot = ObjectType.VERIFICATION_SUBMISSION;
		// admin can always access
		assertTrue(authorizationManager.canAccess(adminUser, verificationId, ot, accessType).getAuthorized());
		
		// owner can access
		when(mockVerificationDao.getVerificationSubmitter(verificationIdLong)).thenReturn(userInfo.getId());
		assertTrue(authorizationManager.canAccess(userInfo, verificationId, ot, accessType).getAuthorized());
		
		// non-owner can't access
		when(mockVerificationDao.getVerificationSubmitter(verificationIdLong)).thenReturn(userInfo.getId()*13);
		assertFalse(authorizationManager.canAccess(userInfo, verificationId, ot, accessType).getAuthorized());
		
		// ACT can access
		UserInfo actInfo = new UserInfo(false);
		actInfo.setId(999L);
		actInfo.setGroups(Collections.singleton(TeamConstants.ACT_TEAM_ID));
		when(mockVerificationDao.getVerificationSubmitter(verificationIdLong)).thenReturn(userInfo.getId()*13);
		assertTrue(authorizationManager.canAccess(actInfo, verificationId, ot, accessType).getAuthorized());
		
		// can't do other operations though
		assertFalse(authorizationManager.canAccess(actInfo, verificationId, ot, ACCESS_TYPE.UPDATE).getAuthorized());

	}
	
	private static void addEntityHeaderTo(String id, Collection<EntityHeader>c) {
		EntityHeader h = new EntityHeader(); 
		h.setId(id); 
		c.add(h);
	}
	
	@Test
	public void testCanMoveEntity() throws Exception {
		// mock nodeDao
		String parentId = "syn12345";
		List<String> ancestorIds = new ArrayList<String>();
		ancestorIds.add(parentId);
		ancestorIds.add("syn999");
		List<EntityHeader> parentAncestors = new ArrayList<EntityHeader>();
		for (String id: ancestorIds) {
			addEntityHeaderTo(id, parentAncestors);
		}
		when(mockNodeDao.getEntityPath(parentId)).thenReturn(parentAncestors);
		
		String newParentId = "syn6789";
		List<String> newAncestorIds = new ArrayList<String>();
		newAncestorIds.add(newParentId);
		newAncestorIds.add("syn888");
		List<EntityHeader> newParentAncestors = new ArrayList<EntityHeader>();
		for (String id: newAncestorIds) {
			addEntityHeaderTo(id, newParentAncestors);
		}
		when(mockNodeDao.getEntityPath(newParentId)).thenReturn(newParentAncestors);
		
		// mock accessRequirementDAO
		List<AccessRequirement> ars = new ArrayList<AccessRequirement>();
		AccessRequirement ar = new TermsOfUseAccessRequirement();
		ars.add(ar);
		when(mockAccessRequirementDAO.getAllAccessRequirementsForSubject(ancestorIds, RestrictableObjectType.ENTITY)).thenReturn(ars);
		when(mockAccessRequirementDAO.getAllAccessRequirementsForSubject(newAncestorIds, RestrictableObjectType.ENTITY)).thenReturn(ars);
		
		// since 'ars' list doesn't change, will return true
		assertTrue(authorizationManager.canUserMoveRestrictedEntity(userInfo, parentId, newParentId).getAuthorized());
		verify(mockNodeDao).getEntityPath(parentId);
		verify(mockAccessRequirementDAO).getAllAccessRequirementsForSubject(ancestorIds, RestrictableObjectType.ENTITY);
		
		// making MORE restrictive is OK
		List<AccessRequirement> mt = new ArrayList<AccessRequirement>(); // i.e, an empty list
		when(mockAccessRequirementDAO.getAllAccessRequirementsForSubject(ancestorIds, RestrictableObjectType.ENTITY)).thenReturn(mt);
		assertTrue(authorizationManager.canUserMoveRestrictedEntity(userInfo, parentId, newParentId).getAuthorized());

		// but making less restrictive is NOT OK
		when(mockAccessRequirementDAO.getAllAccessRequirementsForSubject(ancestorIds, RestrictableObjectType.ENTITY)).thenReturn(ars);
		when(mockAccessRequirementDAO.getAllAccessRequirementsForSubject(newAncestorIds, RestrictableObjectType.ENTITY)).thenReturn(mt);
		assertFalse(authorizationManager.canUserMoveRestrictedEntity(userInfo, parentId, newParentId).getAuthorized());
		
		// but if the user is an admin, will be true
		assertTrue(authorizationManager.canUserMoveRestrictedEntity(adminUser, parentId, newParentId).getAuthorized());
	}
	
	@Test
	public void testCanCreateToUAccessApproval() throws Exception {
		TermsOfUseAccessApproval accessApproval = new TermsOfUseAccessApproval();
		this.authorizationManager.canCreateAccessApproval(userInfo, accessApproval);
	}
	
	@Test
	public void testCanDownloadFileAdmin(){
		List<String> fileHandleIds = Arrays.asList("1","2");
		String associatedObjectId = "456";
		FileHandleAssociateType associationType = FileHandleAssociateType.TableEntity;
		// call under test.
		List<FileHandleAuthorizationStatus> results = authorizationManager.canDownloadFile(adminUser, fileHandleIds, associatedObjectId, associationType);
		assertNotNull(results);
		List<FileHandleAuthorizationStatus> expected = Arrays.asList(
				new FileHandleAuthorizationStatus("1", AuthorizationManagerUtil.AUTHORIZED),
				new FileHandleAuthorizationStatus("2", AuthorizationManagerUtil.AUTHORIZED)
		);
		
		assertEquals(expected, results);
	}
	
	@Test
	public void testCanDownloadFileFileCreator(){
		List<String> fileHandleIds = Arrays.asList("1","2");
		String associatedObjectId = "456";
		FileHandleAssociateType associationType = FileHandleAssociateType.TableEntity;
		//the user is not authorized to download the associated object.
		when(mockEntityPermissionsManager.hasAccess(associatedObjectId, ACCESS_TYPE.DOWNLOAD, userInfo)).thenReturn(AuthorizationManagerUtil.accessDenied("cause"));
		// user is the creator of "2"
		when(mockFileHandleDao.getFileHandleIdsCreatedByUser(userInfo.getId(), fileHandleIds)).thenReturn(Sets.newHashSet("2"));
		// neither file is associated with the object.
		Set<String> emptySet = Sets.newHashSet();
		when(mockFileHandleAssociationManager.getFileHandleIdsAssociatedWithObject(fileHandleIds, associatedObjectId, associationType)).thenReturn(emptySet);
		// call under test.
		List<FileHandleAuthorizationStatus> results = authorizationManager.canDownloadFile(userInfo, fileHandleIds, associatedObjectId, associationType);
		assertNotNull(results);
		List<FileHandleAuthorizationStatus> expected = Arrays.asList(
				new FileHandleAuthorizationStatus("1", AuthorizationManagerUtil.accessDeniedFileNotAssociatedWithObject("1", associatedObjectId,
						associationType)),
						// The user is the creator of this file so they can download it.
				new FileHandleAuthorizationStatus("2", AuthorizationManagerUtil.AUTHORIZED)
		);
		
		assertEquals(expected, results);
	}
	
	@Test
	public void testCanDownloadFileFileNotAssociated(){
		List<String> fileHandleIds = Arrays.asList("1","2");
		String associatedObjectId = "456";
		FileHandleAssociateType associationType = FileHandleAssociateType.TableEntity;
		Set<String> emptySet = Sets.newHashSet();
		// the user has download on the associated object.
		when(mockEntityPermissionsManager.hasAccess(associatedObjectId, ACCESS_TYPE.DOWNLOAD, userInfo)).thenReturn(AuthorizationManagerUtil.AUTHORIZED);
		// user is not the creator of either file.
		when(mockFileHandleDao.getFileHandleIdsCreatedByUser(userInfo.getId(), fileHandleIds)).thenReturn(emptySet);
		// neither file is associated with the object.
		when(mockFileHandleAssociationManager.getFileHandleIdsAssociatedWithObject(fileHandleIds, associatedObjectId, associationType)).thenReturn(emptySet);
		// call under test.
		List<FileHandleAuthorizationStatus> results = authorizationManager.canDownloadFile(userInfo, fileHandleIds, associatedObjectId, associationType);
		assertNotNull(results);
		List<FileHandleAuthorizationStatus> expected = Arrays.asList(
				new FileHandleAuthorizationStatus("1", AuthorizationManagerUtil.accessDeniedFileNotAssociatedWithObject("1", associatedObjectId,
						associationType)),
				new FileHandleAuthorizationStatus("2", AuthorizationManagerUtil.accessDeniedFileNotAssociatedWithObject("2", associatedObjectId,
						associationType))
		);
		assertEquals(expected, results);
	}
	
	@Test
	public void testCanDownloadFileAuthorized(){
		List<String> fileHandleIds = Arrays.asList("1","2");
		String associatedObjectId = "456";
		FileHandleAssociateType associationType = FileHandleAssociateType.TableEntity;
		Set<String> emptySet = Sets.newHashSet();
		//the user is authorized to download the associated object.
		when(mockEntityPermissionsManager.hasAccess(associatedObjectId, ACCESS_TYPE.DOWNLOAD, userInfo)).thenReturn(AuthorizationManagerUtil.AUTHORIZED);
		// user is not the creator of either file.
		when(mockFileHandleDao.getFileHandleIdsCreatedByUser(userInfo.getId(), fileHandleIds)).thenReturn(emptySet);
		// the first file is associated with the object.
		Set<String> bothFileHandlIds = Sets.newHashSet("1");
		when(mockFileHandleAssociationManager.getFileHandleIdsAssociatedWithObject(fileHandleIds, associatedObjectId, associationType)).thenReturn(bothFileHandlIds);
		// call under test.
		List<FileHandleAuthorizationStatus> results = authorizationManager.canDownloadFile(userInfo, fileHandleIds, associatedObjectId, associationType);
		assertNotNull(results);
		List<FileHandleAuthorizationStatus> expected = Arrays.asList(
				new FileHandleAuthorizationStatus("1", AuthorizationManagerUtil.AUTHORIZED),
				new FileHandleAuthorizationStatus("2", AuthorizationManagerUtil.accessDeniedFileNotAssociatedWithObject("2", associatedObjectId,
						associationType))
		);
		assertEquals(expected, results);
	}
	
	@Test
	public void testCanDownloadFileUnAuthorized(){
		List<String> fileHandleIds = Arrays.asList("1","2");
		String associatedObjectId = "456";
		FileHandleAssociateType associationType = FileHandleAssociateType.TableEntity;
		Set<String> emptySet = Sets.newHashSet();
		//the user is not authorized to download the associated object.
		when(mockEntityPermissionsManager.hasAccess(associatedObjectId, ACCESS_TYPE.DOWNLOAD, userInfo)).thenReturn(AuthorizationManagerUtil.accessDenied("cause"));
		// user is not the creator of either file.
		when(mockFileHandleDao.getFileHandleIdsCreatedByUser(userInfo.getId(), fileHandleIds)).thenReturn(emptySet);
		// the first file is associated with the object.
		Set<String> bothFileHandlIds = Sets.newHashSet("1");
		when(mockFileHandleAssociationManager.getFileHandleIdsAssociatedWithObject(fileHandleIds, associatedObjectId, associationType)).thenReturn(bothFileHandlIds);
		// call under test.
		List<FileHandleAuthorizationStatus> results = authorizationManager.canDownloadFile(userInfo, fileHandleIds, associatedObjectId, associationType);
		assertNotNull(results);
		List<FileHandleAuthorizationStatus> expected = Arrays.asList(
				new FileHandleAuthorizationStatus("1", AuthorizationManagerUtil.accessDenied("cause")),
				new FileHandleAuthorizationStatus("2", AuthorizationManagerUtil.accessDeniedFileNotAssociatedWithObject("2", associatedObjectId,
						associationType))
		);
		assertEquals(expected, results);
	}
	
	@Test
	public void testCanReadBenefactorsAdmin(){
		Set<Long> benefactors = Sets.newHashSet(1L,2L);
		// call under test
		Set<Long> results = authorizationManager.getAccessibleBenefactors(adminUser, benefactors);
		assertEquals(benefactors, results);
		verify(mockAclDAO, never()).getAccessibleBenefactors(any(Set.class), any(Set.class), any(ObjectType.class), any(ACCESS_TYPE.class));
	}
	
	@Test
	public void testCanReadBenefactorsNonAdmin(){
		Set<Long> benefactors = Sets.newHashSet(1L,2L);
		// call under test
		authorizationManager.getAccessibleBenefactors(userInfo, benefactors);
		verify(mockAclDAO, times(1)).getAccessibleBenefactors(any(Set.class), any(Set.class), any(ObjectType.class), any(ACCESS_TYPE.class));
	}
	
	@Test
	public void testCanReadBenefactorsTrashAdmin(){
		Set<Long> benefactors = Sets.newHashSet(AuthorizationManagerImpl.TRASH_FOLDER_ID);
		// call under test
		Set<Long> results = authorizationManager.getAccessibleBenefactors(adminUser, benefactors);
		assertNotNull(results);
		assertEquals(0, results.size());
	}
	
	@Test
	public void testCanReadBenefactorsTrashNonAdmin(){
		Set<Long> benefactors = Sets.newHashSet(AuthorizationManagerImpl.TRASH_FOLDER_ID);
		when(mockAclDAO.getAccessibleBenefactors(any(Set.class), any(Set.class), any(ObjectType.class), any(ACCESS_TYPE.class))).thenReturn(benefactors);
		// call under test
		Set<Long> results = authorizationManager.getAccessibleBenefactors(userInfo, benefactors);
		assertNotNull(results);
		assertEquals(0, results.size());
	}

	@Test
	public void testCanSubscribeForumUnauthorized() {
		when(mockEntityPermissionsManager.hasAccess(projectId, ACCESS_TYPE.READ, userInfo)).thenReturn(AuthorizationManagerUtil.ACCESS_DENIED);
		when(mockForumDao.getForum(Long.parseLong(forumId))).thenReturn(forum);
		assertEquals(AuthorizationManagerUtil.ACCESS_DENIED,
				authorizationManager.canSubscribe(userInfo, forumId, SubscriptionObjectType.FORUM));
	}

	@Test
	public void testCanSubscribeForumAuthorized() {
		when(mockEntityPermissionsManager.hasAccess(projectId, ACCESS_TYPE.READ, userInfo)).thenReturn(AuthorizationManagerUtil.AUTHORIZED);
		when(mockForumDao.getForum(Long.parseLong(forumId))).thenReturn(forum);
		assertEquals(AuthorizationManagerUtil.AUTHORIZED,
				authorizationManager.canSubscribe(userInfo, forumId, SubscriptionObjectType.FORUM));
	}

	@Test
	public void testCanSubscribeThreadUnauthorized() {
		when(mockEntityPermissionsManager.hasAccess(projectId, ACCESS_TYPE.READ, userInfo)).thenReturn(AuthorizationManagerUtil.ACCESS_DENIED);
		when(mockThreadDao.getProjectId(threadId)).thenReturn(projectId);
		assertEquals(AuthorizationManagerUtil.ACCESS_DENIED,
				authorizationManager.canSubscribe(userInfo, threadId, SubscriptionObjectType.THREAD));
	}

	@Test
	public void testCanSubscribeThreadAuthorized() {
		when(mockEntityPermissionsManager.hasAccess(projectId, ACCESS_TYPE.READ, userInfo)).thenReturn(AuthorizationManagerUtil.AUTHORIZED);
		when(mockThreadDao.getProjectId(threadId)).thenReturn(projectId);
		assertEquals(AuthorizationManagerUtil.AUTHORIZED,
				authorizationManager.canSubscribe(userInfo, threadId, SubscriptionObjectType.THREAD));
	}
	
	@Test
	public void testGetAccessibleProjectIds(){
		Set<Long> expectedProjectIds = Sets.newHashSet(555L);
		Set<Long> principalIds = Sets.newHashSet(123L);
		when(mockAclDAO.getAccessibleProjectIds(principalIds, ACCESS_TYPE.READ)).thenReturn(expectedProjectIds);
		Set<Long> results = authorizationManager.getAccessibleProjectIds(principalIds);
		assertEquals(expectedProjectIds,results);
	}
	
	@Test
	public void testGetAccessibleProjectIdsEmpty(){
		Set<Long> principalIds = new HashSet<>();
		Set<Long> results = authorizationManager.getAccessibleProjectIds(principalIds);
		assertNotNull(results);
		assertTrue(results.isEmpty());
		verify(mockAclDAO, never()).getAccessibleProjectIds(any(Set.class), any(ACCESS_TYPE.class));
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testGetAccessibleProjectIdsNullPrincipals(){
		Set<Long> principalIds = null;
		authorizationManager.getAccessibleProjectIds(principalIds);
	}
	
	/**
	 * This method cannot be called with PUBLIC_GROUP
	 */
	@Test (expected=IllegalArgumentException.class)
	public void testGetAccessibleProjectIdsWithPublic(){
		Set<Long> principalIds = Sets.newHashSet(BOOTSTRAP_PRINCIPAL.PUBLIC_GROUP.getPrincipalId().longValue());
		authorizationManager.getAccessibleProjectIds(principalIds);
	}
	
	/**
	 * This method cannot be called with AUTHENTICATED_USERS_GROUP
	 */
	@Test (expected=IllegalArgumentException.class)
	public void testGetAccessibleProjectIdsWithAuthenticated(){
		Set<Long> principalIds = Sets.newHashSet(BOOTSTRAP_PRINCIPAL.AUTHENTICATED_USERS_GROUP.getPrincipalId().longValue());
		authorizationManager.getAccessibleProjectIds(principalIds);
	}
	
	/**
	 * This method cannot be called with CERTIFIED_USERS.
	 */
	@Test (expected=IllegalArgumentException.class)
	public void testGetAccessibleProjectIdsWithCertified(){
		Set<Long> principalIds = Sets.newHashSet(BOOTSTRAP_PRINCIPAL.CERTIFIED_USERS.getPrincipalId().longValue());
		authorizationManager.getAccessibleProjectIds(principalIds);
	}
}
