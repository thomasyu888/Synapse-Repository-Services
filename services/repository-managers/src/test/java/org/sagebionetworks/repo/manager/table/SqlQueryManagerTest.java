package org.sagebionetworks.repo.manager.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sagebionetworks.database.semaphore.CountingSemaphore;
import org.sagebionetworks.repo.manager.entity.EntityAuthorizationManager;
import org.sagebionetworks.repo.manager.entity.EntityAuthorizationManager.TableIdAndType;
import org.sagebionetworks.repo.model.ACCESS_TYPE;
import org.sagebionetworks.repo.model.AuthorizationConstants;
import org.sagebionetworks.repo.model.ObjectType;
import org.sagebionetworks.repo.model.UnauthorizedException;
import org.sagebionetworks.repo.model.UserInfo;
import org.sagebionetworks.repo.model.auth.AuthorizationStatus;
import org.sagebionetworks.repo.model.dao.table.TableType;
import org.sagebionetworks.repo.model.entity.IdAndVersion;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.Row;
import org.sagebionetworks.repo.model.table.RowSet;
import org.sagebionetworks.repo.model.table.SqlQueryRequest;
import org.sagebionetworks.repo.model.table.SqlQueryResponse;
import org.sagebionetworks.repo.model.table.TableState;
import org.sagebionetworks.repo.model.table.TableStatus;
import org.sagebionetworks.repo.model.table.TableUnavailableException;
import org.sagebionetworks.table.cluster.ConnectionFactory;
import org.sagebionetworks.table.cluster.QueryTranslator;
import org.sagebionetworks.table.cluster.TableIndexDAO;
import org.sagebionetworks.table.cluster.TranslatedQuery;
import org.sagebionetworks.table.cluster.description.IndexDescription;
import org.sagebionetworks.table.cluster.description.MaterializedViewIndexDescription;
import org.sagebionetworks.table.cluster.description.TableIndexDescription;
import org.sagebionetworks.table.cluster.description.ViewIndexDescription;
import org.sagebionetworks.util.progress.ProgressCallback;
import org.sagebionetworks.util.progress.ProgressingCallable;
import org.sagebionetworks.workers.util.aws.message.RecoverableMessageException;
import org.springframework.dao.QueryTimeoutException;

@ExtendWith(MockitoExtension.class)
public class SqlQueryManagerTest {

	private static final String JOIN_SQL = "SELECT s.id AS id, p.arm AS arm FROM syn1 s JOIN syn2 p ON s.id = p.id";
	private static final String JOB_ID = "123";
	private static final IdAndVersion RESULT_ID = IdAndVersion
			.newBuilder().setId(SqlQueryManager.TRANSIENT_ID_OFFSET + 123).build();

	@Mock
	private TableManagerSupport mockTableManagerSupport;
	@Mock
	private EntityAuthorizationManager mockEntityAuthorizationManager;
	@Mock
	private ColumnModelManager mockColumnModelManager;
	@Mock
	private TableIndexConnectionFactory mockIndexConnectionFactory;
	@Mock
	private ConnectionFactory mockConnectionFactory;
	@Mock
	private CountingSemaphore mockCountingSemaphore;
	@Mock
	private TableIndexManager mockTableIndexManager;
	@Mock
	private TableIndexDAO mockTableIndexDao;
	@Mock
	private ProgressCallback mockCallback;

	@InjectMocks
	private SqlQueryManager manager;

	@Captor
	private ArgumentCaptor<QueryTranslator> translatorCaptor;
	@Captor
	private ArgumentCaptor<TranslatedQuery> readCaptor;

	private UserInfo user;
	private ColumnModel sampleId;
	private ColumnModel participantId;
	private ColumnModel arm;
	private ColumnModel resultId;
	private ColumnModel resultArm;

	@BeforeEach
	public void before() {
		user = new UserInfo(false, 77L, AuthorizationConstants.DEFAULT_REALM_ID);
		sampleId = new ColumnModel().setId("11").setName("id").setColumnType(ColumnType.INTEGER);
		participantId = new ColumnModel().setId("21").setName("id").setColumnType(ColumnType.INTEGER);
		arm = new ColumnModel().setId("22").setName("arm").setColumnType(ColumnType.STRING).setMaximumSize(50L);
		resultId = new ColumnModel().setId("31").setName("id").setColumnType(ColumnType.INTEGER);
		resultArm = new ColumnModel().setId("32").setName("arm").setColumnType(ColumnType.STRING)
				.setMaximumSize(50L);
	}

	@Test
	public void testLimitRows() {
		// call under test
		assertEquals("SELECT * FROM syn1 LIMIT 101 OFFSET 0", SqlQueryManager.limitRows("SELECT * FROM syn1", 101));
	}

	@Test
	public void testLimitRowsWithJoin() {
		// call under test
		assertEquals(JOIN_SQL + " LIMIT 101 OFFSET 0", SqlQueryManager.limitRows(JOIN_SQL, 101));
	}

	@Test
	public void testLimitRowsWithSmallerLimit() {
		// call under test
		assertEquals("SELECT * FROM syn1 LIMIT 5 OFFSET 2",
				SqlQueryManager.limitRows("SELECT * FROM syn1 LIMIT 5 OFFSET 2", 101));
	}

	@Test
	public void testLimitRowsWithLargerLimit() {
		// call under test
		assertEquals("SELECT * FROM syn1 ORDER BY id ASC LIMIT 101",
				SqlQueryManager.limitRows("SELECT * FROM syn1 ORDER BY id ASC LIMIT 5000", 101));
	}

	@Test
	public void testLimitRowsWithUnion() {
		String sql = "SELECT id FROM syn1 UNION SELECT id FROM syn2";

		// call under test
		assertEquals(sql, SqlQueryManager.limitRows(sql, 101));
	}

	@Test
	public void testRunQuery() throws Exception {
		setupSources(new TableIndexDescription(IdAndVersion.parse("syn1")),
				new TableIndexDescription(IdAndVersion.parse("syn2")));
		setupAuthorized();
		setupAvailable();
		setupLock();
		setupBuild();
		setupRows(2);

		// call under test
		SqlQueryResponse response = manager.runQuery(mockCallback, JOB_ID, user,
				new SqlQueryRequest().setSql(JOIN_SQL));

		assertEquals(2, response.getRowSet().getRows().size());
		assertEquals(null, response.getRowSet().getTableId());
		assertFalse(response.getIsTruncated());
		verify(mockEntityAuthorizationManager).canQueryTableOrView(user,
				List.of(new TableIdAndType("1", TableType.table), new TableIdAndType("2", TableType.table)));
		// No source is filtered by user, so the build stops one row past the limit.
		verify(mockTableIndexManager).populateIndexFromDefiningSql(eq(List.of(resultId, resultArm)),
				translatorCaptor.capture(), eq(SqlQueryManager.BUILD_TIMEOUT_SECONDS));
		assertEquals(JOIN_SQL + " LIMIT 101 OFFSET 0", translatorCaptor.getValue().getInputSql());
		// The result is read in build order, one row past the limit.
		assertTrue(readCaptor.getValue().getOutputSQL().contains("ORDER BY ROW_ID LIMIT :"),
				readCaptor.getValue().getOutputSQL());
		assertEquals(101L, readCaptor.getValue().getParameters().get("b0"));
		verify(mockTableIndexManager).deleteTableIndex(RESULT_ID);
		verify(mockCountingSemaphore).releaseLock("SQL_QUERY_USER_77", "token");
	}

	@Test
	public void testRunQueryWithMoreRowsThanTheLimit() throws Exception {
		setupSources(new TableIndexDescription(IdAndVersion.parse("syn1")),
				new TableIndexDescription(IdAndVersion.parse("syn2")));
		setupAuthorized();
		setupAvailable();
		setupLock();
		setupBuild();
		setupRows(3);

		// call under test
		SqlQueryResponse response = manager.runQuery(mockCallback, JOB_ID, user,
				new SqlQueryRequest().setSql(JOIN_SQL).setLimit(2L));

		assertEquals(2, response.getRowSet().getRows().size());
		assertTrue(response.getIsTruncated());
	}

	@Test
	public void testRunQueryWithViewSource() throws Exception {
		IdAndVersion viewId = IdAndVersion.parse("syn2");
		setupSources(new TableIndexDescription(IdAndVersion.parse("syn1")),
				new ViewIndexDescription(viewId, TableType.entityview, 1L));
		setupAuthorized();
		setupAvailable();
		setupLock();
		setupBuild();
		when(mockTableIndexDao.getDistinctLongValues(RESULT_ID, "ROW_BENEFACTOR__A1")).thenReturn(Set.of(5L, 6L));
		when(mockTableManagerSupport.getAccessibleBenefactors(user, ObjectType.ENTITY, Set.of(5L, 6L),
				ACCESS_TYPE.READ)).thenReturn(Set.of(5L));
		setupRows(1);

		// call under test
		manager.runQuery(mockCallback, JOB_ID, user, new SqlQueryRequest().setSql(JOIN_SQL));

		// Rows from a view are filtered after the build, so the build is not cut short.
		verify(mockTableIndexManager).populateIndexFromDefiningSql(any(), translatorCaptor.capture(), anyInt());
		assertEquals(JOIN_SQL, translatorCaptor.getValue().getInputSql());
		String readSql = readCaptor.getValue().getOutputSQL();
		assertTrue(readSql.contains("ROW_BENEFACTOR__A1 IN ("), readSql);
	}

	@Test
	public void testRunQueryWithAggregateOnlySource() throws Exception {
		setupSources(new TableIndexDescription(IdAndVersion.parse("syn1")),
				new TableIndexDescription(IdAndVersion.parse("syn2")));
		when(mockEntityAuthorizationManager.canQueryTableOrView(any(), any())).thenReturn(
				AuthorizationStatus.accessDeniedButAggregateAllowed("Download is not allowed", "syn2"));

		String message = assertThrows(UnauthorizedException.class, () -> {
			// call under test
			manager.runQuery(mockCallback, JOB_ID, user, new SqlQueryRequest().setSql(JOIN_SQL));
		}).getMessage();

		assertEquals(String.format(SqlQueryManager.AGGREGATE_ONLY_MESSAGE, "syn2"), message);
		verifyNoInteractions(mockCountingSemaphore, mockIndexConnectionFactory);
	}

	@Test
	public void testRunQueryWithUnreadableSource() throws Exception {
		setupSources(new TableIndexDescription(IdAndVersion.parse("syn1")),
				new TableIndexDescription(IdAndVersion.parse("syn2")));
		when(mockEntityAuthorizationManager.canQueryTableOrView(any(), any()))
				.thenReturn(AuthorizationStatus.accessDenied("You lack READ access to the requested entity."));

		String message = assertThrows(UnauthorizedException.class, () -> {
			// call under test
			manager.runQuery(mockCallback, JOB_ID, user, new SqlQueryRequest().setSql(JOIN_SQL));
		}).getMessage();

		assertEquals("You lack READ access to the requested entity.", message);
		verifyNoInteractions(mockCountingSemaphore, mockIndexConnectionFactory);
	}

	@Test
	public void testRunQueryWithSourceOfAView() throws Exception {
		// syn2 is a materialized view built from syn3: both must be readable.
		IdAndVersion viewId = IdAndVersion.parse("syn2");
		when(mockTableManagerSupport.getIndexDescription(IdAndVersion.parse("syn3")))
				.thenReturn(new TableIndexDescription(IdAndVersion.parse("syn3")));
		MaterializedViewIndexDescription view = new MaterializedViewIndexDescription(viewId, "SELECT * FROM syn3",
				mockTableManagerSupport);
		setupSources(new TableIndexDescription(IdAndVersion.parse("syn1")), view);
		when(mockEntityAuthorizationManager.canQueryTableOrView(any(), any()))
				.thenReturn(AuthorizationStatus.accessDenied("denied"));

		assertThrows(UnauthorizedException.class, () -> {
			// call under test
			manager.runQuery(mockCallback, JOB_ID, user, new SqlQueryRequest().setSql(JOIN_SQL));
		});

		verify(mockEntityAuthorizationManager).canQueryTableOrView(user,
				List.of(new TableIdAndType("1", TableType.table), new TableIdAndType("2", TableType.materializedview),
						new TableIdAndType("3", TableType.table)));
	}

	@Test
	public void testRunQueryWithSourceProcessing() throws Exception {
		setupSources(new TableIndexDescription(IdAndVersion.parse("syn1")),
				new TableIndexDescription(IdAndVersion.parse("syn2")));
		setupAuthorized();
		when(mockTableManagerSupport.getTableStatusOrCreateIfNotExists(IdAndVersion.parse("syn1")))
				.thenReturn(new TableStatus().setState(TableState.PROCESSING));

		assertThrows(TableUnavailableException.class, () -> {
			// call under test
			manager.runQuery(mockCallback, JOB_ID, user, new SqlQueryRequest().setSql(JOIN_SQL));
		});

		verifyNoInteractions(mockCountingSemaphore, mockIndexConnectionFactory);
	}

	@Test
	public void testRunQueryWithSourceFailed() throws Exception {
		setupSources(new TableIndexDescription(IdAndVersion.parse("syn1")),
				new TableIndexDescription(IdAndVersion.parse("syn2")));
		setupAuthorized();
		when(mockTableManagerSupport.getTableStatusOrCreateIfNotExists(IdAndVersion.parse("syn1")))
				.thenReturn(new TableStatus().setState(TableState.PROCESSING_FAILED).setErrorMessage("bad schema"));

		String message = assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			manager.runQuery(mockCallback, JOB_ID, user, new SqlQueryRequest().setSql(JOIN_SQL));
		}).getMessage();

		assertEquals("syn1 could not be built: bad schema", message);
	}

	@Test
	public void testRunQueryWithQueryAlreadyRunning() throws Exception {
		setupSources(new TableIndexDescription(IdAndVersion.parse("syn1")),
				new TableIndexDescription(IdAndVersion.parse("syn2")));
		setupAuthorized();
		setupAvailable();
		when(mockCountingSemaphore.attemptToAcquireLock(anyString(), anyLong(), anyInt(), anyString()))
				.thenReturn(Optional.empty());

		assertThrows(RecoverableMessageException.class, () -> {
			// call under test
			manager.runQuery(mockCallback, JOB_ID, user, new SqlQueryRequest().setSql(JOIN_SQL));
		});

		verify(mockCountingSemaphore).attemptToAcquireLock("SQL_QUERY_USER_77", 120, 1, "Running SQL for job 123");
		verify(mockCountingSemaphore, never()).releaseLock(any(), any());
		verifyNoInteractions(mockIndexConnectionFactory);
	}

	@Test
	public void testRunQueryWithBuildTimeout() throws Exception {
		setupSources(new TableIndexDescription(IdAndVersion.parse("syn1")),
				new TableIndexDescription(IdAndVersion.parse("syn2")));
		setupAuthorized();
		setupAvailable();
		setupLock();
		setupBuild();
		doThrow(new QueryTimeoutException("Statement cancelled due to timeout")).when(mockTableIndexManager)
				.populateIndexFromDefiningSql(any(), any(), anyInt());

		String message = assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			manager.runQuery(mockCallback, JOB_ID, user, new SqlQueryRequest().setSql(JOIN_SQL));
		}).getMessage();

		assertEquals(SqlQueryManager.BUILD_TIMEOUT_MESSAGE, message);
		// The partial result is dropped and the user's lock released all the same.
		verify(mockTableIndexManager).deleteTableIndex(RESULT_ID);
		verify(mockCountingSemaphore).releaseLock("SQL_QUERY_USER_77", "token");
	}

	@Test
	public void testRunQueryWithLimitTooLarge() throws Exception {
		String message = assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			manager.runQuery(mockCallback, JOB_ID, user, new SqlQueryRequest().setSql(JOIN_SQL).setLimit(101L));
		}).getMessage();

		assertEquals("limit must be between 1 and 100", message);
		verifyNoInteractions(mockTableManagerSupport, mockCountingSemaphore);
	}

	@Test
	public void testRunQueryWithLimitTooSmall() throws Exception {
		assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			manager.runQuery(mockCallback, JOB_ID, user, new SqlQueryRequest().setSql(JOIN_SQL).setLimit(0L));
		});
		verifyNoInteractions(mockTableManagerSupport, mockCountingSemaphore);
	}

	@Test
	public void testRunQueryWithNoSql() throws Exception {
		assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			manager.runQuery(mockCallback, JOB_ID, user, new SqlQueryRequest().setSql(" "));
		});
		verifyNoInteractions(mockTableManagerSupport, mockCountingSemaphore);
	}

	@Test
	public void testDropAbandonedResults() {
		when(mockIndexConnectionFactory.connectToTableIndex(RESULT_ID)).thenReturn(mockTableIndexManager);
		when(mockTableIndexManager.getIndexTableNamesCreatedBefore(eq("T9"), any())).thenReturn(
				List.of("T9000000000000041", "T9000000000000041_IDX_STATUS", "T90210", "T9000000000000042"));

		// call under test
		manager.dropAbandonedResults(RESULT_ID);

		// Only transient results are dropped; T90210 is the index of syn90210.
		verify(mockTableIndexManager).deleteTableIndex(IdAndVersion.parse("syn9000000000000041"));
		verify(mockTableIndexManager).deleteTableIndex(IdAndVersion.parse("syn9000000000000042"));
		verify(mockTableIndexManager, never()).deleteTableIndex(IdAndVersion.parse("syn90210"));
		ArgumentCaptor<Timestamp> before = ArgumentCaptor.forClass(Timestamp.class);
		verify(mockTableIndexManager).getIndexTableNamesCreatedBefore(eq("T9"), before.capture());
		long ageMs = System.currentTimeMillis() - before.getValue().getTime();
		assertTrue(ageMs >= SqlQueryManager.ABANDONED_AFTER_MS && ageMs < SqlQueryManager.ABANDONED_AFTER_MS + 60_000);
	}

	private void setupSources(IndexDescription syn1, IndexDescription syn2) {
		when(mockTableManagerSupport.getIndexDescription(IdAndVersion.parse("syn1"))).thenReturn(syn1);
		when(mockTableManagerSupport.getIndexDescription(IdAndVersion.parse("syn2"))).thenReturn(syn2);
	}

	private void setupAuthorized() {
		when(mockEntityAuthorizationManager.canQueryTableOrView(any(), any()))
				.thenReturn(AuthorizationStatus.authorized());
	}

	private void setupAvailable() {
		when(mockTableManagerSupport.getTableStatusOrCreateIfNotExists(any()))
				.thenReturn(new TableStatus().setState(TableState.AVAILABLE));
	}

	@SuppressWarnings("unchecked")
	private void setupLock() throws Exception {
		when(mockCountingSemaphore.attemptToAcquireLock(anyString(), anyLong(), anyInt(), anyString()))
				.thenReturn(Optional.of("token"));
		doAnswer(invocation -> {
			ProgressingCallable<Object> runner = invocation.getArgument(2);
			return runner.call(mockCallback);
		}).when(mockTableManagerSupport).tryRunWithTableNonExclusiveLock(any(), any(), any(ProgressingCallable.class),
				any(IdAndVersion[].class));
	}

	private void setupBuild() {
		when(mockIndexConnectionFactory.connectToTableIndex(RESULT_ID)).thenReturn(mockTableIndexManager);
		when(mockTableManagerSupport.getTableSchema(IdAndVersion.parse("syn1"))).thenReturn(List.of(sampleId));
		when(mockTableManagerSupport.getTableSchema(IdAndVersion.parse("syn2"))).thenReturn(List.of(participantId, arm));
		Map<String, ColumnModel> sourceColumns = Map.of("11", sampleId, "21", participantId, "22", arm);
		when(mockTableManagerSupport.getColumnModel(any()))
				.thenAnswer(invocation -> sourceColumns.get(invocation.getArgument(0)));
		when(mockColumnModelManager.createColumnModel(any())).thenReturn(resultId, resultArm);
	}

	private void setupRows(int count) {
		when(mockConnectionFactory.getConnection(RESULT_ID)).thenReturn(mockTableIndexDao);
		List<Row> rows = new ArrayList<>();
		for (long i = 1; i <= count; i++) {
			rows.add(new Row().setRowId(i).setValues(List.of(Long.toString(i), "arm " + i)));
		}
		when(mockTableIndexDao.query(readCaptor.capture()))
				.thenReturn(new RowSet().setTableId(RESULT_ID.toString()).setRows(rows));
	}
}
