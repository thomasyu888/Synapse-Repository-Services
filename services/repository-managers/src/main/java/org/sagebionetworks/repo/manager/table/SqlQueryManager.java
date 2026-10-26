package org.sagebionetworks.repo.manager.table;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.database.semaphore.CountingSemaphore;
import org.sagebionetworks.repo.manager.entity.EntityAuthorizationManager;
import org.sagebionetworks.repo.manager.entity.EntityAuthorizationManager.TableIdAndType;
import org.sagebionetworks.repo.model.ACCESS_TYPE;
import org.sagebionetworks.repo.model.UnauthorizedException;
import org.sagebionetworks.repo.model.UserInfo;
import org.sagebionetworks.repo.model.auth.AuthorizationStatus;
import org.sagebionetworks.repo.model.dao.table.TableType;
import org.sagebionetworks.repo.model.entity.IdAndVersion;
import org.sagebionetworks.repo.model.semaphore.LockContext;
import org.sagebionetworks.repo.model.semaphore.LockContext.ContextType;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.RowSet;
import org.sagebionetworks.repo.model.table.SqlQueryRequest;
import org.sagebionetworks.repo.model.table.SqlQueryResponse;
import org.sagebionetworks.repo.model.table.TableStatus;
import org.sagebionetworks.repo.model.table.TableUnavailableException;
import org.sagebionetworks.table.cluster.ConnectionFactory;
import org.sagebionetworks.table.cluster.QueryTranslator;
import org.sagebionetworks.table.cluster.SQLUtils;
import org.sagebionetworks.table.cluster.SchemaProvider;
import org.sagebionetworks.table.cluster.TableIndexDAO;
import org.sagebionetworks.table.cluster.description.BenefactorDescription;
import org.sagebionetworks.table.cluster.description.IndexDescription;
import org.sagebionetworks.table.cluster.description.MaterializedViewIndexDescription;
import org.sagebionetworks.table.query.ParseException;
import org.sagebionetworks.table.query.TableQueryParser;
import org.sagebionetworks.table.query.model.Pagination;
import org.sagebionetworks.table.query.model.QuerySpecification;
import org.sagebionetworks.table.query.model.SqlContext;
import org.sagebionetworks.table.query.model.TableExpression;
import org.sagebionetworks.table.query.util.SqlElementUtils;
import org.sagebionetworks.util.ValidateArgument;
import org.sagebionetworks.util.progress.ProgressCallback;
import org.sagebionetworks.workers.util.aws.message.RecoverableMessageException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.stereotype.Service;

/**
 * Runs SQL in the form of a materialized view's definingSQL, which may combine several tables and views, without
 * saving anything: the result is built into an index table of its own for one request, read under the caller's
 * permissions and dropped. Nothing is recorded as a dependency, so a run never enters table lineage and never causes a
 * rebuild.
 */
@Service
public class SqlQueryManager {

	private static final Logger LOG = LogManager.getLogger(SqlQueryManager.class);

	public static final long MAX_LIMIT = 100;

	/**
	 * The most seconds the build of a result may run. Building reads every source under a read lock, which holds back
	 * updates to those sources, so it must not run unbounded.
	 */
	public static final int BUILD_TIMEOUT_SECONDS = 60;

	/**
	 * The ids of the transient results start here: positive, so they can be named in SQL like any table, and far above
	 * any entity id, so they never name an entity's index.
	 */
	public static final long TRANSIENT_ID_OFFSET = 9_000_000_000_000_000L;

	/**
	 * A transient result older than this is left from a run that never finished, and is dropped.
	 */
	public static final long ABANDONED_AFTER_MS = 60 * 60 * 1000L;

	/**
	 * Each user runs one query at a time, cluster wide. The lock outlives the build so a released or expired lock
	 * always means the build is over.
	 */
	static final int USER_LOCK_TIMEOUT_SECONDS = BUILD_TIMEOUT_SECONDS * 2;

	static final String USER_LOCK_KEY_PREFIX = "SQL_QUERY_USER_";

	public static final String AGGREGATE_ONLY_MESSAGE = "%s can only be queried in aggregate, and a SQL query that combines tables returns rows. Query it directly, or save the SQL as a materialized view and query that.";

	public static final String BUILD_TIMEOUT_MESSAGE = "The SQL took longer than " + BUILD_TIMEOUT_SECONDS
			+ " seconds. Narrow it with a WHERE clause, or save it as a materialized view, which has no time limit.";

	private static final String TRANSIENT_TABLE_PREFIX = SQLUtils.TABLE_PREFIX + "9";

	private static final Pattern TRANSIENT_TABLE_NAME = Pattern
			.compile("^" + SQLUtils.TABLE_PREFIX + "(9\\d{15})$");

	private final TableManagerSupport tableManagerSupport;
	private final EntityAuthorizationManager entityAuthorizationManager;
	private final ColumnModelManager columnModelManager;
	private final TableIndexConnectionFactory indexConnectionFactory;
	private final ConnectionFactory connectionFactory;
	private final CountingSemaphore countingSemaphore;

	public SqlQueryManager(TableManagerSupport tableManagerSupport,
			EntityAuthorizationManager entityAuthorizationManager, ColumnModelManager columnModelManager,
			TableIndexConnectionFactory indexConnectionFactory, ConnectionFactory connectionFactory,
			CountingSemaphore countingSemaphore) {
		this.tableManagerSupport = tableManagerSupport;
		this.entityAuthorizationManager = entityAuthorizationManager;
		this.columnModelManager = columnModelManager;
		this.indexConnectionFactory = indexConnectionFactory;
		this.connectionFactory = connectionFactory;
		this.countingSemaphore = countingSemaphore;
	}

	/**
	 * Run the given SQL and return its first rows.
	 *
	 * @param callback
	 * @param jobId    The id of the job running the query; it names the query's transient result
	 * @param user
	 * @param request
	 * @return Up to the requested number of rows, with isTruncated set when there are more
	 * @throws UnauthorizedException       When the user cannot read every source with row-level access
	 * @throws IllegalArgumentException    When the SQL is not valid definingSQL, or the limit is out of range
	 * @throws RecoverableMessageException When a source is still being built, or the user already has a query running
	 */
	public SqlQueryResponse runQuery(ProgressCallback callback, String jobId, UserInfo user, SqlQueryRequest request)
			throws Exception {
		ValidateArgument.required(callback, "callback");
		ValidateArgument.required(jobId, "jobId");
		ValidateArgument.required(user, "user");
		ValidateArgument.required(request, "request");
		ValidateArgument.requiredNotBlank(request.getSql(), "sql");
		long limit = request.getLimit() == null ? MAX_LIMIT : request.getLimit();
		ValidateArgument.requirement(limit >= 1 && limit <= MAX_LIMIT, "limit must be between 1 and " + MAX_LIMIT);

		IdAndVersion resultId = IdAndVersion.newBuilder().setId(TRANSIENT_ID_OFFSET + Long.parseLong(jobId)).build();
		MaterializedViewIndexDescription description = new MaterializedViewIndexDescription(resultId,
				request.getSql(), tableManagerSupport);
		List<IdAndVersion> sourceIds = description.getDependencies().stream().map(IndexDescription::getIdAndVersion)
				.distinct().collect(Collectors.toList());
		validateRowLevelReadAccess(user, description);
		validateSourcesAvailable(sourceIds);

		String userLockKey = USER_LOCK_KEY_PREFIX + user.getId();
		String lockToken = countingSemaphore
				.attemptToAcquireLock(userLockKey, USER_LOCK_TIMEOUT_SECONDS, 1, "Running SQL for job " + jobId)
				.orElseThrow(() -> new RecoverableMessageException("Waiting for the user's previous SQL query to finish"));
		try {
			dropAbandonedResults(resultId);
			return tableManagerSupport.tryRunWithTableNonExclusiveLock(callback,
					new LockContext(ContextType.SqlQuery, sourceIds.get(0)),
					(ProgressCallback innerCallback) -> buildAndRead(user, description, limit),
					sourceIds.toArray(new IdAndVersion[sourceIds.size()]));
		} finally {
			countingSemaphore.releaseLock(userLockKey, lockToken);
		}
	}

	/**
	 * The sources are checked as one table query would check them, including the sources of any view among them.
	 * Access that is only aggregate is refused: this query returns rows, and the aggregate-only rules (quasi
	 * identifiers, thresholds) are enforced by the table query only.
	 */
	void validateRowLevelReadAccess(UserInfo user, IndexDescription description) {
		List<TableIdAndType> sources = new ArrayList<>();
		for (IndexDescription dependency : description.getDependencies()) {
			collectTableNodes(dependency, sources);
		}
		AuthorizationStatus status = entityAuthorizationManager.canQueryTableOrView(user, sources);
		Optional<String> aggregateSource = status.getAggregateDataSourceId();
		if (!status.isAuthorized() && aggregateSource.isPresent()) {
			throw new UnauthorizedException(String.format(AGGREGATE_ONLY_MESSAGE, aggregateSource.get()));
		}
		status.checkAuthorizationOrElseThrow();
	}

	private static void collectTableNodes(IndexDescription description, List<TableIdAndType> nodes) {
		nodes.add(new TableIdAndType(description.getIdAndVersion().getId().toString(), description.getTableType()));
		for (IndexDescription dependency : description.getDependencies()) {
			collectTableNodes(dependency, nodes);
		}
	}

	/**
	 * Asking for a source's status starts building it when needed, as a table query does.
	 */
	void validateSourcesAvailable(List<IdAndVersion> sourceIds) throws TableUnavailableException {
		for (IdAndVersion sourceId : sourceIds) {
			TableStatus status = tableManagerSupport.getTableStatusOrCreateIfNotExists(sourceId);
			switch (status.getState()) {
			case AVAILABLE:
				break;
			case PROCESSING:
				throw new TableUnavailableException(status);
			case PROCESSING_FAILED:
				throw new IllegalArgumentException(String.format("%s could not be built: %s", sourceId,
						status.getErrorMessage()));
			default:
				throw new IllegalStateException("Unknown state: " + status.getState());
			}
		}
	}

	SqlQueryResponse buildAndRead(UserInfo user, MaterializedViewIndexDescription description, long limit)
			throws ParseException {
		IdAndVersion resultId = description.getIdAndVersion();
		TableIndexManager indexManager = indexConnectionFactory.connectToTableIndex(resultId);
		try {
			// One more row than asked for shows whether there are more. Rows from a view are filtered by the user's
			// access after the build, so the build itself can only stop early when no source is filtered.
			String buildSql = description.getBenefactors().isEmpty()
					? limitRows(description.getDefiningSql(), limit + 1)
					: description.getDefiningSql();
			MaterializedViewIndexDescription buildDescription = new MaterializedViewIndexDescription(resultId,
					buildSql, tableManagerSupport);
			QueryTranslator build = QueryTranslator.builder().sql(buildSql).schemaProvider(tableManagerSupport)
					.sqlContext(SqlContext.build).indexDescription(buildDescription).build();
			List<ColumnModel> schema = build.getSchemaOfSelect().stream().map(columnModelManager::createColumnModel)
					.collect(Collectors.toList());
			indexManager.resetTableIndex(buildDescription, schema, false);
			try {
				indexManager.populateIndexFromDefiningSql(schema, build, BUILD_TIMEOUT_SECONDS);
			} catch (QueryTimeoutException e) {
				throw new IllegalArgumentException(BUILD_TIMEOUT_MESSAGE, e);
			}
			return readRows(user, buildDescription, schema, limit);
		} finally {
			indexManager.deleteTableIndex(resultId);
		}
	}

	SqlQueryResponse readRows(UserInfo user, MaterializedViewIndexDescription description, List<ColumnModel> schema,
			long limit) throws ParseException {
		IdAndVersion resultId = description.getIdAndVersion();
		TableIndexDAO indexDao = connectionFactory.getConnection(resultId);
		QuerySpecification query = new TableQueryParser(
				String.format("SELECT * FROM %s ORDER BY ROW_ID LIMIT %d", resultId, limit + 1)).querySpecificationEOF();
		for (BenefactorDescription benefactor : description.getBenefactors()) {
			Set<Long> benefactorIds = indexDao.getDistinctLongValues(resultId, benefactor.getBenefactorColumnName());
			Set<Long> accessible = new HashSet<>(tableManagerSupport.getAccessibleBenefactors(user,
					benefactor.getBenefactorType(), benefactorIds, ACCESS_TYPE.READ));
			// -1 is the benefactor of a row with none, which is always readable.
			accessible.add(-1L);
			TableQueryManagerImpl.buildBenefactorFilter(query, accessible, benefactor.getBenefactorColumnName());
		}
		QueryTranslator read = QueryTranslator.builder(query, user.getId())
				.schemaProvider(new ResultSchemaProvider(resultId, schema, tableManagerSupport))
				.sqlContext(SqlContext.query).indexDescription(description).build();
		RowSet rowSet = indexDao.query(read).setTableId(null);
		boolean isTruncated = rowSet.getRows().size() > limit;
		if (isTruncated) {
			rowSet.setRows(new ArrayList<>(rowSet.getRows().subList(0, (int) limit)));
		}
		return new SqlQueryResponse().setRowSet(rowSet).setIsTruncated(isTruncated);
	}

	/**
	 * Cap the rows of the SQL at the given number, keeping any smaller LIMIT it already has. Only SQL that is a single
	 * query specification is capped; a UNION is left as it is.
	 */
	static String limitRows(String sql, long maxRows) {
		QuerySpecification query;
		try {
			query = new TableQueryParser(sql).querySpecificationEOF();
		} catch (ParseException | RuntimeException e) {
			return sql;
		}
		TableExpression tableExpression = query.getTableExpression();
		Pagination pagination = tableExpression.getPagination();
		tableExpression.replacePagination(SqlElementUtils.limitMaxRowsPerPage(pagination, maxRows));
		return query.toSql();
	}

	/**
	 * A run that died before dropping its result leaves the table behind; the next run on the same index drops it.
	 */
	void dropAbandonedResults(IdAndVersion resultId) {
		TableIndexManager indexManager = indexConnectionFactory.connectToTableIndex(resultId);
		Timestamp abandonedBefore = new Timestamp(System.currentTimeMillis() - ABANDONED_AFTER_MS);
		for (String tableName : indexManager.getIndexTableNamesCreatedBefore(TRANSIENT_TABLE_PREFIX,
				abandonedBefore)) {
			Matcher matcher = TRANSIENT_TABLE_NAME.matcher(tableName);
			if (matcher.matches()) {
				IdAndVersion abandoned = IdAndVersion.newBuilder().setId(Long.parseLong(matcher.group(1))).build();
				LOG.info("Dropping abandoned SQL query result {}", tableName);
				indexManager.deleteTableIndex(abandoned);
			}
		}
	}

	/**
	 * The transient result is no entity, so its schema comes from its build; every other table is looked up as usual.
	 */
	static class ResultSchemaProvider implements SchemaProvider {

		private final IdAndVersion resultId;
		private final List<ColumnModel> schema;
		private final SchemaProvider delegate;

		ResultSchemaProvider(IdAndVersion resultId, List<ColumnModel> schema, SchemaProvider delegate) {
			this.resultId = resultId;
			this.schema = schema;
			this.delegate = delegate;
		}

		@Override
		public TableType getTableType(IdAndVersion tableId) {
			return resultId.equals(tableId) ? TableType.materializedview : delegate.getTableType(tableId);
		}

		@Override
		public List<ColumnModel> getTableSchema(IdAndVersion tableId) {
			return resultId.equals(tableId) ? schema : delegate.getTableSchema(tableId);
		}

		@Override
		public ColumnModel getColumnModel(String id) {
			return schema.stream().filter(column -> column.getId().equals(id)).findFirst()
					.orElseGet(() -> delegate.getColumnModel(id));
		}
	}
}
