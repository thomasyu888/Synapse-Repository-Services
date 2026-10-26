package org.sagebionetworks.table.worker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.repo.manager.table.SqlQueryManager;
import org.sagebionetworks.repo.model.UnauthorizedException;
import org.sagebionetworks.repo.model.UserInfo;
import org.sagebionetworks.repo.model.dao.asynch.AsyncJobProgressCallback;
import org.sagebionetworks.repo.model.dbo.dao.table.TableExceptionTranslator;
import org.sagebionetworks.repo.model.table.SqlQueryRequest;
import org.sagebionetworks.repo.model.table.SqlQueryResponse;
import org.sagebionetworks.repo.model.table.TableUnavailableException;
import org.sagebionetworks.repo.web.NotFoundException;
import org.sagebionetworks.worker.AsyncJobRunner;
import org.sagebionetworks.workers.util.aws.message.RecoverableMessageException;
import org.sagebionetworks.workers.util.semaphore.LockUnavilableException;
import org.springframework.stereotype.Service;

/**
 * Runs SQL that may combine several tables and views (see {@link SqlQueryManager}).
 */
@Service
public class SqlQueryWorker implements AsyncJobRunner<SqlQueryRequest, SqlQueryResponse> {

	private static final Logger LOG = LogManager.getLogger(SqlQueryWorker.class);

	private final SqlQueryManager sqlQueryManager;
	private final TableExceptionTranslator tableExceptionTranslator;

	public SqlQueryWorker(SqlQueryManager sqlQueryManager, TableExceptionTranslator tableExceptionTranslator) {
		this.sqlQueryManager = sqlQueryManager;
		this.tableExceptionTranslator = tableExceptionTranslator;
	}

	@Override
	public Class<SqlQueryRequest> getRequestType() {
		return SqlQueryRequest.class;
	}

	@Override
	public Class<SqlQueryResponse> getResponseType() {
		return SqlQueryResponse.class;
	}

	@Override
	public SqlQueryResponse run(String jobId, UserInfo user, SqlQueryRequest request,
			AsyncJobProgressCallback jobProgressCallback) throws RecoverableMessageException, Exception {
		try {
			return sqlQueryManager.runQuery(jobProgressCallback, jobId, user, request);
		} catch (TableUnavailableException | LockUnavilableException e) {
			// A source is still being built, or is being updated: try again later.
			jobProgressCallback.updateProgress("Waiting for the tables to become available...", 0L, 100L);
			throw new RecoverableMessageException();
		} catch (RecoverableMessageException e) {
			jobProgressCallback.updateProgress(e.getMessage(), 0L, 100L);
			throw e;
		} catch (UnauthorizedException | NotFoundException | IllegalArgumentException e) {
			// The caller's SQL or access: report it as it is, without logging an error.
			throw tableExceptionTranslator.translateException(e);
		} catch (Throwable e) {
			LOG.error("SQL query job " + jobId + " failed", e);
			throw tableExceptionTranslator.translateException(e);
		}
	}
}
