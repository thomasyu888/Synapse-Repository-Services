package org.sagebionetworks.table.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sagebionetworks.repo.manager.table.SqlQueryManager;
import org.sagebionetworks.repo.model.UnauthorizedException;
import org.sagebionetworks.repo.model.UserInfo;
import org.sagebionetworks.repo.model.dao.asynch.AsyncJobProgressCallback;
import org.sagebionetworks.repo.model.dbo.dao.table.TableExceptionTranslator;
import org.sagebionetworks.repo.model.table.SqlQueryRequest;
import org.sagebionetworks.repo.model.table.SqlQueryResponse;
import org.sagebionetworks.repo.model.table.TableState;
import org.sagebionetworks.repo.model.table.TableStatus;
import org.sagebionetworks.repo.model.table.TableUnavailableException;
import org.sagebionetworks.workers.util.aws.message.RecoverableMessageException;

@ExtendWith(MockitoExtension.class)
public class SqlQueryWorkerTest {

	@Mock
	private SqlQueryManager mockSqlQueryManager;
	@Mock
	private TableExceptionTranslator mockTableExceptionTranslator;
	@Mock
	private AsyncJobProgressCallback mockCallback;
	@Mock
	private UserInfo mockUser;

	@InjectMocks
	private SqlQueryWorker worker;

	private final SqlQueryRequest request = new SqlQueryRequest()
			.setSql("SELECT * FROM syn1 JOIN syn2 ON syn1.id = syn2.id");

	@Test
	public void testRun() throws Exception {
		SqlQueryResponse response = new SqlQueryResponse().setIsTruncated(false);
		when(mockSqlQueryManager.runQuery(mockCallback, "9", mockUser, request)).thenReturn(response);

		// call under test
		assertSame(response, worker.run("9", mockUser, request, mockCallback));
	}

	@Test
	public void testRunWithSourceUnavailable() throws Exception {
		when(mockSqlQueryManager.runQuery(mockCallback, "9", mockUser, request))
				.thenThrow(new TableUnavailableException(new TableStatus().setState(TableState.PROCESSING)));

		assertThrows(RecoverableMessageException.class, () -> {
			// call under test
			worker.run("9", mockUser, request, mockCallback);
		});

		verify(mockCallback).updateProgress("Waiting for the tables to become available...", 0L, 100L);
	}

	@Test
	public void testRunWithQueryAlreadyRunning() throws Exception {
		when(mockSqlQueryManager.runQuery(mockCallback, "9", mockUser, request))
				.thenThrow(new RecoverableMessageException("Waiting for the user's previous SQL query to finish"));

		assertThrows(RecoverableMessageException.class, () -> {
			// call under test
			worker.run("9", mockUser, request, mockCallback);
		});

		verify(mockCallback).updateProgress("Waiting for the user's previous SQL query to finish", 0L, 100L);
	}

	@Test
	public void testRunWithUnauthorized() throws Exception {
		UnauthorizedException denied = new UnauthorizedException("You lack READ access to the requested entity.");
		when(mockSqlQueryManager.runQuery(mockCallback, "9", mockUser, request)).thenThrow(denied);
		when(mockTableExceptionTranslator.translateException(denied)).thenReturn(denied);

		UnauthorizedException thrown = assertThrows(UnauthorizedException.class, () -> {
			// call under test
			worker.run("9", mockUser, request, mockCallback);
		});

		assertEquals("You lack READ access to the requested entity.", thrown.getMessage());
	}
}
