package org.sagebionetworks.repo.service.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sagebionetworks.repo.manager.table.MaterializedViewManager;
import org.sagebionetworks.repo.model.table.SqlWorksheet;

@ExtendWith(MockitoExtension.class)
public class SqlWorksheetMetadataProviderTest {

	private static final String JOIN_SQL = "SELECT s.id, p.arm FROM syn1 s JOIN syn2 p ON s.id = p.id";

	@InjectMocks
	private SqlWorksheetMetadataProvider provider;

	@Mock
	private MaterializedViewManager mockMaterializedViewManager;

	@Mock
	private EntityEvent mockEvent;

	@Test
	public void testValidateEntity() {
		SqlWorksheet worksheet = new SqlWorksheet().setName("cohort join").setDefiningSQL(JOIN_SQL);

		// call under test
		provider.validateEntity(worksheet, mockEvent);

		verify(mockMaterializedViewManager).validateDefiningSql(JOIN_SQL);
	}

	@Test
	public void testValidateEntityWithInvalidSql() {
		SqlWorksheet worksheet = new SqlWorksheet().setName("broken").setDefiningSQL("SELECT FROM");
		doThrow(new IllegalArgumentException("Encountered \" \"FROM\" \"FROM \"\""))
				.when(mockMaterializedViewManager).validateDefiningSql("SELECT FROM");

		String message = assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			provider.validateEntity(worksheet, mockEvent);
		}).getMessage();

		assertEquals("Encountered \" \"FROM\" \"FROM \"\"", message);
	}

	@Test
	public void testValidateEntityWithNoSql() {
		SqlWorksheet worksheet = new SqlWorksheet().setName("empty");

		String message = assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			provider.validateEntity(worksheet, mockEvent);
		}).getMessage();

		assertEquals("The definingSQL of the SQL worksheet is required and must not be the empty string.", message);
		verifyNoInteractions(mockMaterializedViewManager);
	}

	@Test
	public void testValidateDefiningSql() {
		// call under test
		provider.validateDefiningSql(JOIN_SQL);

		verify(mockMaterializedViewManager).validateDefiningSql(JOIN_SQL);
	}

	@Test
	public void testValidateDefiningSqlWithBlank() {
		assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			provider.validateDefiningSql("  ");
		});

		verifyNoInteractions(mockMaterializedViewManager);
	}

	@Test
	public void testIsNotADefiningSqlDependency() {
		// A worksheet records no source tables: it must never take part in lineage or rebuilds.
		assertFalse(provider instanceof TypeSpecificCreateProvider);
		assertFalse(provider instanceof TypeSpecificUpdateProvider);
	}
}
