package org.sagebionetworks.repo.service.metadata;

import org.sagebionetworks.repo.manager.table.MaterializedViewManager;
import org.sagebionetworks.repo.model.DatastoreException;
import org.sagebionetworks.repo.model.InvalidModelException;
import org.sagebionetworks.repo.model.UnauthorizedException;
import org.sagebionetworks.repo.model.table.SqlWorksheet;
import org.sagebionetworks.repo.web.NotFoundException;
import org.sagebionetworks.util.ValidateArgument;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * A worksheet's SQL is held to the rules of a materialized view's definingSQL, so that it runs as a multi-table query
 * and can be saved as a materialized view unchanged. Unlike a view, a worksheet registers no source tables: it is not
 * part of table lineage and nothing rebuilds when its tables change.
 */
@Service
public class SqlWorksheetMetadataProvider
		implements EntityValidator<SqlWorksheet>, TypeSpecificDefiningSqlProvider<SqlWorksheet> {

	private final MaterializedViewManager materializedViewManager;

	@Autowired
	public SqlWorksheetMetadataProvider(MaterializedViewManager materializedViewManager) {
		this.materializedViewManager = materializedViewManager;
	}

	@Override
	public void validateEntity(SqlWorksheet entity, EntityEvent event)
			throws InvalidModelException, NotFoundException, DatastoreException, UnauthorizedException {
		ValidateArgument.required(entity, "entity");
		validateDefiningSql(entity.getDefiningSQL());
	}

	@Override
	public void validateDefiningSql(String definingSql) {
		ValidateArgument.requiredNotBlank(definingSql, "The definingSQL of the SQL worksheet");
		materializedViewManager.validateDefiningSql(definingSql);
	}

}
