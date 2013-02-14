package org.sagebionetworks.repo.manager.file.preview;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sagebionetworks.repo.model.DatastoreException;
import org.sagebionetworks.repo.model.dao.FileHandleDao;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.sagebionetworks.repo.model.file.FileHandleResults;
import org.sagebionetworks.repo.model.file.HasPreviewId;
import org.sagebionetworks.repo.model.file.S3FileHandle;
import org.sagebionetworks.repo.web.NotFoundException;

/**
 * A simple stub implementation of the FileMetadataDao.
 * 
 * @author jmhill
 *
 */
public class StubFileMetadataDao implements FileHandleDao {

	Map<String, FileHandle> map = new HashMap<String, FileHandle>();

	@Override
	public <T extends FileHandle> T createFile(T metadata) {
		// Create the metadata
		String id = ""+map.size()+1;
		metadata.setId(id);
		metadata.setCreatedOn(new Date());
		map.put(id, metadata);
		return metadata;
	}

	@Override
	public void setPreviewId(String fileId, String previewId)
			throws DatastoreException, NotFoundException {
		// Get the file form the mad
		S3FileHandle metadata = (S3FileHandle) map.get(fileId);
		if(metadata == null) throw new NotFoundException();
		metadata.setPreviewId(previewId);
	}

	@Override
	public FileHandle get(String id) throws DatastoreException,
			NotFoundException {
		FileHandle metadata = map.get(id);
		if(metadata == null) throw new NotFoundException();
		return metadata;
	}

	@Override
	public void delete(String id) {
		map.remove(id);
	}

	@Override
	public boolean doesExist(String id) {
		return map.keySet().contains(id);
	}

	@Override
	public String getHandleCreator(String fileHandleId)
			throws NotFoundException {
		return map.get(fileHandleId).getCreatedBy();
	}

	@Override
	public String getPreviewFileHandleId(String handleId)
			throws NotFoundException {
 		return ((HasPreviewId)map.get(handleId)).getPreviewId();
	}

	@Override
	public FileHandleResults getAllFileHandles(List<String> ids,
			boolean includePreviews) {
		throw new UnsupportedOperationException("Not supported");
	}

}
