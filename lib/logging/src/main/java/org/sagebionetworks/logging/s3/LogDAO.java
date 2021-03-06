package org.sagebionetworks.logging.s3;

import java.io.File;
import java.io.IOException;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;

/**
 * An abstraction for the log Data Access Object.
 * 
 * @author jmhill
 *
 */
public interface LogDAO {

	/**
	 * Save a log file to S3.
	 * 
	 * @param toSave
	 * @param timestamp
	 * @return
	 */
	public String saveLogFile(File toSave, long timestamp);
	
	/**
	 * Delete a log file using its key
	 * @param key
	 */
	public void deleteLogFile(String key);
	
	/**
	 * Get a reader that can be used to read one log entry at a time.
	 * @param key
	 * @return
	 * @throws IOException 
	 */
	public LogReader getLogFileReader(String key) throws IOException;
	
	/**
	 * Download a log to the passed destiantion file.
	 * @param key
	 * @return
	 * @throws IOException
	 */
	public ObjectMetadata downloadLogFile(String key, File destination) throws IOException;

	/**
	 * Delete all logs for this Stack Instances.
	 */
	public void deleteAllStackInstanceLogs();
	
	/**
	 * List all log files for this stack
	 * @param marker
	 * @return 
	 */
	public ObjectListing listAllStackInstanceLogs(String marker);
	
	/**
	 * Scans all log files in S3 to find a log contains the passed UUID.
	 * 
	 * @param uuidTofind
	 * @return The key of the first log file that contains the passed UUID. If no log is found then null will be returned.
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public String findLogContainingUUID(String uuidTofind) throws InterruptedException, IOException;
}
