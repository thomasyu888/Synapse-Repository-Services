package org.sagebionetworks.repo.manager.curation.compute.session;

/**
 * A file to copy into a session's /inputs.
 *
 * @param path The path under /inputs.
 * @param url  A pre-signed URL the launcher downloads the file from, before the session starts.
 */
public record SessionInput(String path, String url) {
}
