package org.sagebionetworks.repo.manager.curation.compute.session;

/**
 * A file a stopped session left in /outputs.
 *
 * @param path        The path under /outputs.
 * @param contentSize The size in bytes.
 * @param contentMd5  The MD5 of the content.
 */
public record SessionOutput(String path, Long contentSize, String contentMd5) {
}
