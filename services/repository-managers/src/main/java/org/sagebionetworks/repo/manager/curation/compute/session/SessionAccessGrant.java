package org.sagebionetworks.repo.manager.curation.compute.session;

import java.util.Date;

/**
 * A single-use link that opens a running session.
 *
 * @param url       The link.
 * @param expiresOn When the link stops working if it has not been used.
 */
public record SessionAccessGrant(String url, Date expiresOn) {
}
