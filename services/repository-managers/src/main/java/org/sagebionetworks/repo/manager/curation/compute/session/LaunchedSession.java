package org.sagebionetworks.repo.manager.curation.compute.session;

import java.util.Date;

/**
 * A session a {@link SessionLauncher} started.
 *
 * @param sessionId The launcher's ID for the session.
 * @param endsOn    When the session is ended automatically.
 */
public record LaunchedSession(String sessionId, Date endsOn) {
}
