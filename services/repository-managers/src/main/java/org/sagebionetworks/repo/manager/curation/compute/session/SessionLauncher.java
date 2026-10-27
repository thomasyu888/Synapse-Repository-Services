package org.sagebionetworks.repo.manager.curation.compute.session;

import java.util.List;

import org.sagebionetworks.repo.model.curation.execution.SessionEnvironment;

/**
 * Starts and stops the network-isolated containers behind interactive session tasks. A launcher copies
 * the inputs into the session read-only, runs the session where it can reach nothing but the launcher's
 * own gateway, and gives back what the session wrote to /outputs when it stops. The sandbox runs one on
 * Docker; on AWS it would run each session as an ECS task in a subnet with no route out.
 */
public interface SessionLauncher {

	/**
	 * @return The approved environments a session can run in. Empty when this stack has no launcher.
	 */
	List<SessionEnvironment> listEnvironments();

	/**
	 * Copy the inputs into a new session and start it.
	 *
	 * @return The session's ID and when it will be ended automatically.
	 */
	LaunchedSession start(SessionLaunchRequest request);

	/**
	 * Issue a single-use link that opens the given running session for the given user.
	 */
	SessionAccessGrant grantAccess(String sessionId, Long userId);

	/**
	 * Stop the given session and collect what it wrote to /outputs.
	 *
	 * @return The files the session left in /outputs.
	 */
	List<SessionOutput> stop(String sessionId);
}
