package org.sagebionetworks.repo.manager.curation.compute.session;

import java.util.List;

/**
 * What a {@link SessionLauncher} needs to start a session.
 *
 * @param taskId             The interactive session task being run.
 * @param workspaceId        The /workspace to mount: the ID of the task that owns it.
 * @param environmentId      The approved environment to run.
 * @param userId             The user the session is for; only they can open it.
 * @param maxDurationMinutes How long the session may run.
 * @param inputs             The files to copy into /inputs before the session starts.
 */
public record SessionLaunchRequest(Long taskId, String workspaceId, String environmentId, Long userId,
		Long maxDurationMinutes, List<SessionInput> inputs) {
}
