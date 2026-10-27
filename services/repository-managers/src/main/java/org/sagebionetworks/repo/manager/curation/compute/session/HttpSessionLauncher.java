package org.sagebionetworks.repo.manager.curation.compute.session;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sagebionetworks.StackConfiguration;
import org.sagebionetworks.repo.model.curation.execution.ListSessionEnvironmentsResponse;
import org.sagebionetworks.repo.model.curation.execution.SessionEnvironment;
import org.sagebionetworks.schema.adapter.JSONObjectAdapterException;
import org.sagebionetworks.schema.adapter.org.json.EntityFactory;
import org.sagebionetworks.simpleHttpClient.SimpleHttpClient;
import org.sagebionetworks.simpleHttpClient.SimpleHttpRequest;
import org.sagebionetworks.simpleHttpClient.SimpleHttpResponse;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A {@link SessionLauncher} reached over HTTP at the stack's session launcher endpoint, which runs the
 * sessions next to it (the sandbox's enclave service). Every call presents the shared launcher secret.
 */
@Service
public class HttpSessionLauncher implements SessionLauncher {

	private final SimpleHttpClient httpClient;
	private final ObjectMapper objectMapper;
	private final StackConfiguration config;

	public HttpSessionLauncher(SimpleHttpClient httpClient, ObjectMapper objectMapper, StackConfiguration config) {
		this.httpClient = httpClient;
		this.objectMapper = objectMapper;
		this.config = config;
	}

	@Override
	public List<SessionEnvironment> listEnvironments() {
		if (!isConfigured()) {
			return List.of();
		}
		String json = call("GET", "/environments", null);
		try {
			List<SessionEnvironment> environments = EntityFactory
					.createEntityFromJSONString(json, ListSessionEnvironmentsResponse.class).getEnvironments();
			return environments == null ? List.of() : environments;
		} catch (JSONObjectAdapterException e) {
			throw new IllegalStateException("The session launcher returned environments that could not be read.", e);
		}
	}

	@Override
	public LaunchedSession start(SessionLaunchRequest request) {
		return read(call("POST", "/sessions", write(request)), LaunchedSession.class);
	}

	@Override
	public SessionAccessGrant grantAccess(String sessionId, Long userId) {
		return read(call("POST", sessionPath(sessionId) + "/access", write(Map.of("userId", userId))),
				SessionAccessGrant.class);
	}

	@Override
	public List<SessionOutput> stop(String sessionId) {
		StoppedSession stopped = read(call("POST", sessionPath(sessionId) + "/stop", "{}"), StoppedSession.class);
		return stopped.outputs() == null ? List.of() : stopped.outputs();
	}

	record StoppedSession(List<SessionOutput> outputs) {
	}

	boolean isConfigured() {
		String endpoint = config.getSessionLauncherEndpoint();
		return endpoint != null && !endpoint.isBlank();
	}

	static String sessionPath(String sessionId) {
		return "/sessions/" + URLEncoder.encode(sessionId, StandardCharsets.UTF_8);
	}

	String call(String method, String path, String body) {
		if (!isConfigured()) {
			throw new IllegalStateException("Interactive sessions are not available on this stack.");
		}
		SimpleHttpRequest request = new SimpleHttpRequest();
		request.setUri(config.getSessionLauncherEndpoint().replaceAll("/+$", "") + path);
		// SimpleHttpClient adds to the headers it is given, so they must be mutable.
		request.setHeaders(new HashMap<>(Map.of("Authorization", "Bearer " + config.getSessionLauncherSecret(),
				"Content-Type", "application/json", "Accept", "application/json")));
		SimpleHttpResponse response;
		try {
			response = "GET".equals(method) ? httpClient.get(request) : httpClient.post(request, body);
		} catch (IOException e) {
			throw new IllegalStateException("Could not reach the session launcher: " + e.getMessage(), e);
		}
		int status = response.getStatusCode();
		if (status >= 400 && status < 500) {
			// The launcher refuses requests it cannot honour (an unknown environment, a session that has
			// already stopped) with a 4xx and a message meant for the user.
			throw new IllegalArgumentException("The session launcher refused the request: " + response.getContent());
		}
		if (status < 200 || status >= 300) {
			throw new IllegalStateException(
					"The session launcher failed (" + status + "): " + response.getContent());
		}
		return response.getContent();
	}

	String write(Object value) {
		try {
			return objectMapper.writeValueAsString(value);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	<T> T read(String json, Class<T> type) {
		try {
			return objectMapper.readValue(json, type);
		} catch (IOException e) {
			throw new IllegalStateException("The session launcher returned a response that could not be read.", e);
		}
	}
}
