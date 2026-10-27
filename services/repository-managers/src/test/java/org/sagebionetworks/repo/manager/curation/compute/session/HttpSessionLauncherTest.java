package org.sagebionetworks.repo.manager.curation.compute.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sagebionetworks.StackConfiguration;
import org.sagebionetworks.repo.model.curation.execution.SessionEnvironment;
import org.sagebionetworks.repo.model.curation.execution.SessionPackage;
import org.sagebionetworks.simpleHttpClient.SimpleHttpClient;
import org.sagebionetworks.simpleHttpClient.SimpleHttpRequest;
import org.sagebionetworks.simpleHttpClient.SimpleHttpResponse;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

@ExtendWith(MockitoExtension.class)
public class HttpSessionLauncherTest {

	@Mock
	private SimpleHttpClient mockHttpClient;
	@Mock
	private StackConfiguration mockConfig;

	private HttpSessionLauncher launcher;

	private String endpoint = "http://enclave:8000/";

	@BeforeEach
	public void setup() {
		// The same mapper as ManagerConfiguration's jsonObjectMapper.
		ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
				.registerModule(new JavaTimeModule());
		launcher = new HttpSessionLauncher(mockHttpClient, objectMapper, mockConfig);
	}

	private SimpleHttpResponse response(int statusCode, String content) {
		SimpleHttpResponse response = mock(SimpleHttpResponse.class);
		when(response.getStatusCode()).thenReturn(statusCode);
		when(response.getContent()).thenReturn(content);
		return response;
	}

	private void setupConfigured() {
		when(mockConfig.getSessionLauncherEndpoint()).thenReturn(endpoint);
		when(mockConfig.getSessionLauncherSecret()).thenReturn("s3cret");
	}

	private SimpleHttpRequest expectedRequest(String path) {
		SimpleHttpRequest request = new SimpleHttpRequest();
		request.setUri("http://enclave:8000" + path);
		request.setHeaders(Map.of("Authorization", "Bearer s3cret", "Content-Type", "application/json", "Accept",
				"application/json"));
		return request;
	}

	@Test
	public void testListEnvironmentsWithNoLauncher() {
		when(mockConfig.getSessionLauncherEndpoint()).thenReturn("");

		// call under test
		assertEquals(List.of(), launcher.listEnvironments());

		verifyNoInteractions(mockHttpClient);
	}

	@Test
	public void testListEnvironments() throws IOException {
		setupConfigured();
		SimpleHttpResponse ok = response(200, "{\"environments\":[{\"id\":\"python-3.12\",\"name\":\"Python 3.12\",\"imageDigest\":\"sha256:abc\","
						+ "\"packages\":[{\"name\":\"pandas\",\"version\":\"2.2.3\"}]}]}");
		when(mockHttpClient.get(expectedRequest("/environments"))).thenReturn(ok);

		// call under test
		List<SessionEnvironment> environments = launcher.listEnvironments();

		assertEquals(List.of(new SessionEnvironment().setId("python-3.12").setName("Python 3.12").setImageDigest("sha256:abc")
				.setPackages(List.of(new SessionPackage().setName("pandas").setVersion("2.2.3")))), environments);
	}

	@Test
	public void testStart() throws IOException {
		setupConfigured();
		SimpleHttpResponse created = response(201, "{\"sessionId\":\"session-1\",\"endsOn\":1800000000000}");
		when(mockHttpClient.post(any(), any())).thenReturn(created);
		SessionLaunchRequest request = new SessionLaunchRequest(987L, "985", "python-3.12", 101L, 120L,
				List.of(new SessionInput("files/cohort.csv", "https://s3/fh1")));

		// call under test
		LaunchedSession launched = launcher.start(request);

		assertEquals(new LaunchedSession("session-1", new Date(1_800_000_000_000L)), launched);
		ArgumentCaptor<SimpleHttpRequest> sent = ArgumentCaptor.forClass(SimpleHttpRequest.class);
		ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
		verify(mockHttpClient).post(sent.capture(), body.capture());
		assertEquals(expectedRequest("/sessions"), sent.getValue());
		// SimpleHttpClientImpl adds a Content-Type header to the map it is given.
		sent.getValue().getHeaders().put("Content-Type", "application/json; charset=utf-8");
		assertEquals("{\"taskId\":987,\"workspaceId\":\"985\",\"environmentId\":\"python-3.12\",\"userId\":101,"
				+ "\"maxDurationMinutes\":120,\"inputs\":[{\"path\":\"files/cohort.csv\",\"url\":\"https://s3/fh1\"}]}",
				body.getValue());
	}

	@Test
	public void testStop() throws IOException {
		setupConfigured();
		SimpleHttpResponse ok = response(200,
				"{\"outputs\":[{\"path\":\"results/t1.csv\",\"contentSize\":2048,\"contentMd5\":\"abc\"}]}");
		when(mockHttpClient.post(expectedRequest("/sessions/session-1/stop"), "{}")).thenReturn(ok);

		// call under test
		assertEquals(List.of(new SessionOutput("results/t1.csv", 2048L, "abc")), launcher.stop("session-1"));
	}

	@Test
	public void testGrantAccessWithRefusal() throws IOException {
		setupConfigured();
		SimpleHttpResponse conflict = response(409, "the session has stopped");
		when(mockHttpClient.post(expectedRequest("/sessions/session-1/access"), "{\"userId\":101}")).thenReturn(conflict);

		// call under test
		String message = assertThrows(IllegalArgumentException.class, () -> launcher.grantAccess("session-1", 101L))
				.getMessage();

		assertEquals("The session launcher refused the request: the session has stopped", message);
	}

	@Test
	public void testStartWithLauncherFailure() throws IOException {
		setupConfigured();
		SimpleHttpResponse failure = response(500, "boom");
		when(mockHttpClient.post(any(), any())).thenReturn(failure);

		// call under test
		assertThrows(IllegalStateException.class, () -> launcher.start(new SessionLaunchRequest(987L, "987",
				"python-3.12", 101L, 120L, List.of())));
	}

	@Test
	public void testStartWithNoLauncher() {
		when(mockConfig.getSessionLauncherEndpoint()).thenReturn("");

		// call under test
		String message = assertThrows(IllegalStateException.class, () -> launcher.start(new SessionLaunchRequest(987L,
				"987", "python-3.12", 101L, 120L, List.of()))).getMessage();

		assertEquals("Interactive sessions are not available on this stack.", message);
		verifyNoInteractions(mockHttpClient);
	}
}
