package org.sagebionetworks.repo.manager.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sagebionetworks.repo.manager.entity.EntityAuthorizationManager;
import org.sagebionetworks.repo.manager.entity.decider.UsersEntityAccessInfo;
import org.sagebionetworks.repo.model.ACCESS_TYPE;
import org.sagebionetworks.repo.model.AuthorizationConstants;
import org.sagebionetworks.repo.model.EntityHeader;
import org.sagebionetworks.repo.model.NodeConstants.BOOTSTRAP_NODES;
import org.sagebionetworks.repo.model.NodeDAO;
import org.sagebionetworks.repo.model.UnauthorizedException;
import org.sagebionetworks.repo.model.UserInfo;
import org.sagebionetworks.repo.model.auth.AuthorizationStatus;
import org.sagebionetworks.repo.model.dao.table.TableStatusDAO;
import org.sagebionetworks.repo.model.dbo.dao.table.DefiningSqlDependencyDao;
import org.sagebionetworks.repo.model.dbo.dao.table.DefiningSqlDependencyDao.Dependency;
import org.sagebionetworks.repo.model.entity.IdAndVersion;
import org.sagebionetworks.repo.model.jdo.KeyFactory;
import org.sagebionetworks.repo.model.table.LineageAccess;
import org.sagebionetworks.repo.model.table.LineageDirection;
import org.sagebionetworks.repo.model.table.TableLineage;
import org.sagebionetworks.repo.model.table.TableLineageEdge;
import org.sagebionetworks.repo.model.table.TableLineageNode;
import org.sagebionetworks.repo.model.table.TableState;
import org.sagebionetworks.repo.model.table.TableStatus;

@ExtendWith(MockitoExtension.class)
public class TableLineageManagerTest {

	private static final String TABLE = "org.sagebionetworks.repo.model.table.TableEntity";
	private static final String MATERIALIZED_VIEW = "org.sagebionetworks.repo.model.table.MaterializedView";
	private static final String SEARCH_INDEX = "org.sagebionetworks.repo.model.table.SearchIndex";

	@Mock
	private DefiningSqlDependencyDao mockDefiningSqlDependencyDao;
	@Mock
	private EntityAuthorizationManager mockEntityAuthorizationManager;
	@Mock
	private NodeDAO mockNodeDao;
	@Mock
	private TableStatusDAO mockTableStatusDao;

	@InjectMocks
	private TableLineageManager manager;

	private UserInfo user;
	private Date changedOn;

	// The recorded dependencies, keyed by dependent: what each object is built from.
	private Map<IdAndVersion, Set<IdAndVersion>> sources;
	private Map<Long, EntityHeader> existing;
	private Set<Long> readable;
	private Map<IdAndVersion, TableStatus> statuses;

	/**
	 * syn1 (table) feeds syn2, which syn3 (the root) is built from; syn4 is built from syn3, and the search index syn6
	 * from syn4. syn2 also reads syn5, a view the user cannot read, which is built from syn8, and syn9, which was
	 * deleted. syn7 is a sibling: built from syn1 but neither upstream nor downstream of syn3.
	 */
	@BeforeEach
	public void before() {
		user = new UserInfo(false, 77L, AuthorizationConstants.DEFAULT_REALM_ID);
		changedOn = new Date(1_700_000_000_000L);
		sources = new HashMap<>();
		existing = new HashMap<>();
		readable = new HashSet<>();
		statuses = new HashMap<>();

		addEntity(1, "participants", TABLE, true);
		addEntity(2, "joined", MATERIALIZED_VIEW, true);
		addEntity(3, "cohort", MATERIALIZED_VIEW, true);
		addEntity(4, "summary", MATERIALIZED_VIEW, true);
		addEntity(5, "private", MATERIALIZED_VIEW, false);
		addEntity(6, "summary search", SEARCH_INDEX, true);
		addEntity(7, "sibling", MATERIALIZED_VIEW, true);
		addEntity(8, "hidden", TABLE, true);

		addSources(2, 1, 5, 9);
		addSources(3, 2);
		addSources(4, 3);
		addSources(5, 8);
		addSources(6, 4);
		addSources(7, 1);

		addStatus(IdAndVersion.parse("syn2"), TableState.AVAILABLE, null);
		addStatus(IdAndVersion.parse("syn3"), TableState.AVAILABLE, null);
		addStatus(IdAndVersion.parse("syn4"), TableState.PROCESSING, null);
		addStatus(IdAndVersion.parse("syn6"), TableState.PROCESSING_FAILED, "Search is not available");
	}

	@Test
	public void testGetLineageWithBoth() {
		setupGraph();

		// call under test
		TableLineage result = manager.getLineage(user, IdAndVersion.parse("syn3"), LineageDirection.BOTH, null);

		TableLineage expected = new TableLineage().setRootId("syn3")
				.setNodes(List.of(readableNode(3, "cohort", MATERIALIZED_VIEW, TableState.AVAILABLE, null),
						readableNode(2, "joined", MATERIALIZED_VIEW, TableState.AVAILABLE, null),
						readableNode(1, "participants", TABLE, null, null),
						new TableLineageNode().setId("syn5").setAccess(LineageAccess.RESTRICTED),
						new TableLineageNode().setId("syn9").setAccess(LineageAccess.NOT_FOUND),
						readableNode(4, "summary", MATERIALIZED_VIEW, TableState.PROCESSING, null),
						readableNode(6, "summary search", SEARCH_INDEX, TableState.PROCESSING_FAILED,
								"Search is not available")))
				.setEdges(List.of(edge("syn2", "syn3"), edge("syn1", "syn2"), edge("syn5", "syn2"),
						edge("syn9", "syn2"), edge("syn3", "syn4"), edge("syn4", "syn6")))
				.setIsTruncated(false);
		assertEquals(expected, result);
	}

	@Test
	public void testGetLineageWithNullDirection() {
		setupGraph();

		// call under test
		TableLineage result = manager.getLineage(user, IdAndVersion.parse("syn3"), null, null);

		assertEquals(List.of("syn3", "syn2", "syn1", "syn5", "syn9", "syn4", "syn6"), nodeIds(result));
	}

	@Test
	public void testGetLineageWithUpstream() {
		setupUpstream();

		// call under test
		TableLineage result = manager.getLineage(user, IdAndVersion.parse("syn3"), LineageDirection.UPSTREAM, null);

		assertEquals(List.of("syn3", "syn2", "syn1", "syn5", "syn9"), nodeIds(result));
		assertEquals(List.of(edge("syn2", "syn3"), edge("syn1", "syn2"), edge("syn5", "syn2"), edge("syn9", "syn2")),
				result.getEdges());
		assertFalse(result.getIsTruncated());
	}

	@Test
	public void testGetLineageWithDownstream() {
		setupDownstream();

		// call under test
		TableLineage result = manager.getLineage(user, IdAndVersion.parse("syn3"), LineageDirection.DOWNSTREAM, null);

		assertEquals(List.of("syn3", "syn4", "syn6"), nodeIds(result));
		assertEquals(List.of(edge("syn3", "syn4"), edge("syn4", "syn6")), result.getEdges());
		assertFalse(result.getIsTruncated());
	}

	@Test
	public void testGetLineageWithDownstreamFromTable() {
		setupDownstream();

		// call under test
		TableLineage result = manager.getLineage(user, IdAndVersion.parse("syn1"), LineageDirection.DOWNSTREAM, null);

		// Both views built from syn1 are downstream of it, and the walk continues through each of them.
		assertEquals(List.of("syn1", "syn2", "syn7", "syn3", "syn4", "syn6"), nodeIds(result));
	}

	@Test
	public void testGetLineageWithMaxDepth() {
		setupGraph();

		// call under test
		TableLineage result = manager.getLineage(user, IdAndVersion.parse("syn3"), LineageDirection.BOTH, 1);

		assertEquals(List.of("syn3", "syn2", "syn4"), nodeIds(result));
		assertEquals(List.of(edge("syn2", "syn3"), edge("syn3", "syn4")), result.getEdges());
		assertTrue(result.getIsTruncated());
	}

	@Test
	public void testGetLineageWithMaxDepthAtTheEndOfTheGraph() {
		setupDownstream();

		// call under test
		TableLineage result = manager.getLineage(user, IdAndVersion.parse("syn3"), LineageDirection.DOWNSTREAM, 2);

		assertEquals(List.of("syn3", "syn4", "syn6"), nodeIds(result));
		assertFalse(result.getIsTruncated());
	}

	@Test
	public void testGetLineageWithNoDependencies() {
		addEntity(10, "standalone", TABLE, true);
		setupRootAccess();
		when(mockDefiningSqlDependencyDao.getSourceTablesOfObjects(anySet())).thenReturn(List.of());
		setupDownstreamDao();
		setupHeaders();
		setupStatuses();

		// call under test
		TableLineage result = manager.getLineage(user, IdAndVersion.parse("syn10"), LineageDirection.BOTH, null);

		TableLineage expected = new TableLineage().setRootId("syn10")
				.setNodes(List.of(readableNode(10, "standalone", TABLE, null, null))).setEdges(List.of())
				.setIsTruncated(false);
		assertEquals(expected, result);
	}

	@Test
	public void testGetLineageWithVersionedSource() {
		IdAndVersion pinned = IdAndVersion.parse("syn1.4");
		sources.put(IdAndVersion.parse("syn3"), Set.of(pinned));
		addStatus(pinned, TableState.AVAILABLE, null);
		setupUpstream();

		// call under test
		TableLineage result = manager.getLineage(user, IdAndVersion.parse("syn3"), LineageDirection.UPSTREAM, null);

		assertEquals(List.of(readableNode(3, "cohort", MATERIALIZED_VIEW, TableState.AVAILABLE, null),
				readableNode(1, "participants", TABLE, TableState.AVAILABLE, null).setId("syn1.4")), result.getNodes());
		assertEquals(List.of(edge("syn1.4", "syn3")), result.getEdges());
	}

	@Test
	public void testGetLineageWithMoreThanMaxNodes() {
		IdAndVersion root = IdAndVersion.parse("syn1000");
		addEntity(1000, "wide", TABLE, true);
		List<Dependency> dependents = LongStream.rangeClosed(1001, 1600).mapToObj(id -> {
			addEntity(id, "view " + id, MATERIALIZED_VIEW, true);
			return new Dependency(IdAndVersion.newBuilder().setId(id).build(), root);
		}).collect(Collectors.toList());
		when(mockEntityAuthorizationManager.hasAccess(user, "1000", ACCESS_TYPE.READ))
				.thenReturn(AuthorizationStatus.authorized());
		when(mockDefiningSqlDependencyDao.getDependentsOfSources(anySet(), anyLong())).thenAnswer(invocation -> {
			Set<IdAndVersion> sourceIds = invocation.getArgument(0);
			long limit = invocation.getArgument(1);
			return sourceIds.contains(root) ? dependents.subList(0, (int) limit) : List.of();
		});
		setupEntities();
		setupStatuses();

		// call under test
		TableLineage result = manager.getLineage(user, root, LineageDirection.DOWNSTREAM, null);

		assertEquals(TableLineageManager.MAX_NODES, result.getNodes().size());
		assertEquals(TableLineageManager.MAX_NODES - 1, result.getEdges().size());
		assertTrue(result.getIsTruncated());
		// A hub's dependents are read only up to what the graph can hold.
		verify(mockDefiningSqlDependencyDao).getDependentsOfSources(Set.of(root), TableLineageManager.MAX_NODES + 1);
	}

	@Test
	public void testGetLineageWithTrashedDependent() {
		// syn4 was moved to the trash: its dependency on syn3 stays until the trash is purged.
		existing.get(4L).setBenefactorId(BOOTSTRAP_NODES.TRASH.getId());
		setupDownstream();

		// call under test
		TableLineage result = manager.getLineage(user, IdAndVersion.parse("syn3"), LineageDirection.DOWNSTREAM, null);

		assertEquals(List.of(readableNode(3, "cohort", MATERIALIZED_VIEW, TableState.AVAILABLE, null),
				new TableLineageNode().setId("syn4").setAccess(LineageAccess.NOT_FOUND)), result.getNodes());
		assertEquals(List.of(edge("syn3", "syn4")), result.getEdges());
	}

	@Test
	public void testGetLineageWithOneReadPerLevel() {
		setupGraph();

		// call under test
		manager.getLineage(user, IdAndVersion.parse("syn3"), LineageDirection.BOTH, null);

		// Upstream: syn3, then syn2, then syn1 (syn5 is restricted, syn9 deleted, so neither is expanded), which has
		// none. Downstream: syn3, syn4, then syn6, which has none. Statuses for the whole graph at once.
		verify(mockDefiningSqlDependencyDao, times(3)).getSourceTablesOfObjects(anySet());
		verify(mockDefiningSqlDependencyDao, times(3)).getDependentsOfSources(anySet(), anyLong());
		verify(mockTableStatusDao, times(1)).getTableStatuses(anySet());
	}

	@Test
	public void testGetLineageWithStaleViews() {
		Date later = new Date(changedOn.getTime() + 1000);
		// participants changed after joined was built; joined has not rebuilt yet, so cohort is behind too.
		addStatus(IdAndVersion.parse("syn1"), TableState.AVAILABLE, null);
		statuses.get(IdAndVersion.parse("syn1")).setChangedOn(later);
		addStatus(IdAndVersion.parse("syn4"), TableState.AVAILABLE, null);
		setupGraph();

		// call under test
		TableLineage result = manager.getLineage(user, IdAndVersion.parse("syn3"), LineageDirection.BOTH, null);

		// syn6 failed, so it is not AVAILABLE and not stale; its source syn4 is.
		assertEquals(Map.of("syn1", false, "syn2", true, "syn3", true, "syn4", true, "syn6", false),
				getStaleness(result));
	}

	@Test
	public void testGetLineageWithSourceProcessing() {
		addStatus(IdAndVersion.parse("syn1"), TableState.PROCESSING, null);
		setupUpstream();

		// call under test
		TableLineage result = manager.getLineage(user, IdAndVersion.parse("syn3"), LineageDirection.UPSTREAM, null);

		// participants is being rebuilt, so joined will rebuild after it, and cohort after joined.
		assertEquals(Map.of("syn1", false, "syn2", true, "syn3", true), getStaleness(result));
	}

	@Test
	public void testGetLineageWithUnreadableRoot() {
		when(mockEntityAuthorizationManager.hasAccess(user, "5", ACCESS_TYPE.READ))
				.thenReturn(AuthorizationStatus.accessDenied("You lack READ access to the requested entity."));

		assertThrows(UnauthorizedException.class, () -> {
			// call under test
			manager.getLineage(user, IdAndVersion.parse("syn5"), LineageDirection.BOTH, null);
		});

		verifyNoInteractions(mockDefiningSqlDependencyDao, mockNodeDao, mockTableStatusDao);
	}

	@Test
	public void testGetLineageWithMaxDepthTooSmall() {
		String message = assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			manager.getLineage(user, IdAndVersion.parse("syn3"), LineageDirection.BOTH, 0);
		}).getMessage();

		assertEquals("maxDepth must be between 1 and 50", message);
		verifyNoInteractions(mockEntityAuthorizationManager, mockDefiningSqlDependencyDao);
	}

	@Test
	public void testGetLineageWithMaxDepthTooLarge() {
		String message = assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			manager.getLineage(user, IdAndVersion.parse("syn3"), LineageDirection.BOTH, 51);
		}).getMessage();

		assertEquals("maxDepth must be between 1 and 50", message);
		verifyNoInteractions(mockEntityAuthorizationManager, mockDefiningSqlDependencyDao);
	}

	@Test
	public void testGetLineageWithNullUser() {
		assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			manager.getLineage(null, IdAndVersion.parse("syn3"), LineageDirection.BOTH, null);
		});
		verifyNoInteractions(mockEntityAuthorizationManager, mockDefiningSqlDependencyDao);
	}

	@Test
	public void testGetLineageWithNullRoot() {
		assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			manager.getLineage(user, null, LineageDirection.BOTH, null);
		});
		verifyNoInteractions(mockEntityAuthorizationManager, mockDefiningSqlDependencyDao);
	}

	private void addEntity(long id, String name, String type, boolean canRead) {
		existing.put(id, new EntityHeader().setId(KeyFactory.keyToString(id)).setName(name).setType(type));
		if (canRead) {
			readable.add(id);
		}
	}

	private void addSources(long dependent, long... sourceIds) {
		sources.put(IdAndVersion.newBuilder().setId(dependent).build(), LongStream.of(sourceIds)
				.mapToObj(id -> IdAndVersion.newBuilder().setId(id).build()).collect(Collectors.toSet()));
	}

	private void addStatus(IdAndVersion id, TableState state, String errorMessage) {
		statuses.put(id, new TableStatus().setTableId(id.getId().toString()).setState(state).setChangedOn(changedOn)
				.setErrorMessage(errorMessage));
	}

	private void setupGraph() {
		setupUpstream();
		setupDownstreamDao();
	}

	private void setupUpstream() {
		setupRootAccess();
		when(mockDefiningSqlDependencyDao.getSourceTablesOfObjects(anySet())).thenAnswer(invocation -> {
			Set<IdAndVersion> dependentIds = invocation.getArgument(0);
			// The DAO reports dependencies in its own order; the manager orders each node's edges.
			List<Dependency> dependencies = new ArrayList<>();
			dependentIds.forEach(dependent -> sources.getOrDefault(dependent, Set.of())
					.forEach(source -> dependencies.add(new Dependency(dependent, source))));
			return dependencies;
		});
		setupEntities();
		setupStatuses();
	}

	private void setupDownstream() {
		setupRootAccess();
		setupDownstreamDao();
		setupEntities();
		setupStatuses();
	}

	private void setupDownstreamDao() {
		when(mockDefiningSqlDependencyDao.getDependentsOfSources(anySet(), anyLong())).thenAnswer(invocation -> {
			Set<IdAndVersion> sourceIds = invocation.getArgument(0);
			// The DAO reports dependents in its own order; the manager orders each node's edges.
			List<Dependency> dependents = new ArrayList<>();
			sources.forEach((dependent, its) -> its.stream().filter(sourceIds::contains)
					.forEach(source -> dependents.add(new Dependency(dependent, source))));
			dependents.sort((a, b) -> Long.compare(b.objectId().getId(), a.objectId().getId()));
			return dependents;
		});
	}

	private void setupRootAccess() {
		when(mockEntityAuthorizationManager.hasAccess(eq(user), any(String.class), eq(ACCESS_TYPE.READ)))
				.thenReturn(AuthorizationStatus.authorized());
	}

	private void setupEntities() {
		setupHeaders();
		when(mockEntityAuthorizationManager.batchHasAccess(eq(user), anyList(), eq(ACCESS_TYPE.READ)))
				.thenAnswer(invocation -> {
					Collection<Long> ids = invocation.getArgument(1);
					return ids.stream().map(id -> new UsersEntityAccessInfo().withEntityId(id)
							.withAuthorizationStatus(readable.contains(id) ? AuthorizationStatus.authorized()
									: AuthorizationStatus.accessDenied("denied")))
							.collect(Collectors.toList());
				});
	}

	private void setupHeaders() {
		when(mockNodeDao.getEntityHeader(anySet())).thenAnswer(invocation -> {
			Set<Long> ids = invocation.getArgument(0);
			return ids.stream().filter(existing::containsKey).map(existing::get).collect(Collectors.toList());
		});
	}

	private void setupStatuses() {
		when(mockTableStatusDao.getTableStatuses(anySet())).thenAnswer(invocation -> {
			Set<IdAndVersion> ids = invocation.getArgument(0);
			return ids.stream().filter(statuses::containsKey).collect(Collectors.toMap(id -> id, statuses::get));
		});
	}

	private TableLineageNode readableNode(long id, String name, String type, TableState state, String errorMessage) {
		TableLineageNode node = new TableLineageNode().setId("syn" + id).setAccess(LineageAccess.READABLE).setName(name)
				.setConcreteType(type);
		if (state != null) {
			node.setTableState(state).setStateChangedOn(changedOn).setErrorMessage(errorMessage).setIsStale(false);
		}
		return node;
	}

	private static TableLineageEdge edge(String sourceId, String dependentId) {
		return new TableLineageEdge().setSourceId(sourceId).setDependentId(dependentId);
	}

	private static Map<String, Boolean> getStaleness(TableLineage lineage) {
		return lineage.getNodes().stream().filter(node -> node.getIsStale() != null)
				.collect(Collectors.toMap(TableLineageNode::getId, TableLineageNode::getIsStale));
	}

	private static List<String> nodeIds(TableLineage lineage) {
		return lineage.getNodes().stream().map(TableLineageNode::getId).collect(Collectors.toList());
	}
}
