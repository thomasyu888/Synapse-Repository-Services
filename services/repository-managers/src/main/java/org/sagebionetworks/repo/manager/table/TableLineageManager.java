package org.sagebionetworks.repo.manager.table;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.sagebionetworks.repo.manager.entity.EntityAuthorizationManager;
import org.sagebionetworks.repo.manager.entity.decider.UsersEntityAccessInfo;
import org.sagebionetworks.repo.model.ACCESS_TYPE;
import org.sagebionetworks.repo.model.EntityHeader;
import org.sagebionetworks.repo.model.NodeConstants.BOOTSTRAP_NODES;
import org.sagebionetworks.repo.model.NodeDAO;
import org.sagebionetworks.repo.model.UnauthorizedException;
import org.sagebionetworks.repo.model.UserInfo;
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
import org.sagebionetworks.repo.web.NotFoundException;
import org.sagebionetworks.util.ValidateArgument;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Builds the lineage graph of a table or view from the recorded defining-SQL dependencies of materialized views and
 * search indexes.
 */
@Service
public class TableLineageManager {

	public static final int DEFAULT_MAX_DEPTH = 10;

	public static final int MAX_MAX_DEPTH = 50;

	/**
	 * The maximum number of nodes in a single lineage graph, the root included.
	 */
	public static final int MAX_NODES = 500;

	private static final Long TRASH_FOLDER_ID = BOOTSTRAP_NODES.TRASH.getId();

	private static final Comparator<IdAndVersion> ID_ORDER = Comparator.comparing(IdAndVersion::getId)
			.thenComparing(id -> id.getVersion().orElse(-1L));

	private final DefiningSqlDependencyDao definingSqlDependencyDao;
	private final EntityAuthorizationManager entityAuthorizationManager;
	private final NodeDAO nodeDao;
	private final TableStatusDAO tableStatusDao;

	@Autowired
	public TableLineageManager(DefiningSqlDependencyDao definingSqlDependencyDao,
			EntityAuthorizationManager entityAuthorizationManager, NodeDAO nodeDao, TableStatusDAO tableStatusDao) {
		this.definingSqlDependencyDao = definingSqlDependencyDao;
		this.entityAuthorizationManager = entityAuthorizationManager;
		this.nodeDao = nodeDao;
		this.tableStatusDao = tableStatusDao;
	}

	/**
	 * Get the lineage of the given table or view.
	 * <p>
	 * The caller must have READ on the root. Every other object is included with its name, type and index state when
	 * the caller can read it; otherwise it is included by id only, as RESTRICTED, and the lineage does not continue
	 * past it.
	 *
	 * @param userInfo  The caller
	 * @param rootId    The table or view to start from
	 * @param direction Which dependencies to follow; when null, {@link LineageDirection#BOTH}
	 * @param maxDepth  How many dependency steps to follow in each direction, from 1 to {@link #MAX_MAX_DEPTH}; when
	 *                  null, {@link #DEFAULT_MAX_DEPTH}
	 * @return The lineage graph, with isTruncated set when maxDepth or {@link #MAX_NODES} cut it short
	 * @throws UnauthorizedException When the caller cannot read the root
	 * @throws NotFoundException     When the root does not exist
	 */
	public TableLineage getLineage(UserInfo userInfo, IdAndVersion rootId, LineageDirection direction, Integer maxDepth) {
		ValidateArgument.required(userInfo, "userInfo");
		ValidateArgument.required(rootId, "rootId");
		LineageDirection resolvedDirection = direction == null ? LineageDirection.BOTH : direction;
		int resolvedMaxDepth = maxDepth == null ? DEFAULT_MAX_DEPTH : maxDepth;
		ValidateArgument.requirement(resolvedMaxDepth >= 1 && resolvedMaxDepth <= MAX_MAX_DEPTH,
				"maxDepth must be between 1 and " + MAX_MAX_DEPTH);

		entityAuthorizationManager.hasAccess(userInfo, rootId.getId().toString(), ACCESS_TYPE.READ)
				.checkAuthorizationOrElseThrow();

		LineageGraph graph = new LineageGraph(rootId);
		if (resolvedDirection != LineageDirection.DOWNSTREAM) {
			walk(userInfo, graph, resolvedMaxDepth, this::getUpstreamEdges);
		}
		if (resolvedDirection != LineageDirection.UPSTREAM) {
			walk(userInfo, graph, resolvedMaxDepth, this::getDownstreamEdges);
		}

		return new TableLineage()
				.setRootId(rootId.toString())
				.setNodes(createNodes(graph))
				.setEdges(new ArrayList<>(graph.edges))
				.setIsTruncated(graph.truncated);
	}

	/**
	 * Breadth-first walk from the root in one direction. Each level's edges are read together and its new nodes are
	 * access-checked together, and only the readable ones are expanded, so the graph never reveals what lies beyond an
	 * object the caller cannot read.
	 */
	void walk(UserInfo userInfo, LineageGraph graph, int maxDepth, Function<Set<IdAndVersion>, Level> edgesOf) {
		List<IdAndVersion> frontier = List.of(graph.rootId);
		for (int depth = 0; !frontier.isEmpty(); depth++) {
			Level level = edgesOf.apply(new LinkedHashSet<>(frontier));
			if (depth == maxDepth) {
				if (!level.edges().isEmpty()) {
					graph.truncated = true;
				}
				return;
			}
			if (level.isIncomplete()) {
				graph.truncated = true;
			}
			List<IdAndVersion> discovered = new ArrayList<>();
			for (IdAndVersion id : frontier) {
				for (Edge edge : level.edges().getOrDefault(id, List.of())) {
					if (!graph.access.containsKey(edge.neighbor)) {
						if (graph.access.size() >= MAX_NODES) {
							graph.truncated = true;
							continue;
						}
						graph.access.put(edge.neighbor, null);
						discovered.add(edge.neighbor);
					}
					graph.edges.add(edge.edge);
				}
			}
			resolveAccess(userInfo, graph, discovered);
			frontier = discovered.stream().filter(id -> graph.access.get(id) == LineageAccess.READABLE)
					.collect(Collectors.toList());
		}
	}

	Level getUpstreamEdges(Set<IdAndVersion> dependentIds) {
		Map<IdAndVersion, List<Edge>> edges = new HashMap<>();
		for (Dependency dependency : definingSqlDependencyDao.getSourceTablesOfObjects(dependentIds)) {
			edges.computeIfAbsent(dependency.objectId(), k -> new ArrayList<>()).add(new Edge(
					dependency.sourceTableId(), edge(dependency.sourceTableId(), dependency.objectId())));
		}
		return new Level(sortByNeighbor(edges), false);
	}

	/**
	 * A source table can have any number of dependents, while a graph holds at most {@link #MAX_NODES}, so one more
	 * dependency than that is read per level: reading more cannot add a node, and reading one more shows that some
	 * were left out.
	 */
	Level getDownstreamEdges(Set<IdAndVersion> sourceIds) {
		List<Dependency> dependents = definingSqlDependencyDao.getDependentsOfSources(sourceIds, MAX_NODES + 1);
		Map<IdAndVersion, List<Edge>> edges = new HashMap<>();
		for (Dependency dependency : dependents) {
			edges.computeIfAbsent(dependency.sourceTableId(), k -> new ArrayList<>()).add(new Edge(
					dependency.objectId(), edge(dependency.sourceTableId(), dependency.objectId())));
		}
		return new Level(sortByNeighbor(edges), dependents.size() > MAX_NODES);
	}

	private static Map<IdAndVersion, List<Edge>> sortByNeighbor(Map<IdAndVersion, List<Edge>> edges) {
		edges.values().forEach(list -> list.sort(Comparator.comparing(Edge::neighbor, ID_ORDER)));
		return edges;
	}

	/**
	 * A recorded dependency can outlive the entity it names: a deleted table, or one in the trash, whose dependency is
	 * only removed when the trash is purged. Either is reported as NOT_FOUND rather than treated as an error.
	 */
	void resolveAccess(UserInfo userInfo, LineageGraph graph, List<IdAndVersion> ids) {
		if (ids.isEmpty()) {
			return;
		}
		Set<Long> entityIds = ids.stream().map(IdAndVersion::getId).collect(Collectors.toCollection(LinkedHashSet::new));
		graph.headers.putAll(nodeDao.getEntityHeader(entityIds).stream()
				.collect(Collectors.toMap(h -> KeyFactory.stringToKey(h.getId()), Function.identity())));
		Set<Long> readable = entityAuthorizationManager
				.batchHasAccess(userInfo, new ArrayList<>(entityIds), ACCESS_TYPE.READ).stream()
				.filter(info -> info.getAuthorizationStatus().isAuthorized()).map(UsersEntityAccessInfo::getEntityId)
				.collect(Collectors.toSet());
		for (IdAndVersion id : ids) {
			EntityHeader header = graph.headers.get(id.getId());
			if (header == null || TRASH_FOLDER_ID.equals(header.getBenefactorId())) {
				graph.access.put(id, LineageAccess.NOT_FOUND);
			} else if (readable.contains(id.getId())) {
				graph.access.put(id, LineageAccess.READABLE);
			} else {
				graph.access.put(id, LineageAccess.RESTRICTED);
			}
		}
	}

	List<TableLineageNode> createNodes(LineageGraph graph) {
		if (!graph.headers.containsKey(graph.rootId.getId())) {
			graph.headers.putAll(nodeDao.getEntityHeader(Set.of(graph.rootId.getId())).stream()
					.collect(Collectors.toMap(h -> KeyFactory.stringToKey(h.getId()), Function.identity())));
		}
		// An object has no status until its index is first requested, so not every readable node has one.
		Set<IdAndVersion> readableIds = graph.access.entrySet().stream()
				.filter(entry -> entry.getValue() == LineageAccess.READABLE).map(Map.Entry::getKey)
				.collect(Collectors.toCollection(LinkedHashSet::new));
		Map<IdAndVersion, TableStatus> statuses = tableStatusDao.getTableStatuses(readableIds);
		Map<IdAndVersion, List<IdAndVersion>> readableSources = graph.edges.stream()
				.map(edge -> new Dependency(IdAndVersion.parse(edge.getDependentId()),
						IdAndVersion.parse(edge.getSourceId())))
				.filter(dependency -> readableIds.contains(dependency.sourceTableId()))
				.collect(Collectors.groupingBy(Dependency::objectId,
						Collectors.mapping(Dependency::sourceTableId, Collectors.toList())));
		Map<IdAndVersion, Boolean> staleness = new HashMap<>();
		List<TableLineageNode> nodes = new ArrayList<>(graph.access.size());
		graph.access.forEach((id, access) -> nodes.add(createNode(id, access, graph.headers.get(id.getId()),
				statuses.get(id), isStale(readableSources, statuses, staleness, id))));
		return nodes;
	}

	TableLineageNode createNode(IdAndVersion id, LineageAccess access, EntityHeader header, TableStatus status,
			boolean isStale) {
		TableLineageNode node = new TableLineageNode().setId(id.toString()).setAccess(access);
		if (access != LineageAccess.READABLE) {
			return node;
		}
		node.setName(header.getName()).setConcreteType(header.getType());
		if (status != null) {
			node.setTableState(status.getState()).setStateChangedOn(status.getChangedOn())
					.setErrorMessage(status.getErrorMessage()).setIsStale(isStale);
		}
		return node;
	}

	/**
	 * Whether an AVAILABLE object is due a rebuild, judged from the sources in the graph alone: a source that changed
	 * after the object was built, that is being rebuilt, or that is itself stale will make the object rebuild. A view
	 * stays AVAILABLE while it rebuilds, so this is the only sign of a change still travelling downstream.
	 */
	boolean isStale(Map<IdAndVersion, List<IdAndVersion>> readableSources, Map<IdAndVersion, TableStatus> statuses,
			Map<IdAndVersion, Boolean> staleness, IdAndVersion id) {
		Boolean known = staleness.get(id);
		if (known != null) {
			return known;
		}
		// Recorded dependencies cannot form a cycle, but the walk does not rely on it: an object being decided is
		// treated as not stale should it be reached again.
		staleness.put(id, false);
		TableStatus status = statuses.get(id);
		boolean stale = status != null && status.getState() == TableState.AVAILABLE
				&& readableSources.getOrDefault(id, List.of()).stream()
						.anyMatch(sourceId -> isSourceAhead(statuses.get(sourceId), status)
								|| isStale(readableSources, statuses, staleness, sourceId));
		staleness.put(id, stale);
		return stale;
	}

	private static boolean isSourceAhead(TableStatus source, TableStatus dependent) {
		if (source == null) {
			return false;
		}
		if (source.getState() == TableState.PROCESSING) {
			return true;
		}
		return source.getState() == TableState.AVAILABLE && source.getChangedOn() != null
				&& dependent.getChangedOn() != null && source.getChangedOn().after(dependent.getChangedOn());
	}

	private static TableLineageEdge edge(IdAndVersion sourceId, IdAndVersion dependentId) {
		return new TableLineageEdge().setSourceId(sourceId.toString()).setDependentId(dependentId.toString());
	}

	/**
	 * An edge reached while walking, with the node at its far end.
	 */
	record Edge(IdAndVersion neighbor, TableLineageEdge edge) {}

	/**
	 * The edges of one level of the walk, keyed by the frontier node they start from, each list in neighbor order.
	 * isIncomplete is set when some edges were left out.
	 */
	record Level(Map<IdAndVersion, List<Edge>> edges, boolean isIncomplete) {}

	/**
	 * The graph built so far. A node's access is null between its discovery and the access check of its level.
	 */
	static class LineageGraph {
		final IdAndVersion rootId;
		final Map<IdAndVersion, LineageAccess> access = new LinkedHashMap<>();
		final Set<TableLineageEdge> edges = new LinkedHashSet<>();
		final Map<Long, EntityHeader> headers = new LinkedHashMap<>();
		boolean truncated = false;

		LineageGraph(IdAndVersion rootId) {
			this.rootId = rootId;
			this.access.put(rootId, LineageAccess.READABLE);
		}
	}
}
