package org.sagebionetworks.repo.manager.table;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.sagebionetworks.repo.manager.entity.EntityAuthorizationManager;
import org.sagebionetworks.repo.manager.entity.decider.UsersEntityAccessInfo;
import org.sagebionetworks.repo.model.ACCESS_TYPE;
import org.sagebionetworks.repo.model.EntityHeader;
import org.sagebionetworks.repo.model.NodeDAO;
import org.sagebionetworks.repo.model.UnauthorizedException;
import org.sagebionetworks.repo.model.UserInfo;
import org.sagebionetworks.repo.model.dao.table.TableStatusDAO;
import org.sagebionetworks.repo.model.dbo.dao.table.DefiningSqlDependencyDao;
import org.sagebionetworks.repo.model.entity.IdAndVersion;
import org.sagebionetworks.repo.model.jdo.KeyFactory;
import org.sagebionetworks.repo.model.table.LineageAccess;
import org.sagebionetworks.repo.model.table.LineageDirection;
import org.sagebionetworks.repo.model.table.TableLineage;
import org.sagebionetworks.repo.model.table.TableLineageEdge;
import org.sagebionetworks.repo.model.table.TableLineageNode;
import org.sagebionetworks.repo.model.table.TableStatus;
import org.sagebionetworks.repo.web.NotFoundException;
import org.sagebionetworks.util.PaginationIterator;
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

	private static final long DEPENDENTS_PAGE_SIZE = 1000;

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
	 * Breadth-first walk from the root in one direction. Each level's new nodes are access-checked together, and only
	 * the readable ones are expanded, so the graph never reveals what lies beyond an object the caller cannot read.
	 */
	void walk(UserInfo userInfo, LineageGraph graph, int maxDepth, Function<IdAndVersion, List<Edge>> edgesOf) {
		List<IdAndVersion> frontier = List.of(graph.rootId);
		for (int depth = 0; !frontier.isEmpty(); depth++) {
			if (depth == maxDepth) {
				if (frontier.stream().anyMatch(id -> !edgesOf.apply(id).isEmpty())) {
					graph.truncated = true;
				}
				return;
			}
			List<IdAndVersion> discovered = new ArrayList<>();
			for (IdAndVersion id : frontier) {
				for (Edge edge : edgesOf.apply(id)) {
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

	List<Edge> getUpstreamEdges(IdAndVersion dependentId) {
		return definingSqlDependencyDao.getSourceTables(dependentId).stream().sorted(ID_ORDER)
				.map(sourceId -> new Edge(sourceId, edge(sourceId, dependentId))).collect(Collectors.toList());
	}

	List<Edge> getDownstreamEdges(IdAndVersion sourceId) {
		List<Edge> edges = new ArrayList<>();
		new PaginationIterator<>(
				(limit, offset) -> definingSqlDependencyDao.getDependentsPage(sourceId, limit, offset),
				DEPENDENTS_PAGE_SIZE)
				.forEachRemaining(dependent -> edges.add(new Edge(dependent.objectId(), edge(sourceId, dependent.objectId()))));
		edges.sort(Comparator.comparing(e -> e.neighbor, ID_ORDER));
		return edges;
	}

	/**
	 * A recorded dependency can outlive the entity it names (a deleted source table), so a missing entity is reported
	 * as NOT_FOUND rather than treated as an error.
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
			if (!graph.headers.containsKey(id.getId())) {
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
		List<TableLineageNode> nodes = new ArrayList<>(graph.access.size());
		graph.access.forEach((id, access) -> nodes.add(createNode(id, access, graph.headers.get(id.getId()))));
		return nodes;
	}

	TableLineageNode createNode(IdAndVersion id, LineageAccess access, EntityHeader header) {
		TableLineageNode node = new TableLineageNode().setId(id.toString()).setAccess(access);
		if (access != LineageAccess.READABLE) {
			return node;
		}
		node.setName(header.getName()).setConcreteType(header.getType());
		getTableStatus(id).ifPresent(status -> node.setTableState(status.getState())
				.setStateChangedOn(status.getChangedOn()).setErrorMessage(status.getErrorMessage()));
		return node;
	}

	Optional<TableStatus> getTableStatus(IdAndVersion id) {
		try {
			return Optional.of(tableStatusDao.getTableStatus(id));
		} catch (NotFoundException e) {
			// An object has no status until its index is first requested.
			return Optional.empty();
		}
	}

	private static TableLineageEdge edge(IdAndVersion sourceId, IdAndVersion dependentId) {
		return new TableLineageEdge().setSourceId(sourceId.toString()).setDependentId(dependentId.toString());
	}

	/**
	 * An edge reached while walking, with the node at its far end.
	 */
	record Edge(IdAndVersion neighbor, TableLineageEdge edge) {}

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
