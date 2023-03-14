package org.heigit.ohsome.oshdb.store.update;

import static java.util.Arrays.stream;
import static java.util.Comparator.comparingInt;
import static java.util.function.Predicate.not;
import static org.heigit.ohsome.oshdb.osm.OSMType.NODE;
import static org.heigit.ohsome.oshdb.osm.OSMType.RELATION;
import static org.heigit.ohsome.oshdb.osm.OSMType.WAY;
import static org.heigit.ohsome.oshdb.store.BackRefType.*;

import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.heigit.ohsome.oshdb.OSHDB;
import org.heigit.ohsome.oshdb.impl.osh.OSHEntityImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHWayImpl;
import org.heigit.ohsome.oshdb.index.XYGridTree;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osh.OSHRelation;
import org.heigit.ohsome.oshdb.osh.OSHWay;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.store.BackRef;
import org.heigit.ohsome.oshdb.store.OSHDBStore;
import org.heigit.ohsome.oshdb.store.OSHData;
import org.heigit.ohsome.oshdb.util.CellId;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;

public class Updates {

  private static final Comparator<OSMEntity> VERSION_REVERSE_ORDER = comparingInt(
      OSMEntity::getVersion).reversed();

  private static final XYGridTree gridIndex = new XYGridTree(OSHDB.MAXZOOM);
  private static final CellId ZERO = CellId.fromLevelId(0);

  private final Set<CellId> gridUpdates = new HashSet<>();

  private final OSHDBStore store;
  private final Map<OSMType, Set<Long>> minorUpdates;
  private final Map<OSMType, Set<Long>> updatedEntities;

  public Updates(OSHDBStore store, Map<OSMType, Set<Long>> minorUpdates,
      Map<OSMType, Set<Long>> updatedEntities) {
    this.store = store;
    this.minorUpdates = minorUpdates;
    this.updatedEntities = updatedEntities;
  }

  public Set<CellId> getGridUpdates(){
    return gridUpdates;
  }

  public Flux<OSHData> nodes(Map<Long, Collection<OSMNode>> entities){
    var dataMap = store.entities(NODE, entities.keySet());
    var backRefMap = store.backRefs(NODE, entities.keySet());
    return Flux.fromIterable(entities.entrySet())
        .map(entry -> node(dataMap, backRefMap, entry));
  }

  private OSHData node(Map<Long, OSHData> dataMap, Map<Long, BackRef> backRefMap,
      Entry<Long, Collection<OSMNode>> entry) {
    var id = entry.getKey();
    var versions = entry.getValue();
    return node(id, versions, dataMap.get(id), backRefMap.get(id));
  }

  private OSHData node(long id, Collection<OSMNode> versions, OSHData data, BackRef backRef) {
    var mergedVersions = mergePrevious(data, OSMNode.class);
    var major = false;
    for (var version : versions) {
      major |= mergedVersions.add(version);
    }
    if (!major) {
      return data; // no updates
    }

    var osh = OSHNodeImpl.build(new ArrayList<>(mergedVersions));
    return getData(id, data, backRef, osh);
  }

  public Flux<OSHData> ways(Map<Long, Collection<OSMWay>> entities){
    var dataMap = store.entities(WAY, entities.keySet());
    var backRefMap = store.backRefs(WAY, entities.keySet());
    return Flux.fromIterable(entities.entrySet())
        .map(entry -> way(dataMap, backRefMap, entry))
        .filter(Objects::nonNull);
  }

  private OSHData way(Map<Long, OSHData> dataMap, Map<Long, BackRef> backRefMap,
      Entry<Long, Collection<OSMWay>> entry) {
    var id = entry.getKey();
    var versions = entry.getValue();
    return way(id, versions, dataMap.get(id), backRefMap.get(id));
  }

  private OSHData way(long id, Collection<OSMWay> versions, OSHData data, BackRef backRef) {
    var mergedVersions = mergePrevious(data, OSMWay.class);
    var members = new TreeMap<Long, OSHNode>();
    if (data != null) {
      OSHWay osh = data.getOSHEntity();
      osh.getNodes().forEach(node -> members.put(node.getId(), node));
    }
    var updatedNodes = updatedEntities.get(NODE);
    var updatedMembers = new TreeSet<Long>();
    members.keySet().stream().filter(updatedNodes::contains).forEach(updatedMembers::add);

    var minor = !updatedMembers.isEmpty();
    var major = false;
    for (var version : versions) {
      major |= mergedVersions.add(version);
    }
    if (!minor && !major) {
      return data; // no updates
    }

    var newMembers = new TreeSet<Long>();
    versions.stream()
        .flatMap(version -> stream(version.getMembers()))
        .map(OSMMember::getId)
        .filter(not(members::containsKey))
        .forEach(newMembers::add);

    updatedMembers.addAll(newMembers);
    store.entities(NODE, updatedMembers).values().stream()
        .map(member -> (OSHNode) member.getOSHEntity())
        .filter(Objects::nonNull)
        .forEach(node -> members.put(node.getId(), node));
    store.backRefsMerge(NODE_WAY, id, newMembers);

    var osh = OSHWayImpl.build(new ArrayList<>(mergedVersions), members.values());
    return getData(id, data, backRef, osh);
  }

  public Flux<OSHData> relations(Map<Long, Collection<OSMRelation>> entities){
    var dataMap = store.entities(RELATION, entities.keySet());
    var backRefMap = store.backRefs(RELATION, entities.keySet());
    return Flux.fromIterable(entities.entrySet())
        .map(entry -> relation(dataMap, backRefMap, entry));
  }

  private OSHData relation(Map<Long, OSHData> dataMap, Map<Long, BackRef> backRefMap,
      Entry<Long, Collection<OSMRelation>> entry) {
    var id = entry.getKey();
    var versions = entry.getValue();
    return relation(id, versions, dataMap.get(id), backRefMap.get(id));
  }

  private OSHData relation(long id, Collection<OSMRelation> versions, OSHData data, BackRef backRef) {
    var mergedVersions = mergePrevious(data, OSMRelation.class);
    var nodeMembers = new TreeMap<Long, OSHNode>();
    var wayMembers = new TreeMap<Long, OSHWay>();
    var relationMembers = new HashSet<Long>();
    if (data != null) {
      OSHRelation osh = data.getOSHEntity();
      osh.getNodes().forEach(node -> nodeMembers.put(node.getId(), node));
      osh.getWays().forEach(way -> wayMembers.put(way.getId(), way));
      Streams.stream(osh.getVersions())
          .flatMap(version -> Arrays.stream(version.getMembers()))
          .filter(member -> member.getType() == RELATION)
          .forEach(member -> relationMembers.add(member.getId()));
    }
    var updatedNodes = updatedEntities.get(NODE);
    var updatedNodeMembers = new TreeSet<Long>();
    nodeMembers.keySet().stream().filter(updatedNodes::contains).forEach(updatedNodeMembers::add);

    var updatedWays = updatedEntities.get(WAY);
    var updatedWayMembers = new TreeSet<Long>();
    wayMembers.keySet().stream().filter(updatedWays::contains).forEach(updatedWayMembers::add);

    var minor = !updatedNodeMembers.isEmpty() || !updatedWays.isEmpty();
    var major = false;
    for (var version : versions) {
      major |= mergedVersions.add(version);
    }
    if (!minor && !major) {
      return data; // no updates
    }

    var newNodeMembers = new TreeSet<Long>();
    versions.stream()
        .flatMap(version -> stream(version.getMembers()))
        .filter(member -> member.getType() == NODE)
        .map(OSMMember::getId)
        .filter(not(nodeMembers::containsKey))
        .forEach(newNodeMembers::add);
    updatedNodeMembers.addAll(newNodeMembers);
    store.entities(NODE, updatedNodeMembers).values().stream()
        .map(member -> (OSHNode) member.getOSHEntity())
        .filter(Objects::nonNull)
        .forEach(node -> nodeMembers.put(node.getId(), node));
    store.backRefsMerge(NODE_RELATION, id, newNodeMembers);

    var newWayMembers = new TreeSet<Long>();
    versions.stream()
        .flatMap(version -> stream(version.getMembers()))
        .filter(member -> member.getType() == WAY)
        .map(OSMMember::getId)
        .filter(not(wayMembers::containsKey))
        .forEach(newWayMembers::add);
    updatedWayMembers.addAll(newWayMembers);
    store.entities(WAY, updatedWayMembers).values().stream()
        .map(member -> (OSHWay) member.getOSHEntity())
        .filter(Objects::nonNull)
        .forEach(way -> wayMembers.put(way.getId(), way));
    store.backRefsMerge(WAY_RELATION, id, newWayMembers);

    var newRelationMembers = new TreeSet<Long>();
    versions.stream()
        .flatMap(version -> stream(version.getMembers()))
        .filter(member -> member.getType() == RELATION)
        .map(OSMMember::getId)
        .filter(not(relationMembers::contains))
        .forEach(newRelationMembers::add);
    store.backRefsMerge(RELATION_RELATION, id, newRelationMembers);

    var osh = OSHRelationImpl.build(new ArrayList<>(mergedVersions), nodeMembers.values(), wayMembers.values());
    return getData(id, data, backRef, osh);
  }

  @NotNull
  private OSHData getData(long id, OSHData data, BackRef backRef, OSHEntityImpl osh) {
    var cellId = gridIndex(osh);
    if (data != null) {
      var prevCellId = CellId.fromLevelId(data.getGridId());
      if (prevCellId.getZoomLevel() > 0 && prevCellId.getZoomLevel() < cellId.getZoomLevel()) {
        // keep entity in lower zoomlevel (
        cellId = prevCellId;
      } else if (prevCellId.getZoomLevel() > 0) {
        gridUpdates.add(prevCellId);
      }
    }

    forwardBackRefs(backRef);
    gridUpdates.add(cellId);
    updatedEntities(osh);
    return new OSHData(osh.getType(), id, cellId.getLevelId(), osh.getData());
  }

  private <T extends OSMEntity> TreeSet<T> mergePrevious(OSHData previous, Class<T> clazz) {
    var merged = new TreeSet<T>(VERSION_REVERSE_ORDER);
    if (previous != null) {
      var osh = previous.getOSHEntity();
      osh.getVersions().forEach(version -> merged.add((T) version));
    }
    return merged;
  }

  private void forwardBackRefs(BackRef backRefs) {
    if (backRefs != null) {
      backRefs.ways().forEach(backRef -> minorUpdates.get(WAY).add(backRef));
      backRefs.relations().forEach(backRef -> minorUpdates.get(RELATION).add(backRef));
    }
  }

  private CellId gridIndex(OSHEntity osh) {
    var bbox = osh.getBoundable().getBoundingBox();
    if (!bbox.isValid()) {
      return ZERO;
    }
    return gridIndex.getInsertId(bbox);
  }

  private void updatedEntities(OSHEntity osh) {
    updatedEntities.computeIfAbsent(osh.getType(), x -> new HashSet<>()).add(osh.getId());
  }
}
