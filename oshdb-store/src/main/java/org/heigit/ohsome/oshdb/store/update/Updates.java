package org.heigit.ohsome.oshdb.store.update;

import static java.util.Collections.emptyList;
import static java.util.function.Predicate.not;
import static org.heigit.ohsome.oshdb.osm.OSMType.NODE;
import static org.heigit.ohsome.oshdb.osm.OSMType.RELATION;
import static org.heigit.ohsome.oshdb.osm.OSMType.WAY;
import static org.heigit.ohsome.oshdb.store.BackRefType.NODE_RELATION;
import static org.heigit.ohsome.oshdb.store.BackRefType.NODE_WAY;
import static org.heigit.ohsome.oshdb.store.BackRefType.RELATION_RELATION;
import static org.heigit.ohsome.oshdb.store.BackRefType.WAY_RELATION;
import static reactor.core.publisher.Mono.fromCallable;

import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
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
import org.heigit.ohsome.oshdb.osm.OSM;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class Updates {
  private static final Logger log = LoggerFactory.getLogger(Updates.class);

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

  public Flux<OSHEntity> nodes(Map<Long, Collection<OSMNode>> entities){
    var dataMap = store.entities(NODE, entities.keySet());
    var backRefMap = store.backRefs(NODE, entities.keySet());
    return Flux.fromIterable(entities.entrySet())
        .concatMap(entry -> Mono.fromCallable(() -> node(dataMap, backRefMap, entry)));
  }

  private OSHEntity node(Map<Long, OSHData> dataMap, Map<Long, BackRef> backRefMap,
      Entry<Long, Collection<OSMNode>> entry) {
    var id = entry.getKey();
    var versions = entry.getValue();
    return node(id, versions, dataMap.get(id), backRefMap.get(id));
  }

  private OSHEntity node(long id, Collection<OSMNode> newVersions, OSHData data, BackRef backRef) {
    var versions = new HashSet<OSMNode>();
    if (data != null) {
      OSHNode osh = data.getOSHEntity();
      osh.getVersions().forEach(versions::add);
    }
    var isMajorUpdate = versions.addAll(newVersions);
    if (!isMajorUpdate) {
      return null; // no updates
    }

    var osh = OSHNodeImpl.build(new ArrayList<>(versions));
    updateStore(id, data, backRef, osh);
    return osh;
  }

  public Flux<OSHEntity> ways(Map<Long, Collection<OSMWay>> entities){
    var dataMap = store.entities(WAY, entities.keySet());
    var backRefMap = store.backRefs(WAY, entities.keySet());
    return Flux.fromIterable(entities.entrySet())
        .concatMap(entry -> fromCallable(() -> way(dataMap, backRefMap, entry)));
  }

  private OSHEntity way(Map<Long, OSHData> dataMap, Map<Long, BackRef> backRefMap,
      Entry<Long, Collection<OSMWay>> entry) {
    var id = entry.getKey();
    var versions = entry.getValue();
    return way(id, versions, dataMap.get(id), backRefMap.get(id));
  }

  private OSHEntity way(long id, Collection<OSMWay> newVersions, OSHData data, BackRef backRef) {
    var versions = new HashSet<OSMWay>();
    var members = new TreeMap<Long, OSHNode>();
    if (data != null) {
      OSHWay osh = data.getOSHEntity();
      osh.getVersions().forEach(versions::add);
      osh.getNodes().forEach(node -> members.put(node.getId(), node));
    }
    var updatedNodes = updatedEntities.get(NODE);
    var updatedMembers = new TreeSet<Long>();
    members.keySet().stream().filter(updatedNodes::contains).forEach(updatedMembers::add);

    var isMajorUpdate = versions.addAll(newVersions);
    var isMinorUpdate = !updatedMembers.isEmpty();
    if (!isMinorUpdate && !isMajorUpdate) {
      return null; // no updates
    }

    var newMembers = newMembers(newVersions, OSMWay::getMembers, NODE, members);
    updatedMembers.addAll(newMembers);
    updateMembers(NODE, updatedMembers, members);
    store.backRefsMerge(NODE_WAY, id, newMembers);

    var osh = OSHWayImpl.build(new ArrayList<>(versions), members.values());
    updateStore(id, data, backRef, osh);
    return osh;
  }

  public Flux<OSHEntity> relations(Map<Long, Collection<OSMRelation>> entities){
    var dataMap = store.entities(RELATION, entities.keySet());
    var backRefMap = store.backRefs(RELATION, entities.keySet());
    return Flux.fromIterable(entities.entrySet())
        .concatMap(entry -> fromCallable(() -> relation(dataMap, backRefMap, entry)));
  }

  private OSHEntity relation(Map<Long, OSHData> dataMap, Map<Long, BackRef> backRefMap,
      Entry<Long, Collection<OSMRelation>> entry) {
    var id = entry.getKey();
    var versions = entry.getValue();
    return relation(id, versions, dataMap.get(id), backRefMap.get(id));
  }

  private static final OSHRelation DUMMY = OSHRelationImpl.build(
    new ArrayList<>(List.of(OSM.relation(0,0,0,0,0, new int[0], new OSMMember[0]))),
      emptyList(), emptyList());

  private OSHEntity relation(long id, Collection<OSMRelation> newVersions, OSHData data, BackRef backRef) {
    var versions = new HashSet<OSMRelation>();
    var nodeMembers = new TreeMap<Long, OSHNode>();
    var wayMembers = new TreeMap<Long, OSHWay>();
    var relationMembers = new TreeMap<Long, OSHRelation>();

    if (data != null) {
      OSHRelation osh = data.getOSHEntity();
      osh.getVersions().forEach(versions::add);
      osh.getNodes().forEach(node -> nodeMembers.put(node.getId(), node));
      osh.getWays().forEach(way -> wayMembers.put(way.getId(), way));

      Streams.stream(osh.getVersions())
          .flatMap(version -> Arrays.stream(version.getMembers()))
          .filter(member -> member.getType() == RELATION)
          .forEach(member -> relationMembers.put(member.getId(), DUMMY));
    }
    var updatedNodes = updatedEntities.get(NODE);
    var updatedNodeMembers = new TreeSet<Long>();
    nodeMembers.keySet().stream().filter(updatedNodes::contains).forEach(updatedNodeMembers::add);

    var updatedWays = updatedEntities.get(WAY);
    var updatedWayMembers = new TreeSet<Long>();
    wayMembers.keySet().stream().filter(updatedWays::contains).forEach(updatedWayMembers::add);

    var isMajorUpdate = versions.addAll(newVersions);
    var isMinorUpdate = !updatedNodeMembers.isEmpty() || !updatedWays.isEmpty();
    if (!isMinorUpdate && !isMajorUpdate) {
      return null; // no updates
    }

    var newNodeMembers = newMembers(newVersions, OSMRelation::getMembers, NODE, nodeMembers);
    store.backRefsMerge(NODE_RELATION, id, newNodeMembers);
    updatedNodeMembers.addAll(newNodeMembers);
    updateMembers(NODE,updatedNodeMembers, nodeMembers);

    var newWayMembers = newMembers(newVersions, OSMRelation::getMembers, WAY, wayMembers);
    store.backRefsMerge(WAY_RELATION, id, newWayMembers);
    updatedWayMembers.addAll(newWayMembers);
    updateMembers(WAY, updatedWayMembers, wayMembers);

    var newRelationMembers = newMembers(newVersions, OSMRelation::getMembers, RELATION, relationMembers);
    store.backRefsMerge(RELATION_RELATION, id, newRelationMembers);

    var osh = OSHRelationImpl.build(new ArrayList<>(versions), nodeMembers.values(), wayMembers.values());
    updateStore(id, data, backRef, osh);
    return osh;
  }

  @SuppressWarnings("unchecked")
  private <T extends OSHEntity> void updateMembers(OSMType type, Set<Long> membersToUpdate, Map<Long, T> members) {
    store.entities(type, membersToUpdate).values().stream()
        .map(member -> (T) member.getOSHEntity())
        .filter(Objects::nonNull)
        .forEach(way -> members.put(way.getId(), way));
  }

  private <T extends OSMEntity> Set<Long> newMembers(Collection<T> versions,Function<T, OSMMember[]> fnt, OSMType type,  Map<Long, ?> members) {
    var newMembers = new TreeSet<Long>();
    versions.stream()
        .map(fnt)
        .flatMap(Arrays::stream)
        .filter(member -> member.getType() == type)
        .map(OSMMember::getId)
        .filter(not(members::containsKey))
        .forEach(newMembers::add);
    return newMembers;
  }

  private void updateStore(long id, OSHData data, BackRef backRef, OSHEntityImpl osh) {
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

    var updatedData = new OSHData(osh.getType(), id, cellId.getLevelId(), osh.getData());
    store.entities(Set.of(updatedData));
  }

  private void forwardBackRefs(BackRef backRefs) {
    if (backRefs != null) {
      backRefs.ways().forEach(backRef -> minorUpdates.computeIfAbsent(WAY, x -> new HashSet<>()).add(backRef));
      backRefs.relations().forEach(backRef -> minorUpdates.computeIfAbsent(RELATION, x -> new HashSet<>()).add(backRef));
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
