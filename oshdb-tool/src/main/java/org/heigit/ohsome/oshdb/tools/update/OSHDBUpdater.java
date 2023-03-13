package org.heigit.ohsome.oshdb.tools.update;


import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Comparator.comparingInt;
import static org.heigit.ohsome.oshdb.osm.OSMType.NODE;
import static org.heigit.ohsome.oshdb.osm.OSMType.RELATION;
import static org.heigit.ohsome.oshdb.osm.OSMType.WAY;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import org.heigit.ohsome.oshdb.OSHDB;
import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHWayImpl;
import org.heigit.ohsome.oshdb.index.XYGridTree;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osh.OSHNode;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

public class OSHDBUpdater {

  private static final Comparator<OSMEntity> VERSION_REVERSE_ORDER = comparingInt(
      OSMEntity::getVersion).reversed();

  private static final XYGridTree gridIndex = new XYGridTree(OSHDB.MAXZOOM);

  private OSHDBStore store;

  private final Map<OSMType, Set<Long>> minorUpdates = new EnumMap<>(OSMType.class);
  private final Map<OSMType, Set<Long>> updatedEntities = new EnumMap<>(OSMType.class);

  public void updateEntities(Flux<Tuple2<OSMType, Flux<OSMEntity>>> entities) {
    entities.concatMap(wnd -> Mono.fromCallable(() -> {
      return wnd.getT2().collectMultimap(OSMEntity::getId);

    }));

    entities.windowUntilChanged(OSMEntity::getType)
        .concatMap(wnd -> wnd.bufferUntilChanged(OSMEntity::getId).collectMap(this::id))
        .map(this::bla);
  }

  private Object bla(Map<Long, List<OSMEntity>> entities) {
    var type = type(entities);
    switch (type) {
      case NODE:
        return nodes(entities);
      case WAY:
        return ways(entities);
//      case RELATION: return relations(entities);
    }
    return null;
  }

  private OSMType type(Map<Long, List<OSMEntity>> entities) {
    return entities.values().stream()
        .map(versions -> versions.get(0).getType())
        .findAny().orElseThrow(NoSuchElementException::new);
  }

  private long id(List<OSMEntity> versions) {
    return versions.get(0).getId();
  }

  private Object nodes(Map<Long, List<OSMEntity>> entities) {
    var dataMap = store.entities(NODE, entities.keySet());
    var backRefMap = store.backRefs(NODE, entities.keySet());

    entities.entrySet().stream()
        .map(entry -> node(entry.getKey(), entry.getValue(),
            dataMap.get(entry.getKey()),
            backRefMap.get(entry.getKey())));
    return null;
  }

  private Object ways(Map<Long, List<OSMEntity>> entities) {
    minorUpdates.get(WAY).forEach(id -> entities.put(id, emptyList()));
    var dataMap = store.entities(WAY, entities.keySet());
    var backRefMap = store.backRefs(WAY, entities.keySet());

    return null;
  }

  private <T extends OSMEntity> TreeSet<T> mergePrevious(OSHData previous, Class<T> clazz) {
    var merged = new TreeSet<T>(VERSION_REVERSE_ORDER);
    if (previous != null) {
      var osh = previous.getOSHEntity();
      osh.getVersions().forEach(version -> merged.add((T) version));
    }
    return merged;
  }

  private OSHData node(long id, List<OSMEntity> versions, OSHData previous, BackRef backRefs) {
    var merged = mergePrevious(previous, OSMNode.class);
    var major = isMajor(versions, merged);

    if (!major) {
      // shotcut nothing changed
      return previous;
    }

    forwardBackRefs(backRefs);

    var osh = OSHNodeImpl.build(new ArrayList<>(merged));
    var gridId = gridIndex(osh);
    updatedEntities.computeIfAbsent(NODE, x -> new HashMap<>()).put(id, osh);
    return new OSHData(NODE, id, gridId, osh.getData());
  }

  private void forwardBackRefs(BackRef backRefs) {
    if (backRefs != null) {
      backRefs.ways().forEach(backRef -> minorUpdates.get(WAY).put(backRef, emptyList()));
      backRefs.relations().forEach(backRef -> minorUpdates.get(RELATION).put(backRef, emptyList()));
    }
  }

  private <T extends OSHEntity> boolean updatedMembers(OSMType type,
      Map<OSMType, Map<Long, OSHEntity>> members, List<T> entities) {
    var minor = false;
    for (var member : entities) {
      var updated = updatedEntities.get(type).get(member.getId());
      if (updated != null) {
        minor = true;
        member = (T) updated;
      }
      members.computeIfAbsent(type, x -> new TreeMap<>()).put(member.getId(), member);
    }
    return minor;
  }

  private OSHData way(long id, List<OSMEntity> newVersions, OSHData previous, BackRef backRefs) {
    var merged = mergePrevious(previous, OSMWay.class);
    var members = new TreeMap<Long, OSHNode>();
    if (previous != null) {
      OSHWay osh = previous.getOSHEntity();
      osh.getNodes().forEach(node -> members.put(node.getId(), node));
    }
    var updatedNodes = updatedEntities.get(NODE);
    var updatedMembers = new TreeSet<Long>();
    members.keySet().stream().filter(updatedNodes::containsKey).forEach(updatedMembers::add);

    var minor = !updatedMembers.isEmpty();
    var major = isMajor(newVersions, merged);

    if (!minor && !major) {
      // short cut;
      return previous;
    }


    var newMembers = new TreeSet<Long>();
    newVersions.stream()
        .map(version -> (OSMWay) version)
        .flatMap(version -> stream(version.getMembers()))
        .map(OSMMember::getId)
        .filter(members::containsKey)
        .forEach(newMembers::add);

    updatedMembers.addAll(newMembers);
    store.entities(NODE, updatedMembers).values().stream()
        .map(data -> (OSHNode) data.getOSHEntity())
        .filter(Objects::nonNull)
        .forEach(node -> members.put(node.getId(), node));


    store.appendBackRef(NODE, WAY, newMembers, id);

    //TODO update backrefs newMembers!
    var osh = OSHWayImpl.build(new ArrayList<>(merged), members.values());
    var gridId = gridIndex(osh);
    if (previous != null) {
      var previousGridId = previous.getGridId();

    }
    updatedEntities.computeIfAbsent(WAY, x -> new HashSet<>()).add(id);
    forwardBackRefs(backRefs);
    return new OSHData(WAY, id, gridId, osh.getData());
  }


  private long gridIndex(OSHEntity osh) {
    var bbox = osh.getBoundable().getBoundingBox();
    if (!bbox.isValid()) {
      return -1;
    }
    var cellId = gridIndex.getInsertId(bbox);
    return cellId.getLevelId();
  }

  private static <T> boolean isMajor(List<OSMEntity> versions, TreeSet<T> merged) {
    return versions.stream().map(version -> (T) version).filter(merged::add).count() > 0;
  }


  private <T extends OSMEntity> boolean majorUpdate(List<OSMEntity> versions, Set<T> merged,
      Function<T, OSMMember[]> getMembers,
      Map<OSMType, Map<Long, OSHEntity>> members, Map<OSMType, Set<Long>> newMembers) {

    return versions.stream()
        .map(version -> (T) version)
        .filter(merged::add)
        .map(version -> collectNewMembers(getMembers.apply(version), members, newMembers))
        .count() > 0;
  }

  private long collectNewMembers(OSMMember[] members, Map<OSMType, Map<Long, OSHEntity>> knowMembers,
      Map<OSMType, Set<Long>> newMembers) {
    return stream(members)
        .filter(member -> !knowMembers.getOrDefault(member.getType(), emptyMap()).containsKey(member.getId()))
        .filter(member -> newMembers.computeIfAbsent(member.getType(), x -> new TreeSet<>()).add(member.getId()))
        .count();
  }

  private <T extends OSMEntity> boolean minorUpdate(OSHData data, Set<T> versions,
      Map<OSMType, Map<Long, OSHEntity>> members) {
    if (data == null) {
      return false;
    }
    var osh = data.getOSHEntity();
    osh.getVersions().forEach(version -> versions.add((T) version));
    var minor = updatedMembers(NODE, members, osh.getNodes());
    minor |= updatedMembers(WAY, members, osh.getWays());
    return minor;
  }

  private Object relations(Map<Long, List<OSMEntity>> entities) {
    entities.putAll(minorUpdates.getOrDefault(RELATION, emptyMap()));
    var bla = store.entities(RELATION, entities.keySet());
    var blu = store.backRefs(RELATION, entities.keySet());
    return null;
  }

  private Object relation(long id, List<OSMEntity> versions, OSHData previous, BackRef backRef) {
    var merged = new TreeSet<OSMRelation>(VERSION_REVERSE_ORDER);
    var members = new EnumMap<OSMType, Map<Long, OSHEntity>>(OSMType.class);
    var minor = minorUpdate(previous, merged, members);

    var newMembers = new EnumMap<OSMType, Set<Long>>(OSMType.class);
    var major = majorUpdate(versions, merged, OSMRelation::getMembers, members, newMembers);
    for (var entry : newMembers.entrySet()) {
      store.entities(entry.getKey(), entry.getValue())
          .forEach((memId, data) -> members.computeIfAbsent(entry.getKey(), x -> new TreeMap<>())
              .put(memId, data.getOSHEntity()));
    }

    //TODO update backrefs newMembers!

    var osh = OSHRelationImpl.build(new ArrayList<>(merged),
        (Collection<OSHNode>) (Collection<?>) members.get(NODE).values(),
        (Collection<OSHWay>) (Collection<?>) members.get(WAY).values());

    //TODO get new cellId
    var gridId = -1;

    return new OSHData(RELATION, id, gridId, osh.getData());
  }



}
