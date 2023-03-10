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
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHWayImpl;
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

public class OSHDBUpdater {
  private static final Comparator<OSMEntity> VERSION_REVERSE_ORDER = comparingInt(OSMEntity::getVersion).reversed();


  private OSHDBStore store;

  private final Map<OSMType, Map<Long, List<OSMEntity>>> minorUpdates = new EnumMap<>(OSMType.class);
  private final Map<OSMType, Map<Long, OSHEntity>> updatedEntities = new EnumMap<>(OSMType.class);

  public void updateEntities(Flux<OSMEntity> entities) {
    entities.windowUntilChanged(OSMEntity::getType)
        .concatMap(wnd -> wnd.bufferUntilChanged(OSMEntity::getId).collectMap(this::id))
        .map(this::bla);
  }

  private Object bla(Map<Long, List<OSMEntity>> entities){
    var type = type(entities);
    switch (type) {
      case NODE: return nodes(entities);
      case WAY: return ways(entities);
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
    entities.putAll(minorUpdates.getOrDefault(WAY, emptyMap()));
    var dataMap = store.entities(WAY, entities.keySet());
    var backRefMap = store.backRefs(WAY, entities.keySet());


    return null;
  }

  private Object node(long id, List<OSMEntity> versions, OSHData previous, BackRef backRefs) {
    var merged = new TreeSet<OSMNode>(VERSION_REVERSE_ORDER);
    if (previous != null) {
      OSHNode osh = previous.getOSHEntity();
      osh.getVersions().forEach(merged::add);
    }
    var major = versions.stream().map(version -> (OSMNode) version)
        .filter(merged::add)
        .count() > 0;

    if (!major) {
      return previous;
    }
    if (backRefs != null) {
      backRefs.ways().forEach(backRef -> minorUpdates.get(WAY).put(backRef, emptyList()));
      backRefs.relations().forEach(backRef -> minorUpdates.get(RELATION).put(backRef, emptyList()));
    }

    var osh = OSHNodeImpl.build(new ArrayList<>(merged));
    //TODO get new cellId
    var gridId = -1;
    updatedEntities.computeIfAbsent(NODE, x -> new HashMap<>()).put(id, osh);
    return new OSHData(NODE, id, gridId, osh.getData());
  }



  private OSHData way(long id, List<OSMEntity> versions, OSHData previous, BackRef backRefs) {
    var merged = new TreeSet<OSMWay>(VERSION_REVERSE_ORDER);
    var members = new EnumMap<OSMType, Map<Long, OSHEntity>>(OSMType.class);
    var minor = minorUpdate(previous, merged, members);

    var newMembers = new EnumMap<OSMType, Set<Long>>(OSMType.class);
    var major = majorUpdate(versions, merged, OSMWay::getMembers, members, newMembers);

    if (!minor && !major) {
      return previous;
    }

    if (backRefs != null) {
      backRefs.relations().forEach(backRef -> minorUpdates.get(RELATION).put(backRef, emptyList()));
    }

    for (var entry : newMembers.entrySet()) {
      store.entities(entry.getKey(), entry.getValue())
          .forEach((memId, data) -> members.computeIfAbsent(entry.getKey(), x -> new TreeMap<>())
              .put(memId, data.getOSHEntity()));
    }

    //TODO update backrefs newMembers!
    var osh = OSHWayImpl.build(new ArrayList<>(merged),
        (Collection<OSHNode>)(Collection<?>) members.get(NODE).values());

    //TODO get new cellId
    var gridId = -1;
    updatedEntities.computeIfAbsent(WAY, x -> new HashMap<>()).put(id, osh);
    return new OSHData(WAY, id, gridId, osh.getData());
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

  private long collectNewMembers(OSMMember[] bla, Map<OSMType, Map<Long, OSHEntity>> members, Map<OSMType, Set<Long>> newMembers) {
    return stream(bla)
        .filter(member -> !members.getOrDefault(member.getType(), emptyMap()).containsKey(member.getId()))
        .map(member -> newMembers.computeIfAbsent(member.getType(), x -> new TreeSet<>()).add(member.getId()))
        .count();
  }

  private <T extends OSMEntity> boolean minorUpdate(OSHData data, Set<T> versions, Map<OSMType, Map<Long, OSHEntity>> members) {
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
        (Collection<OSHNode>)(Collection<?>) members.get(NODE).values(),
        (Collection<OSHWay>)(Collection<?>) members.get(WAY).values());

    //TODO get new cellId
    var gridId = -1;

    return new OSHData(RELATION, id, gridId, osh.getData());
  }

  private <T extends OSHEntity> boolean updatedMembers(OSMType type, Map<OSMType, Map<Long, OSHEntity>> members, List<T> entities) {
    var minor = false;
    for(var member : entities) {
      var updated = updatedEntities.get(type).get(member.getId());
      if (updated != null) {
        minor = true;
        member = (T) updated;
      }
      members.computeIfAbsent(type, x -> new TreeMap<>()).put(member.getId(), member);
    }
    return minor;
  }

}
