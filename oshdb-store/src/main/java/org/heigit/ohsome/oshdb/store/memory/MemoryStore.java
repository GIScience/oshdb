package org.heigit.ohsome.oshdb.store.memory;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.heigit.ohsome.oshdb.store.BackRefType.NODE_RELATION;
import static org.heigit.ohsome.oshdb.store.BackRefType.NODE_WAY;
import static org.heigit.ohsome.oshdb.store.BackRefType.RELATION_RELATION;
import static org.heigit.ohsome.oshdb.store.BackRefType.WAY_RELATION;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.source.ReplicationInfo;
import org.heigit.ohsome.oshdb.store.BackRef;
import org.heigit.ohsome.oshdb.store.BackRefType;
import org.heigit.ohsome.oshdb.store.OSHDBStore;
import org.heigit.ohsome.oshdb.store.OSHData;
import org.heigit.ohsome.oshdb.util.CellId;
import org.heigit.ohsome.oshdb.util.tagtranslator.MemoryTagTranslator;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;

public class MemoryStore implements OSHDBStore {

  private final MemoryTagTranslator tagTranslator = new MemoryTagTranslator();
  private final Map<OSMType, Map<Long, OSHData>> entityStore = new EnumMap<>(OSMType.class);
  private final Map<BackRefType, Map<Long, Set<Long>>> backRefStore = new EnumMap<>(BackRefType.class);

  private ReplicationInfo state;

  public MemoryStore(ReplicationInfo state) {
    this.state = state;
  }

  @Override
  public ReplicationInfo state() {
    return state;
  }

  @Override
  public void state(ReplicationInfo state) {
    this.state = state;
  }

  @Override
  public TagTranslator getTagTranslator() {
    return tagTranslator;
  }

  @Override
  public Map<Long, OSHData> entities(OSMType type, Set<Long> ids) {
    var entities = entityStore.getOrDefault(type, emptyMap());
    return ids.stream().map(entities::get)
        .filter(Objects::nonNull)
        .collect(toMap(OSHData::getId, identity()));
  }

  @Override
  public void entities(Set<OSHData> entities) {
    entities.forEach(data -> entityStore
        .computeIfAbsent(data.getType(), x -> new TreeMap<>())
        .put(data.getId(), data));
  }

  @Override
  public List<OSHData> grid(OSMType type, CellId cellId) {
    var gridId = cellId.getLevelId();
    return entityStore.getOrDefault(type, emptyMap())
        .values()
        .stream()
        .filter(data -> data.getGridId() == gridId)
        .toList();
  }

  @Override
  public Map<Long, BackRef> backRefs(OSMType type, Set<Long> ids) {
    return ids.stream().map(id -> backRef(type, id))
        .collect(toMap(BackRef::getId, identity()));
  }

  @Override
  public BackRef backRef(OSMType type, long id) {
    var ways = Collections.<Long>emptySet();
    var relations = Collections.<Long>emptySet();
    if (Objects.requireNonNull(type) == OSMType.NODE) {
      ways = backRefStore.getOrDefault(NODE_WAY, emptyMap()).getOrDefault(id, emptySet());
      relations = backRefStore.getOrDefault(NODE_RELATION, emptyMap())
          .getOrDefault(id, emptySet());
    } else if (type == OSMType.WAY) {
      relations = backRefStore.getOrDefault(WAY_RELATION, emptyMap()).getOrDefault(id, emptySet());
    } else if (type == OSMType.RELATION) {
      relations = backRefStore.getOrDefault(RELATION_RELATION, emptyMap())
          .getOrDefault(id, emptySet());
    }
    return new BackRef(type, id, ways, relations);
  }

  @Override
  public void backRefsMerge(BackRefType type, long backRef, Set<Long> ids) {
    ids.forEach(id -> backRefStore.computeIfAbsent(type, x -> new TreeMap<>())
        .computeIfAbsent(id, x -> new TreeSet<>())
        .add(backRef));
  }

  @Override
  public void close() {
    // no/op
  }
}
