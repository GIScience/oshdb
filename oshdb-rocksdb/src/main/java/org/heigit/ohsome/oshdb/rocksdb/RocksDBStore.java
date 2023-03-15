package org.heigit.ohsome.oshdb.rocksdb;

import static java.util.Collections.emptyMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.source.ReplicationInfo;
import org.heigit.ohsome.oshdb.store.BackRef;
import org.heigit.ohsome.oshdb.store.BackRefType;
import org.heigit.ohsome.oshdb.store.OSHDBStore;
import org.heigit.ohsome.oshdb.store.OSHData;
import org.heigit.ohsome.oshdb.util.CellId;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBStore implements OSHDBStore {

  static {
    RocksDB.loadLibrary();
  }

  private final TagTranslator tagTranslator;
  private final Path path;
  private final Cache cache;
  private final Map<OSMType, EntityStore> entityStore = new EnumMap<>(OSMType.class);
  private final Map<BackRefType, BackRefStore> backRefStore = new EnumMap<>(BackRefType.class);

  public RocksDBStore(TagTranslator tagTranslator, Path path, long cacheSize) throws IOException, RocksDBException {
    this.tagTranslator = tagTranslator;
    this.path = path;
    Files.createDirectories(path);
    cache = new LRUCache(cacheSize);
    try {
      for (var type: OSMType.values()) {
        entityStore.put(type, new EntityStore(type, path.resolve("entities/" + type), cache));
      }
      for (var type: BackRefType.values()) {
        backRefStore.put(type, new BackRefStore(type, path.resolve("backrefs/" + type), cache));
      }
    } catch(RocksDBException e) {
      close();
      throw e;
    }
  }

  @Override
  public TagTranslator getTagTranslator() {
    return tagTranslator;
  }

  @Override
  public void state(ReplicationInfo state) {
    var props = new Properties();
    props.put("baseUrl", state.getBaseUrl());
    props.put("sequenceNumber", state.getSequenceNumber());
    props.put("timestamp", state.getTimestamp());
    try (var out = Files.newOutputStream(path.resolve("state.txt"))) {
      props.store(out, "rocksdb store state");
    } catch (IOException e) {
      throw new OSHDBException(e);
    }
  }

  @Override
  public ReplicationInfo state() {
    var props = new Properties();
    try (var in = Files.newInputStream(path.resolve("state.txt"))) {
      props.load(in);
      return new ReplicationInfo() {
        @Override
        public String getBaseUrl() {
          return props.getProperty("baseUrl", "");
        }

        @Override
        public ZonedDateTime getTimestamp() {
          return ZonedDateTime.parse(props.getProperty("timestamp"));
        }

        @Override
        public int getSequenceNumber() {
          return Integer.parseInt(props.getProperty("sequenceNumber"));
        }
      };
    } catch (IOException e) {
      throw new OSHDBException(e);
    }
  }

  @Override
  public Map<Long, OSHData> entities(OSMType type, Set<Long> ids) {
    try {
      return entityStore.get(type).entities(ids);
    } catch (RocksDBException e) {
      throw new OSHDBException(e);
    }
  }

  @Override
  public void entities(Set<OSHData> entities) {
    for (var entry : entities.stream().collect(groupingBy(OSHData::getType)).entrySet()){
      try {
        entityStore.get(entry.getKey()).update(entry.getValue());
      } catch (RocksDBException e) {
        throw new OSHDBException(e);
      }
    }
  }

  @Override
  public List<OSHData> grid(OSMType type, CellId gridId) {
    try {
      return entityStore.get(type).grid(gridId.getLevelId());
    } catch (RocksDBException e) {
      throw new OSHDBException(e);
    }
  }

  @Override
  public Map<Long, BackRef> backRefs(OSMType type, Set<Long> ids) {
    Map<Long, Set<Long>> ways;
    Map<Long, Set<Long>> relations;
    try {
      if (type == OSMType.NODE) {
        ways = backRefStore.get(BackRefType.NODE_WAY).backRefs(ids);
        relations = backRefStore.get(BackRefType.NODE_RELATION).backRefs(ids);
      } else if (type == OSMType.WAY) {
        ways = emptyMap();
        relations = backRefStore.get(BackRefType.WAY_RELATION).backRefs(ids);
      } else if (type == OSMType.RELATION) {
        ways = emptyMap();
        relations = backRefStore.get(BackRefType.RELATION_RELATION).backRefs(ids);
      } else {
        throw new IllegalStateException();
      }

      return ids.stream()
              .map(id -> new BackRef(type, id, ways.get(id), relations.get(id)))
              .collect(Collectors.toMap(BackRef::getId, identity()));
    } catch (RocksDBException e) {
      throw new OSHDBException();
    }
  }

  @Override
  public void backRefsMerge(BackRefType type, long backRef, Set<Long> ids) {
    try {
      backRefStore.get(type).merge(backRef, ids);
    } catch (RocksDBException e) {
      throw new OSHDBException(e);
    }
  }

  @Override
  public void close() {
    backRefStore.values().forEach(BackRefStore::close);
    entityStore.values().forEach(EntityStore::close);
    cache.close();
  }
}
