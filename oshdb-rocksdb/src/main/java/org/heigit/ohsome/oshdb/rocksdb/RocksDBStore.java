package org.heigit.ohsome.oshdb.rocksdb;

import static java.util.stream.Collectors.groupingBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.store.BackRef;
import org.heigit.ohsome.oshdb.store.OSHDBStore;
import org.heigit.ohsome.oshdb.store.OSHData;
import org.heigit.ohsome.oshdb.util.CellId;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBStore implements OSHDBStore {

  static {
    RocksDB.loadLibrary();
  }

  private final Cache cache;
  private final Map<OSMType, EntityStore> entityStore = new EnumMap<>(OSMType.class);
  private final Map<OSMType, BackRefStore> backRefStore = new EnumMap<>(OSMType.class);

  public RocksDBStore(Path path, long cacheSize) throws IOException, RocksDBException {
    Files.createDirectories(path);
    cache = new LRUCache(cacheSize);
    try {
      for (var type: OSMType.values()) {
        entityStore.put(type, new EntityStore(type, path.resolve("entities/" + type), cache));
        backRefStore.put(type, new BackRefStore(type, path.resolve("backrefs/" + type), cache));
      }
    } catch(RocksDBException e) {
      close();
      throw e;
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
    return backRefStore.get(type).backRefs(ids);
  }

  @Override
  public void backRefs(Set<BackRef> backRefs) {
    for (var entry : backRefs.stream().collect(groupingBy(BackRef::getType)).entrySet()){
      try {
        backRefStore.get(entry.getKey()).update(entry.getValue());
      } catch (RocksDBException e) {
        throw new OSHDBException(e);
      }
    }
  }

  @Override
  public void close() {
    backRefStore.values().forEach(BackRefStore::close);
    entityStore.values().forEach(EntityStore::close);
    cache.close();
  }
}
