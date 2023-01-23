package org.heigit.ohsome.oshdb.rocksdb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.store.BackRefs;
import org.heigit.ohsome.oshdb.store.OSHDBData;
import org.heigit.ohsome.oshdb.store.OSHDBStore;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * directory layout: store ├── backrefs │   ├── node │   ├── relation │   └── way ├── entities │
 * ├── node │   ├── relation │   └── way └── grids ├── node ├── relation └── way
 */
public class RocksDBStore extends OSHDBStore {

  static {
    RocksDB.loadLibrary();
  }

  public static RocksDBStore open(Path path, long cacheSize) throws IOException, OSHDBException {
    Files.createDirectories(path);
    var entityStore = new EnumMap<OSMType, EntityStore>(OSMType.class);
    var backRefStore = new EnumMap<OSMType, BackRefStore>(OSMType.class);

    var cache = new LRUCache(cacheSize);
    try {
      for (var type : OSMType.values()) {
        var p = path.resolve("entities/" + type);
        Files.createDirectories(p);
        entityStore.put(type, new EntityStore(type, p, cache));
        p = path.resolve("backrefs/" + type);
        Files.createDirectories(p);
        backRefStore.put(type, new BackRefStore(type, p, cache));
      }
    } catch (RocksDBException e) {
      entityStore.values().forEach(EntityStore::close);
      throw new OSHDBException(e);
    }
    return new RocksDBStore(entityStore, backRefStore);
  }

  private final Map<OSMType, EntityStore> entityStore;
  private final Map<OSMType, BackRefStore> backRefStore;

  private RocksDBStore(Map<OSMType, EntityStore> entityStore, Map<OSMType, BackRefStore> backRefStore) {
    this.entityStore = entityStore;
    this.backRefStore = backRefStore;
  }

  @Override
  public Map<Long, OSHDBData> entities(OSMType type, List<Long> ids) {
    try {
      return entityStore.get(type).entities(ids);
    } catch (RocksDBException e) {
      throw new OSHDBException(e);
    }
  }

  @Override
  public void putEntities(OSMType type, List<OSHDBData> entities) {
    try {
      entityStore.get(type).put(entities);
    } catch (RocksDBException e) {
      throw new OSHDBException(e);
    }
  }

  @Override
  public List<OSHDBData> entitiesByGrid(OSMType type, long gridId) {
    try {
      return entityStore.get(type).byGrid(gridId);
    } catch (RocksDBException e) {
      throw new OSHDBException(e);
    }
  }

  @Override
  public Map<Long, BackRefs> backRefs(OSMType type, List<Long> ids) {
    try {
      return backRefStore.get(type).backRefs(ids);
    } catch (RocksDBException e) {
      throw new OSHDBException(e);
    }
  }

  @Override
  public void appendBackRefs(OSMType type, List<BackRefs> backRefs) {
    try {
      backRefStore.get(type).append(backRefs);
    } catch (RocksDBException e) {
      throw new OSHDBException(e);
    }
  }

  @Override
  public void close() {
    entityStore.values().forEach(EntityStore::close);
  }
}
