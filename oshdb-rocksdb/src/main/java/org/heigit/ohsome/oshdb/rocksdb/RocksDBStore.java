package org.heigit.ohsome.oshdb.rocksdb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.store.BackRefs;
import org.heigit.ohsome.oshdb.store.OSHDBData;
import org.heigit.ohsome.oshdb.store.OSHDBStore;
import org.heigit.ohsome.oshdb.util.CellId;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * directory layout:
 * store
 * ├── backrefs
 * │   ├── node
 * │   ├── relation
 * │   └── way
 * ├── entities
 * │   ├── node
 * │   ├── relation
 * │   └── way
 * └── grids
 *     ├── node
 *     ├── relation
 *     └── way
 */
public class RocksDBStore extends OSHDBStore {

  static {
    RocksDB.loadLibrary();
  }

  public static RocksDBStore open(Path path, long cacheSize) throws IOException, OSHDBException {
    Files.createDirectories(path);
    var entityStore = new EnumMap<OSMType, EntityStore>(OSMType.class);
    var cache = new LRUCache(cacheSize);
    try {
      for (var type : OSMType.values()) {
        var p = path.resolve("entities/" + type);
        Files.createDirectories(p);
        entityStore.put(type, new EntityStore(type, p, cache));
      }
    } catch(RocksDBException e) {
      entityStore.values().forEach(EntityStore::close);
      throw new OSHDBException(e);
    }
    return new RocksDBStore(entityStore);
  }

  private final Map<OSMType, EntityStore> entityStore;

  private RocksDBStore(Map<OSMType, EntityStore> entityStore) {
    this.entityStore = entityStore;
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
  public void entities(List<OSHDBData> entities) {
    entities.stream().collect(Collectors.groupingBy(OSHDBData::getType))
        .forEach((type, list) -> entityStore.get(type).put(list));
  }

  @Override
  public Map<Long, List<OSHDBData>> entitiesByGrid(OSMType type, Collection<Long> gridIds) {
    var store = entityStore.get(type);
    return store.byGrid(gridIds);
  }

  @Override
  public Map<CellId, List<OSHDBData>> grids(OSMType type, Collection<CellId> cellIds) {
    return null;
  }

  @Override
  public Map<Long, BackRefs> backRefs(OSMType type, Collection<Long> ids) {
    return null;
  }

  @Override
  public void close() {
    entityStore.values().forEach(EntityStore::close);
  }
}
