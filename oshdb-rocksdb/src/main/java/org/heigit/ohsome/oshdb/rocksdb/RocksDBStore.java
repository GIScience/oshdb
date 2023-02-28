package org.heigit.ohsome.oshdb.rocksdb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.Map;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBStore implements AutoCloseable {

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
  public void close() {
    backRefStore.values().forEach(BackRefStore::close);
    entityStore.values().forEach(EntityStore::close);
    cache.close();
  }
}
