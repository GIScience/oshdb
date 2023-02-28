package org.heigit.ohsome.oshdb.rocksdb;

import static org.heigit.ohsome.oshdb.rocksdb.RocksDBUtil.cfOptions;
import static org.heigit.ohsome.oshdb.rocksdb.RocksDBUtil.setCommonDBOption;
import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class EntityStore implements AutoCloseable {

  private final OSMType type;
  private final DBOptions dbOptions;
  private final Map<byte[],ColumnFamilyOptions> cfOptions;
  private final List<ColumnFamilyHandle> cfHandles;
  private final RocksDB db;

  public EntityStore(OSMType type, Path path, Cache cache) throws RocksDBException, IOException {
    Files.createDirectories(path);
    this.type = type;
    this.dbOptions = new DBOptions();
    setCommonDBOption(dbOptions);

    cfOptions = Map.of(
        DEFAULT_COLUMN_FAMILY, cfOptions(cache),
        "idx_entity_grid".getBytes(), cfOptions(cache, tableConfig ->
            tableConfig.setFilterPolicy(new BloomFilter(10))));

    var cfDescriptors = cfOptions.entrySet().stream()
        .map(option -> new ColumnFamilyDescriptor(option.getKey(), option.getValue()))
        .collect(Collectors.toList());
    this.cfHandles = new ArrayList<>();
    try {
      db = RocksDB.open(dbOptions, path.toString(), cfDescriptors, cfHandles);
    } catch (RocksDBException e) {
      cfHandles.forEach(ColumnFamilyHandle::close);
      cfOptions.values().forEach(ColumnFamilyOptions::close);
      dbOptions.close();
      throw e;
    }
  }

  @Override
  public void close() {
    cfHandles.forEach(ColumnFamilyHandle::close);
    db.close();
    dbOptions.close();
    cfOptions.values().forEach(ColumnFamilyOptions::close);
  }

  @Override
  public String toString() {
    return "EntityStore " + type;
  }
}
