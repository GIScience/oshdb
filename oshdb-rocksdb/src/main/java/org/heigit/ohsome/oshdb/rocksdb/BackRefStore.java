package org.heigit.ohsome.oshdb.rocksdb;

import static java.util.Optional.ofNullable;
import static org.heigit.ohsome.oshdb.rocksdb.RocksDBUtil.cfOptions;
import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.store.BackRef;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.util.SizeUnit;

public class BackRefStore implements AutoCloseable {

  private final OSMType type;
  private final DBOptions dbOptions;
  private final List<ColumnFamilyHandle> cfHandles;
  private final RocksDB db;

  public BackRefStore(OSMType type, Path path, Cache cache) throws IOException, RocksDBException {
    Files.createDirectories(path);
    this.type = type;
    this.dbOptions = new DBOptions();
    dbOptions.setCreateIfMissing(true);
    dbOptions.setCreateMissingColumnFamilies(true);
    dbOptions.setMaxBackgroundJobs(6);
    dbOptions.setBytesPerSync(SizeUnit.MB);

    var cfDescriptors = List.of(
        new ColumnFamilyDescriptor(DEFAULT_COLUMN_FAMILY, cfOptions(cache)),
        new ColumnFamilyDescriptor((type + "_way").getBytes(),
            cfOptions(cache, tableConfig -> tableConfig.setFilterPolicy(new BloomFilter(10)))),
        new ColumnFamilyDescriptor((type + "_relation").getBytes(),
            cfOptions(cache, tableConfig -> tableConfig.setFilterPolicy(new BloomFilter(10)))));
    this.cfHandles = new ArrayList<>();
    try {
      db = RocksDB.open(dbOptions, path.toString(), cfDescriptors, cfHandles);
    } catch (RocksDBException e) {
      cfHandles.forEach(ColumnFamilyHandle::close);
      dbOptions.close();
      throw e;
    }
  }

  public Map<Long, BackRef> backRefs(Set<Long> ids) {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public void update(List<BackRef> backRefs) throws RocksDBException {
    throw new UnsupportedOperationException("not yet implemented");
  }

  @Override
  public void close() {
    cfHandles.forEach(ColumnFamilyHandle::close);
    ofNullable(db).ifPresent(RocksDB::close);
    dbOptions.close();
  }

  @Override
  public String toString() {
    return "BackRefStore " + type;
  }


}
