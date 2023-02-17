package org.heigit.ohsome.oshdb.rocksdb;

import org.rocksdb.DBOptions;
import org.rocksdb.util.SizeUnit;

public abstract class RocksStore {

  protected DBOptions dbOptions() {
    return new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true)
        .setMaxBackgroundJobs(6)
        .setBytesPerSync(SizeUnit.MB);
  }
}
