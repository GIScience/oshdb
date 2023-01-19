package org.heigit.ohsome.oshdb.rocksdb;

import java.util.function.Consumer;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionPriority;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.util.SizeUnit;

public class RocksDBUtil {

  public static DBOptions dbOptions() {
    return new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true)
        .setMaxBackgroundJobs(6)
        .setBytesPerSync(SizeUnit.MB)
//        .setWriteBufferManager(wbManager);
//        .setDbWriteBufferSize(64 * SizeUnit.GB)
    ;
  }

  public static ColumnFamilyOptions cfOptions(Cache cache) {
    return cfOptions(cache, x -> {});
  }

  public static ColumnFamilyOptions cfOptions(
      Cache cache, Consumer<BlockBasedTableConfig> blockTableConfig){
    var options =  new ColumnFamilyOptions();
    options.setCompressionType(CompressionType.LZ4_COMPRESSION);
    options.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION);
    options.setCompactionPriority(CompactionPriority.MinOverlappingRatio);
    options.setLevelCompactionDynamicLevelBytes(true);

    var tableConfig = new BlockBasedTableConfig();
    tableConfig.setBlockCache(cache);
    tableConfig.setBlockSize(16 * SizeUnit.KB);
    tableConfig.setCacheIndexAndFilterBlocks(true);
    tableConfig.setPinL0FilterAndIndexBlocksInCache(true);
    tableConfig.setFormatVersion(5);
    blockTableConfig.accept(tableConfig);
    options.setTableFormatConfig(tableConfig);

    return options;
  }
}
