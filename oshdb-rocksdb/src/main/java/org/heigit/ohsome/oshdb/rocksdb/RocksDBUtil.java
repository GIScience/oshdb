package org.heigit.ohsome.oshdb.rocksdb;

import static org.rocksdb.CompactionPriority.MinOverlappingRatio;
import static org.rocksdb.CompressionType.LZ4_COMPRESSION;
import static org.rocksdb.CompressionType.ZSTD_COMPRESSION;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.util.SizeUnit;

public class RocksDBUtil {

  private RocksDBUtil() {
    throw new IllegalStateException("Utility class");
  }

  public static void setCommonDBOption(DBOptions dbOptions) {
    dbOptions.setCreateIfMissing(true);
    dbOptions.setCreateMissingColumnFamilies(true);
    dbOptions.setMaxBackgroundJobs(6);
    dbOptions.setBytesPerSync(SizeUnit.MB);
  }

  public static ColumnFamilyOptions cfOptions(Cache cache) {
    return cfOptions(cache, x -> {
    });
  }

  public static ColumnFamilyOptions cfOptions(
      Cache cache, Consumer<BlockBasedTableConfig> blockTableConfig) {
    var tableConfig = new BlockBasedTableConfig()
        .setBlockCache(cache)
        .setBlockSize(16 * SizeUnit.KB)
        .setCacheIndexAndFilterBlocks(true)
        .setPinL0FilterAndIndexBlocksInCache(true)
        .setFormatVersion(5)
        .setOptimizeFiltersForMemory(true);
    blockTableConfig.accept(tableConfig);

    var cfOptions = new ColumnFamilyOptions();
    cfOptions.setCompressionType(LZ4_COMPRESSION);
    cfOptions.setBottommostCompressionType(ZSTD_COMPRESSION);
    cfOptions.setCompactionPriority(MinOverlappingRatio);
    cfOptions.setLevelCompactionDynamicLevelBytes(true);
    cfOptions.setTableFormatConfig(tableConfig);
    return cfOptions;
  }

  public static List<byte[]> idsToKeys(Iterable<Long> ids, int size) {
    var keys = new ArrayList<byte[]>(size);
    for (var id : ids) {
      keys.add(idToKey(id));
    }
    return keys;
  }

  public static byte[] idToKey(long id) {
    return ByteBuffer.allocate(Long.BYTES).putLong(id).array();
  }
}
