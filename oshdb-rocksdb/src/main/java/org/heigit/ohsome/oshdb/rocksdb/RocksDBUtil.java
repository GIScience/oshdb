package org.heigit.ohsome.oshdb.rocksdb;

import static org.rocksdb.CompactionPriority.MinOverlappingRatio;
import static org.rocksdb.CompressionType.LZ4_COMPRESSION;
import static org.rocksdb.CompressionType.ZSTD_COMPRESSION;
import static org.rocksdb.util.SizeUnit.KB;
import static org.rocksdb.util.SizeUnit.MB;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ColumnFamilyOptionsInterface;
import org.rocksdb.DBOptions;
import org.rocksdb.DBOptionsInterface;
import org.rocksdb.MutableColumnFamilyOptionsInterface;
import org.rocksdb.MutableDBOptionsInterface;
import org.rocksdb.Options;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;

public class RocksDBUtil {

  private RocksDBUtil() {
    throw new IllegalStateException("Utility class");
  }

  public static Options defaultOptions() {
    var options = new Options();
    defaultOptions(options);
    return options;
  }

  public static BlockBasedTableConfig defaultOptions(Options options) {
    defaultDBOptions(options);
    defaultMDBOptions(options);

    defaultCFOptions(options);
    defaultMCFOptions(options);

    final var tableOptions = new BlockBasedTableConfig();
    options.setTableFormatConfig(tableOptions);
    defaultTableConfig(tableOptions);
    return tableOptions;
  }

  public static void defaultDBOptions(DBOptionsInterface<?> options) {
    options.setCreateIfMissing(true);
    options.setCreateMissingColumnFamilies(true);
  }

  public static void defaultMDBOptions(MutableDBOptionsInterface<?> options) {
    options.setMaxBackgroundJobs(6);
    options.setBytesPerSync(1L * MB);
  }

  public static void defaultCFOptions(ColumnFamilyOptionsInterface<?> options) {
    options.setBottommostCompressionType(ZSTD_COMPRESSION);
    // general options
    options.setLevelCompactionDynamicLevelBytes(true);
    options.setCompactionPriority(MinOverlappingRatio);
  }

  public static void defaultMCFOptions(MutableColumnFamilyOptionsInterface<?> options) {
    options.setCompressionType(LZ4_COMPRESSION);
  }

  public static void defaultTableConfig(BlockBasedTableConfig tableOptions) {
    tableOptions.setBlockSize(16 * KB);
    tableOptions.setCacheIndexAndFilterBlocks(true);
    tableOptions.setPinL0FilterAndIndexBlocksInCache(true);
    tableOptions.setFormatVersion(5);
    tableOptions.setIndexBlockRestartInterval(16);
    tableOptions.setOptimizeFiltersForMemory(true);
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

  public static WriteOptions disableWAL() {
    WriteOptions writeOptions = new WriteOptions();
    writeOptions.setDisableWAL(true);
    return writeOptions;
  }
}
