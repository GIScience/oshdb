package org.heigit.ohsome.oshdb.rocksdb;

import static org.heigit.ohsome.oshdb.rocksdb.RocksDBUtil.cfOptions;
import static org.heigit.ohsome.oshdb.rocksdb.RocksDBUtil.idToBytes;
import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.Closeable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.store.BackRefs;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayOutputWrapper;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayWrapper;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.StringAppendOperator;

public class BackRefStore implements Closeable {
  private static final int WAY = 1;
  private static final int RELATION = 2;

  private final OSMType type;
  private final Path path;
  private final DBOptions dbOptions;
  private final RocksDB db;
  private final List<ColumnFamilyHandle> cfHandles;

  public BackRefStore(OSMType type, Path path, Cache cache) throws RocksDBException {
    this.type = type;
    this.path = path;
    dbOptions = RocksDBUtil.dbOptions();
    cfHandles = new ArrayList<>();
    try {
      var cfDescriptors = List.of(
          new ColumnFamilyDescriptor(DEFAULT_COLUMN_FAMILY),
          new ColumnFamilyDescriptor((type + "_way").getBytes(),
              cfOptions(cache, tableConfig ->
                  tableConfig.setFilterPolicy(new BloomFilter(10))
                      .setOptimizeFiltersForMemory(true)
              ).setMergeOperator(new StringAppendOperator((char)0))
          ),
          new ColumnFamilyDescriptor((type + "_relation").getBytes(),
              cfOptions(cache, tableConfig ->
                  tableConfig.setFilterPolicy(new BloomFilter(10))
                      .setOptimizeFiltersForMemory(true)
              ).setMergeOperator(new StringAppendOperator((char)0))
          ));
      db = RocksDB.open(dbOptions, path.toString(), cfDescriptors, cfHandles);
    } catch (RocksDBException e) {
      dbOptions.close();
      throw e;
    }
  }


  Map<Long, BackRefs> backRefs(List<Long> ids) throws RocksDBException {
    var opt = new ReadOptions();
    var keys = RocksDBUtil.idsToKeys(ids, ids.size());
    var cfsList = new ColumnFamilyHandle[ids.size()];

    Arrays.fill(cfsList, cfHandles.get(RELATION));
    var relations = db.multiGetAsList(opt, Arrays.asList(cfsList), keys);

    if (type == OSMType.NODE) {
      Arrays.fill(cfsList, cfHandles.get(WAY));
      var ways = db.multiGetAsList(opt, Arrays.asList(cfsList), keys);

      return toBackRefs(ids, i -> new BackRefs(type, ids.get(i), toSet(ways.get(i)), toSet(relations.get(i))));
    }
    return toBackRefs(ids, i -> new BackRefs(type, ids.get(i), toSet(relations.get(i))));
  }

  void append(List<BackRefs> backRefs) throws RocksDBException {
    var output = new ByteArrayOutputWrapper();
    for (var backRef : backRefs) {
      var key = idToBytes(backRef.getId());
      append(cfHandles.get(WAY), key, backRef.ways(), output);
      append(cfHandles.get(RELATION), key, backRef.relations(), output);
    }
  }

  private void append(ColumnFamilyHandle cfh, byte[] key, Set<Long> refs,
      ByteArrayOutputWrapper output) throws RocksDBException {
    if (refs.isEmpty()) { return; }
    toValue(refs, output);
    db.merge(cfh, key, 0, key.length, output.array(), 0, output.length());
  }


  private void toValue(Set<Long> ways, ByteArrayOutputWrapper output) {
    output.reset();
    var sorted = new ArrayList<>(ways);
    Collections.sort(sorted);
    var last = 0L;
    for (var id : sorted) {
      output.writeU64(id - last);
      last = id;
    }
  }

  private Map<Long, BackRefs> toBackRefs(List<Long> ids, IntFunction<BackRefs> backRefs) {
    var result = Maps.<Long, BackRefs>newHashMapWithExpectedSize(ids.size());
    for (var i = 0; i < ids.size(); i++) {
      result.put(ids.get(i), backRefs.apply(i));
    }
    return result;
  }

  private Set<Long> toSet(byte[] bytes) {
    if (bytes == null) {
      return Collections.emptySet();
    }
    var input = ByteArrayWrapper.newInstance(bytes);
    var id = 0L;
    var result = new ArrayList<Long>();
    while (input.hasLeft() > 0) {
      var v = input.readU64();
      if (v == 0) {
        id = 0L;
        continue;
      }
      id += v;
      result.add(id);
    }
    return Sets.newHashSet(result);
  }

  public void close() {
    cfHandles.forEach(ColumnFamilyHandle::close);
    db.close();
    dbOptions.close();
  }
}
