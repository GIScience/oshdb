package org.heigit.ohsome.oshdb.rocksdb;

import static com.google.common.collect.Streams.zip;
import static java.util.Collections.emptySet;
import static java.util.Map.entry;
import static java.util.Optional.ofNullable;
import static org.heigit.ohsome.oshdb.rocksdb.RocksDBUtil.idToKey;
import static org.heigit.ohsome.oshdb.rocksdb.RocksDBUtil.idsToKeys;

import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.store.BackRef;
import org.heigit.ohsome.oshdb.store.BackRefType;
import org.rocksdb.Cache;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public class BackRefStore implements AutoCloseable {

  private final BackRefType type;
  private final Options dbOptions;
  private final RocksDB db;

  public BackRefStore(BackRefType type, Path path, Cache cache) throws IOException, RocksDBException {
    Files.createDirectories(path);
    this.type = type;

    this.dbOptions = RocksDBUtil.defaultOptions();
    dbOptions.setMergeOperator(new StringAppendOperator((char) 0));
    dbOptions.unorderedWrite();

    try {
      db = RocksDB.open(dbOptions, path.toString());
    } catch (RocksDBException e) {
      close();
      throw e;
    }
  }

  public Map<Long, Set<Long>> backRefs(Set<Long> ids) throws RocksDBException {
    var keys = idsToKeys(ids, ids.size());
    try (var ro = new ReadOptions()) {
      var backRefIfs = db.multiGetAsList(keys);
      var map = Maps.<Long, Set<Long>>newHashMapWithExpectedSize(ids.size());
      zip(ids.stream(), backRefIfs.stream(), (id, backRef) -> entry(id, keysToSet(backRef)))
          .forEach(entry -> map.put(entry.getKey(), entry.getValue()));
      return map;
    }
  }

  private Set<Long> keysToSet(byte[] backRefIds) {
    if (backRefIds == null){
      return emptySet();
    }
    var bb = ByteBuffer.wrap(backRefIds);
    var set = new TreeSet<Long>();
    set.add(bb.getLong());
    while (bb.hasRemaining()) {
      bb.get(); // delimiter;
      set.add(bb.getLong());
    }
    return set;
  }

  public void update(List<BackRef> backRefs) throws RocksDBException {
    throw new UnsupportedOperationException("not yet implemented");
  }

  @Override
  public void close() {
    ofNullable(db).ifPresent(RocksDB::close);
    dbOptions.close();
  }

  @Override
  public String toString() {
    return "BackRefStore " + type;
  }

  public void merge(long backRef, Set<Long> ids) throws RocksDBException {
    var backRefKey = idToKey(backRef);
    try ( var wo = new WriteOptions().setDisableWAL(true);
          var wb = new WriteBatch()) {
      for (var id : ids) {
        var key = idToKey(id);
        wb.merge(key, backRefKey);
      }
      db.write(wo, wb);
    }
  }
}
