package org.heigit.ohsome.oshdb.rocksdb;

import static com.google.common.collect.Streams.zip;
import static java.util.Optional.ofNullable;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.heigit.ohsome.oshdb.rocksdb.RocksDBUtil.cfOptions;
import static org.heigit.ohsome.oshdb.rocksdb.RocksDBUtil.idToKey;
import static org.heigit.ohsome.oshdb.rocksdb.RocksDBUtil.idsToKeys;
import static org.heigit.ohsome.oshdb.rocksdb.RocksDBUtil.setCommonDBOption;
import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.store.OSHData;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public class EntityStore implements AutoCloseable {

  private static final byte[] EMPTY = new byte[0];
  private static final byte[] KEY_ZERO = idToKey(0);

  private final OSMType type;
  private final Map<byte[],ColumnFamilyOptions> cfOptions;
  private final DBOptions dbOptions;
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
      close();
      throw e;
    }
  }

  public Map<Long, OSHData> entities(Collection<Long> ids) throws RocksDBException {
    try (var opt = new ReadOptions()) {
      var cfsList = new ColumnFamilyHandle[ids.size()];
      Arrays.fill(cfsList, entityGridCFHandle());
      var keys = idsToKeys(ids, ids.size());
      var gridIds = db.multiGetAsList(opt, Arrays.asList(cfsList), keys);

      @SuppressWarnings("UnstableApiUsage")
      var gridEntityKeys = zip(gridIds.stream(), keys.stream(), this::gridEntityKey)
          .filter(key -> key.length != 0)
          .collect(Collectors.toList());

      var data = db.multiGetAsList(opt, gridEntityKeys);
      @SuppressWarnings("UnstableApiUsage")
      var entities = zip(gridEntityKeys.stream(), data.stream(), this::gridEntityToOSHData)
          .filter(Objects::nonNull)
          .collect(toMap(OSHData::getId, identity()));
      return entities;
    }
  }



  public void update(List<OSHData> entities) throws RocksDBException {
    var opt = new ReadOptions();
    var cfsList = new ColumnFamilyHandle[entities.size()];
    Arrays.fill(cfsList, entityGridCFHandle());
    var keys = idsToKeys(Iterables.transform(entities, OSHData::getId), entities.size());
    var gridKeys = db.multiGetAsList(opt, Arrays.asList(cfsList), keys);

    try (var wo = new WriteOptions().setDisableWAL(true);
         var wb = new WriteBatch()) {

      var idx = 0;
      for (var entity : entities) {
        var gridKey = idToKey(entity.getGridId());
        var key = keys.get(idx);
        var prevGridKey = gridKeys.get(idx);

        if (prevGridKey != null && !Arrays.equals(prevGridKey, gridKey)) {
          wb.delete(gridEntityDataCFHandle(), gridEntityKey(prevGridKey, key));
        }
        wb.put(entityGridCFHandle(), key, gridKey);
        wb.put(gridEntityDataCFHandle(), gridEntityKey(gridKey, key), entity.getData());
        idx++;
      }
      db.write(wo, wb);
    }
  }

  public List<OSHData> grid(long gridId) throws RocksDBException {
    var gridKey = idToKey(gridId);
    var gridEntityKey = gridEntityKey(gridKey, KEY_ZERO);
    var nextGridEntityKey = gridEntityKey(idToKey(gridId+1), KEY_ZERO);
    try (var opts = new ReadOptions().setIterateUpperBound(new Slice(nextGridEntityKey));
        var itr = db.newIterator(opts)) {
      var list = new ArrayList<OSHData>();
      itr.seek(gridEntityKey);
      for (; itr.isValid(); itr.next()) {
        var key = itr.key();
        var data = itr.value();
        var entityId = ByteBuffer.wrap(key, 8, 8).getLong();
        list.add(new OSHData(type, entityId, gridId, data));
      }
      itr.status();
      return list;
    }
  }

  private ColumnFamilyHandle gridEntityDataCFHandle() {
    return cfHandles.get(0);
  }

  private ColumnFamilyHandle entityGridCFHandle() {
    return cfHandles.get(1);
  }

  private byte[] gridEntityKey(byte[] gridId, byte[] entityId) {
    if (gridId == null) {
      return EMPTY;
    }
    return ByteBuffer.allocate(Long.BYTES * 2).put(gridId).put(entityId).array();
  }

  private OSHData gridEntityToOSHData(byte[] gridEntityKey, byte[] data) {
    if (data == null) {
      return null;
    }
    var bb = ByteBuffer.wrap(gridEntityKey);
    var gridId = bb.getLong();
    var entityId = bb.getLong();
    return new OSHData(type, entityId, gridId, data);
  }

  @Override
  public void close() {
    cfHandles.forEach(ColumnFamilyHandle::close);
    ofNullable(db).ifPresent(RocksDB::close);
    dbOptions.close();
    cfOptions.values().forEach(ColumnFamilyOptions::close);
  }

  @Override
  public String toString() {
    return "EntityStore " + type;
  }
}
