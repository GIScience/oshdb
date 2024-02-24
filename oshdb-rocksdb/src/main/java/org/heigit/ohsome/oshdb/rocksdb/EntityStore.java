package org.heigit.ohsome.oshdb.rocksdb;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Streams.zip;
import static java.nio.ByteBuffer.allocate;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Arrays.fill;
import static java.util.Collections.emptyMap;
import static java.util.Optional.ofNullable;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.heigit.ohsome.oshdb.rocksdb.RocksDBUtil.cfOptions;
import static org.heigit.ohsome.oshdb.rocksdb.RocksDBUtil.idToKey;
import static org.heigit.ohsome.oshdb.rocksdb.RocksDBUtil.idsToKeys;
import static org.heigit.ohsome.oshdb.rocksdb.RocksDBUtil.setCommonDBOption;
import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

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
import java.util.concurrent.locks.ReentrantLock;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityStore implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(EntityStore.class);

  private static final byte[] GRID_ENTITY_COLUMN_FAMILY = DEFAULT_COLUMN_FAMILY;
  private static final byte[] IDX_ENTITY_GRID_COLUMN_FAMILY = "idx_entity_grid".getBytes(UTF_8);
  private static final byte[] DIRTY_GRIDS_COLUMN_FAMILY = "dirty_grids".getBytes(UTF_8);

  private static final byte[] EMPTY = new byte[0];
  private static final byte[] KEY_ZERO = idToKey(0);

  private final OSMType type;
  private final Map<byte[], ColumnFamilyOptions> cfOptions;
  private final DBOptions dbOptions;
  private final List<ColumnFamilyHandle> cfHandles;
  private final RocksDB db;

  private final ReentrantLock lock = new ReentrantLock();

  public EntityStore(OSMType type, Path path, Cache cache) throws RocksDBException, IOException {
    Files.createDirectories(path);
    this.type = type;
    this.dbOptions = new DBOptions();
    setCommonDBOption(dbOptions);

    cfOptions = Map.of(
        GRID_ENTITY_COLUMN_FAMILY, cfOptions(cache),
        IDX_ENTITY_GRID_COLUMN_FAMILY, cfOptions(cache, tableConfig ->
            tableConfig.setFilterPolicy(new BloomFilter(10))),
        DIRTY_GRIDS_COLUMN_FAMILY, cfOptions(cache));

    var cfDescriptors = cfOptions.entrySet().stream()
        .map(option -> new ColumnFamilyDescriptor(option.getKey(), option.getValue()))
        .toList();
    this.cfHandles = new ArrayList<>();
    try {
      db = RocksDB.open(dbOptions, path.toString(), cfDescriptors, cfHandles);
    } catch (RocksDBException e) {
      close();
      throw e;
    }
  }

  public Map<Long, OSHData> entities(Collection<Long> ids) throws RocksDBException {
    var cfsList = new ColumnFamilyHandle[ids.size()];

    fill(cfsList, entityGridCFHandle());
    var keys = idsToKeys(ids, ids.size());
    var gridIds = db.multiGetAsList(asList(cfsList), keys);

    @SuppressWarnings("UnstableApiUsage")
    var gridEntityKeys = zip(gridIds.stream(), keys.stream(), this::gridEntityKey)
        .filter(key -> key.length != 0)
        .toList();

    if (gridEntityKeys.isEmpty()) {
      return emptyMap();
    }

    fill(cfsList, gridEntityDataCFHandle());
    var data = db.multiGetAsList(asList(cfsList), gridEntityKeys);

    @SuppressWarnings("UnstableApiUsage")
    var entities = zip(gridEntityKeys.stream(), data.stream(), this::gridEntityToOSHData)
        .filter(Objects::nonNull)
        .collect(toMap(OSHData::getId, identity()));
    return entities;
  }

  public void update(List<OSHData> entities) throws RocksDBException {
    lock.lock();
    try (
        var writeBatch = new WriteBatch();
        var writeOptions = new WriteOptions()) {

      var cfsList = new ColumnFamilyHandle[entities.size()];
      fill(cfsList, entityGridCFHandle());
      var keys = idsToKeys(transform(entities, OSHData::getId), entities.size());
      var gridKeys = db.multiGetAsList(asList(cfsList), keys);

      var idx = 0;
      for (var entity : entities) {
        var gridKey = idToKey(entity.getGridId());
        var key = keys.get(idx);
        var prevGridKey = gridKeys.get(idx);

        if (prevGridKey != null && !Arrays.equals(prevGridKey, gridKey)) {
          writeBatch.put(dirtyGridCFHandle(), prevGridKey, EMPTY);
          writeBatch.delete(gridEntityDataCFHandle(), gridEntityKey(prevGridKey, key));
        }

        writeBatch.put(dirtyGridCFHandle(), gridKey, EMPTY);
        writeBatch.put(entityGridCFHandle(), key, gridKey);
        var gridEntityKey = gridEntityKey(gridKey, key);
        writeBatch.put(gridEntityDataCFHandle(), gridEntityKey, entity.getData());
        idx++;
      }
      db.write(writeOptions, writeBatch);
    } finally {
      lock.lock();
    }
  }

  public List<OSHData> grid(long gridId) throws RocksDBException {
    var gridKey = idToKey(gridId);
    var gridEntityKey = gridEntityKey(gridKey, KEY_ZERO);
    var nextGridEntityKey = gridEntityKey(idToKey(gridId + 1), KEY_ZERO);
    try (var opts = new ReadOptions().setIterateUpperBound(new Slice(nextGridEntityKey));
        var itr = db.newIterator(gridEntityDataCFHandle(), opts)) {
      var list = new ArrayList<OSHData>();
      itr.seek(gridEntityKey);
      for (; itr.isValid(); itr.next()) {
        var key = itr.key();
        var data = itr.value();
        var entityId = ByteBuffer.wrap(key, 8, 8).getLong();
        var oshData = new OSHData(type, entityId, gridId, data);
        list.add(oshData);
      }
      itr.status();
      return list;
    }
  }

  public Collection<Long> dirtyGrids() throws RocksDBException {
    var cellIds = new ArrayList<Long>();
    try (var itr = db.newIterator(dirtyGridCFHandle())) {
      itr.seekToFirst();
      for (; itr.isValid(); itr.next()) {
        var gridId = ByteBuffer.wrap(itr.key()).getLong();
        cellIds.add(gridId);
      }
      itr.status();
      return cellIds;
    }
  }

  public void resetDirtyGrids() throws RocksDBException {
    log.debug("reset dirty grids {}", type);
    db.dropColumnFamily(dirtyGridCFHandle());
    var cfHandle = db.createColumnFamily(
        new ColumnFamilyDescriptor(DIRTY_GRIDS_COLUMN_FAMILY,
            cfOptions.get(DIRTY_GRIDS_COLUMN_FAMILY)));
    dirtyGridCFHandle(cfHandle);
  }

  private ColumnFamilyHandle gridEntityDataCFHandle() {
    return cfHandles.get(0);
  }

  private ColumnFamilyHandle entityGridCFHandle() {
    return cfHandles.get(1);
  }

  private ColumnFamilyHandle dirtyGridCFHandle() {
    return cfHandles.get(2);
  }

  private void dirtyGridCFHandle(ColumnFamilyHandle cfHandle) {
    cfHandles.set(2, cfHandle);
  }

  private byte[] gridEntityKey(byte[] gridId, byte[] entityId) {
    if (gridId == null) {
      return EMPTY;
    }
    return allocate(Long.BYTES * 2).put(gridId).put(entityId).array();
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
