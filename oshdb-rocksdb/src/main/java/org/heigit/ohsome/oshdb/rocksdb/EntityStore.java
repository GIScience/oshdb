package org.heigit.ohsome.oshdb.rocksdb;

import static org.heigit.ohsome.oshdb.rocksdb.RocksDBUtil.cfOptions;
import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

import com.google.common.collect.Maps;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.store.OSHDBData;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

class EntityStore {

  private final OSMType type;
  private final Path path;
  private final DBOptions dbOptions;
  private final RocksDB db;
  private final List<ColumnFamilyHandle> cfHandles;

  EntityStore(OSMType type, Path path, Cache cache) throws RocksDBException {
    this.type = type;
    this.path = path;
    dbOptions = RocksDBUtil.dbOptions();
    cfHandles = new ArrayList<>();
    try {
      var cfDescriptors = List.of(
          new ColumnFamilyDescriptor(DEFAULT_COLUMN_FAMILY, cfOptions(cache)
          ),
          new ColumnFamilyDescriptor("idx_entity_grid".getBytes(),
              cfOptions(cache, tableConfig ->
                  tableConfig.setFilterPolicy(new BloomFilter(10))
                      .setOptimizeFiltersForMemory(true)
              )));
      db = RocksDB.open(dbOptions, path.toString(), cfDescriptors, cfHandles);
    } catch (RocksDBException e) {
      dbOptions.close();
      throw e;
    }
  }

  Map<Long, OSHDBData> entities(List<Long> ids) throws RocksDBException {
    var opt = new ReadOptions();
    var cfsList = new ColumnFamilyHandle[ids.size()];
    Arrays.fill(cfsList, cfHandles.get(1));

    var keys = new ArrayList<byte[]>(ids.size());
    for (Long id : ids) {
      keys.add(idToBytes(id));
    }
    var entityGridId = db.multiGetAsList(opt, Arrays.asList(cfsList), keys);

    var gridEntityKey = new ArrayList<byte[]>(keys.size());
    for (var i = 0; i < keys.size(); i++) {
      var gridId = entityGridId.get(i);
      var key = keys.get(i);
      if (gridId != null) {
        gridEntityKey.add(gridEntityKey(gridId, key));
      }
    }
    var data = db.multiGetAsList(gridEntityKey).iterator();
    var result = Maps.<Long, OSHDBData>newHashMapWithExpectedSize(gridEntityKey.size());
    for (var i = 0; i < ids.size(); i++) {
      var id = ids.get(i);
      var gridId = entityGridId.get(i);
      if (gridId != null) {
        result.put(id, OSHDBData.of(type, id, ByteBuffer.wrap(gridId).getLong(), data.next()));
      }
    }
    return result;
  }

  List<OSHDBData> byGrid(long gridId) throws RocksDBException {
    var gridKey = idToBytes(gridId);
    var gridEntityKey = gridEntityKey(gridKey, idToBytes(0));
    try (var opts = new ReadOptions().setIterateUpperBound(new Slice(idToBytes(gridId + 1)));
         var itr = db.newIterator(opts);) {
      itr.seek(gridEntityKey);
      var list = new ArrayList<OSHDBData>();
      for (; itr.isValid(); itr.next()) {
        var key = itr.key();
        var data = itr.value();
        var entityId = ByteBuffer.wrap(key, 8, 8).getLong();
        list.add(OSHDBData.of(type, entityId, gridId, data));
      }
      itr.status();
      return list;
    }
  }

  private byte[] gridEntityKey(byte[] gridId, byte[] entityId) {
    return ByteBuffer.allocate(Long.BYTES * 2).put(gridId).put(entityId).array();
  }

  void put(List<OSHDBData> entities) throws RocksDBException {
    var opt = new ReadOptions();
    var cfsList = new ColumnFamilyHandle[entities.size()];
    Arrays.fill(cfsList, cfHandles.get(1));

    var keys = new ArrayList<byte[]>(entities.size());
    for (var entity : entities) {
      keys.add(idToBytes(entity.getId()));
    }
    var entityGridId = db.multiGetAsList(opt, Arrays.asList(cfsList), keys);

    try (var wb = new WriteBatch()) {
      for (var i = 0; i < entities.size(); i++) {
        var entity = entities.get(i);
        var key = keys.get(i);
        var gridId = entityGridId.get(i);

        if (gridId != null && bytesToId(gridId) != entity.getGridId()) {
          wb.delete(cfHandles.get(0), gridEntityKey(gridId, key));
          gridId = null;
        }

        if (gridId == null) {
          gridId = idToBytes(entity.getGridId());
        }

        wb.put(cfHandles.get(1), key, gridId);
        var gridEntityId = gridEntityKey(gridId, key);
        wb.put(cfHandles.get(0), gridEntityId, entity.getData());
      }
      db.write(new WriteOptions(), wb);
    }
  }

  private byte[] idToBytes(long id) {
    return ByteBuffer.allocate(Long.BYTES).putLong(id).array();
  }

  private long bytesToId(byte[] id) {
    return ByteBuffer.wrap(id).getLong();
  }


  void close() {
    cfHandles.forEach(ColumnFamilyHandle::close);
    db.close();
    dbOptions.close();
  }
}
