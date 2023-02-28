package org.heigit.ohsome.oshdb.rocksdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.store.OSHDBStore;
import org.heigit.ohsome.oshdb.store.OSHData;
import org.heigit.ohsome.oshdb.util.CellId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;
import org.rocksdb.util.SizeUnit;

class RocksDBStoreTest {

  private static final Path STORE_TEST_PATH = Path.of("tests/store/rocksdb");

  OSHDBStore openStore() throws RocksDBException, IOException {
    return new RocksDBStore(STORE_TEST_PATH, 10 * SizeUnit.MB);
  }

  @AfterEach
  void cleanUp() throws Exception {
    MoreFiles.deleteRecursively(STORE_TEST_PATH, RecursiveDeleteOption.ALLOW_INSECURE);
    System.out.println("clean up");
  }

  @Test
  void entities() throws Exception {
    try (var store = openStore()) {
      var entities = Set.of(
          new OSHData(OSMType.NODE, 10, 1, "Test Node 10".getBytes())
          , new OSHData(OSMType.NODE, 20, 2, "Test Node 20".getBytes())
          , new OSHData(OSMType.NODE, 22, 2, "Test Node 22".getBytes())
          , new OSHData(OSMType.NODE, 30, 3, "Test Node 30".getBytes())
      );
      store.entities(entities);
      var actual = store.entity(OSMType.NODE, 20);
      assertNotNull(actual);
      assertEquals(20L, actual.getId());
      assertArrayEquals("Test Node 20".getBytes(), actual.getData());
      assertEquals(2L, actual.getGridId());

      var grid = store.grid(OSMType.NODE, CellId.fromLevelId(2));
      assertEquals(2, grid.size());

      store.entities(Set.of(new OSHData(OSMType.NODE, 22, 22, "Test Node 22 updated".getBytes())));
      actual = store.entity(OSMType.NODE, 22);
      assertArrayEquals("Test Node 22 updated".getBytes(), actual.getData());
      assertEquals(22L, actual.getGridId());
      grid = store.grid(OSMType.NODE, CellId.fromLevelId(2));
      assertEquals(1, grid.size());
      grid = store.grid(OSMType.NODE, CellId.fromLevelId(22));
      assertEquals(1, grid.size());
    }

    try (var store = openStore()) {
      var actual = store.entity(OSMType.NODE, 30);
      assertNotNull(actual);
      assertEquals(30L, actual.getId());
      assertArrayEquals("Test Node 30".getBytes(), actual.getData());
      assertEquals(3L, actual.getGridId());
    }
  }
}