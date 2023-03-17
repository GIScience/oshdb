package org.heigit.ohsome.oshdb.rocksdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.h2.jdbcx.JdbcConnectionPool;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.store.BackRefType;
import org.heigit.ohsome.oshdb.store.OSHDBStore;
import org.heigit.ohsome.oshdb.store.OSHData;
import org.heigit.ohsome.oshdb.util.tagtranslator.JdbcTagTranslator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;
import org.rocksdb.util.SizeUnit;


class RocksDBStoreTest {

  private static final Path STORE_TEST_PATH = Path.of("tests/store/rocksdb");

  OSHDBStore openStore() throws RocksDBException, IOException {
    Files.createDirectories(STORE_TEST_PATH);
    var dataSource = JdbcConnectionPool.create("jdbc:h2:" + STORE_TEST_PATH.resolve("keytables"),"sa","");
    var tagTranslator = new JdbcTagTranslator(dataSource);
    return new RocksDBStore(tagTranslator, STORE_TEST_PATH, 10 * SizeUnit.MB);
  }

  @AfterAll
  static void cleanUp() throws Exception {
    //noinspection UnstableApiUsage
    MoreFiles.deleteRecursively(STORE_TEST_PATH, RecursiveDeleteOption.ALLOW_INSECURE);
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

      var actuals = store.entities(OSMType.NODE, Set.of(20L));
      System.out.println("actual = " + actuals);
      var actual = actuals.get(20L);
      assertNotNull(actual);
      assertEquals(20L, actual.getId());
      assertArrayEquals("Test Node 20".getBytes(), actual.getData());
      assertEquals(2L, actual.getGridId());

      var grid = store.grid(OSMType.NODE, 2L);
      assertEquals(2, grid.size());

      store.entities(Set.of(new OSHData(OSMType.NODE, 22, 22, "Test Node 22 updated".getBytes())));
      actual = store.entity(OSMType.NODE, 22);
      assertArrayEquals("Test Node 22 updated".getBytes(), actual.getData());
      assertEquals(22L, actual.getGridId());
      grid = store.grid(OSMType.NODE, 2L);
      assertEquals(1, grid.size());
      grid = store.grid(OSMType.NODE, 22L);
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

  @Test
  void backRefs() throws Exception {
    try (var store = openStore()) {
      store.backRefsMerge(BackRefType.NODE_WAY, 1234L, Set.of(1L, 2L, 3L, 4L));
      var backRefs = store.backRefs(OSMType.NODE, Set.of(1L, 2L, 3L, 4L));
      assertEquals(4, backRefs.size());
      Assertions.assertThat(backRefs.get(1L).ways())
          .hasSameElementsAs(Set.of(1234L));
      store.backRefsMerge(BackRefType.NODE_WAY, 2222L, Set.of(1L, 4L));
      backRefs = store.backRefs(OSMType.NODE, Set.of(1L));
      assertEquals(1, backRefs.size());
      Assertions.assertThat(backRefs.get(1L).ways())
          .hasSameElementsAs(Set.of(1234L, 2222L));
    }
    try (var store = openStore()) {
      var backRefs = store.backRefs(OSMType.NODE, Set.of(4L));
      assertEquals(1, backRefs.size());
      Assertions.assertThat(backRefs.get(4L).ways())
          .hasSameElementsAs(Set.of(1234L, 2222L));
    }
  }
}