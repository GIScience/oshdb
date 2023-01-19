package org.heigit.ohsome.oshdb.rocksdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.io.MoreFiles;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.store.OSHDBData;
import org.heigit.ohsome.oshdb.store.OSHDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RocksDBStoreTest {
  private static final Path TEST_STORE_PATH = Path.of("test/oshdb-store");
  private OSHDBStore store;

  @BeforeEach
  void openStore() throws IOException {
    store = RocksDBStore.open(TEST_STORE_PATH, 1L << 10);
  }

  @AfterEach
  void closeStore() throws Exception {
    try {
      store.close();
    } finally {
      if (Files.exists(TEST_STORE_PATH)){
        MoreFiles.deleteRecursively(TEST_STORE_PATH);
      }
    }
  }

  @Test
  void addEntities() {
    var node = OSHDBData.of(OSMType.NODE, 123L, 0L, "NODE Troilo".getBytes());
    var way = OSHDBData.of(OSMType.WAY, 345L, 0L, "WAY Troilo".getBytes());
    var relation = OSHDBData.of(OSMType.RELATION, 567L, 0L, "REL Troilo".getBytes());
    store.entities(List.of(way,node,relation));

    assertStoreContains(node);
    assertStoreContains(way);
    assertStoreContains(relation);

    var byGrid = store.entitiesByGrid(OSMType.NODE, List.of(0L));
    assertEquals(1, byGrid.get(0L).size());
    assertArrayEquals(node.getData(), byGrid.get(0L).get(0).getData());

    var update = OSHDBData.of(OSMType.NODE, 123L, 1L, "NODE Troilo 2".getBytes());
    store.entities(List.of(update));
    assertStoreContains(update);

    byGrid = store.entitiesByGrid(OSMType.NODE, List.of(0L, 1L));
    assertEquals(0, byGrid.get(0L).size());
    assertEquals(1, byGrid.get(1L).size());
    assertArrayEquals(update.getData(),byGrid.get(1L).get(0).getData());
  }

  private void assertStoreContains(OSHDBData data) {
   var actual = store.entities(data.getType(), List.of(data.getId())).get(data.getId());
   assertNotNull(actual);
   assertArrayEquals(data.getData(), actual.getData());
  }
}
