package org.heigit.ohsome.oshdb.rocksdb;

import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.io.MoreFiles;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.store.BackRefs;
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
      if (Files.exists(TEST_STORE_PATH)) {
        MoreFiles.deleteRecursively(TEST_STORE_PATH, ALLOW_INSECURE);
      }
    }
  }

  @Test
  void addEntities() {
    var node = OSHDBData.of(OSMType.NODE, 123L, 0L, "NODE Troilo".getBytes());
    var way = OSHDBData.of(OSMType.WAY, 345L, 0L, "WAY Troilo".getBytes());
    var relation = OSHDBData.of(OSMType.RELATION, 567L, 0L, "REL Troilo".getBytes());
    store.putEntities(OSMType.WAY, List.of(way));
    store.putEntities(OSMType.NODE, List.of(node));
    store.putEntities(OSMType.RELATION, List.of(relation));

    assertStoreContains(node);
    assertStoreContains(way);
    assertStoreContains(relation);

    var byGrid = store.entitiesByGrid(OSMType.NODE, 0L);
    assertEquals(1, byGrid.size());
    assertArrayEquals(node.getData(), byGrid.get(0).getData());

    var update = OSHDBData.of(OSMType.NODE, 123L, 1L, "NODE Troilo 2".getBytes());
    store.putEntities(OSMType.NODE, List.of(update));
    assertStoreContains(update);

    byGrid = store.entitiesByGrid(OSMType.NODE, 0L);
    assertEquals(0, byGrid.size());

    byGrid = store.entitiesByGrid(OSMType.NODE, 1L);
    assertEquals(1, byGrid.size());
    assertArrayEquals(update.getData(), byGrid.get(0).getData());
  }

  @Test
  void backRefs() {
    store.appendBackRefs(OSMType.NODE,
        List.of(new BackRefs(OSMType.NODE, 123L, Set.of(1L, 2L), Set.of())));
    var backRefs = store.backRefs(OSMType.NODE, List.of(123L));
    assertEquals(1, backRefs.size());
    var backRef = backRefs.get(123L);
    assertNotNull(backRef);
    assertEquals(2, backRef.ways().size());
    assertEquals(0, backRef.relations().size());
    assertTrue(backRef.ways().contains(1L));
    assertTrue(backRef.ways().contains(2L));

    store.appendBackRefs(OSMType.NODE,
        List.of(new BackRefs(OSMType.NODE, 123L, Set.of(3L), Set.of())));

    backRefs = store.backRefs(OSMType.NODE, List.of(123L));
    assertEquals(1, backRefs.size());
    backRef = backRefs.get(123L);
    assertNotNull(backRef);

    assertEquals(3, backRef.ways().size());
    assertEquals(0, backRef.relations().size());
    assertTrue(backRef.ways().contains(1L));
    assertTrue(backRef.ways().contains(2L));
    assertTrue(backRef.ways().contains(3L));
  }

  private void assertStoreContains(OSHDBData data) {
    var actual = store.entities(data.getType(), List.of(data.getId())).get(data.getId());
    assertNotNull(actual);
    assertArrayEquals(data.getData(), actual.getData());
  }
}
