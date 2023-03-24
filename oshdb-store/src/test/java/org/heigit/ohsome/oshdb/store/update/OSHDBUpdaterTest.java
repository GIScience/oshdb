package org.heigit.ohsome.oshdb.store.update;

import static org.heigit.ohsome.oshdb.osm.OSM.node;
import static org.heigit.ohsome.oshdb.osm.OSM.way;
import static org.heigit.ohsome.oshdb.osm.OSMType.NODE;
import static org.heigit.ohsome.oshdb.osm.OSMType.WAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static reactor.core.publisher.Flux.just;

import com.google.common.collect.Iterables;
import java.util.List;
import java.util.Set;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.source.ReplicationInfo;
import org.heigit.ohsome.oshdb.store.memory.MemoryStore;
import org.junit.jupiter.api.Test;
import reactor.util.function.Tuples;

class OSHDBUpdaterTest {

  @Test
  void entities() {
    ReplicationInfo state = null;
    try (var store = new MemoryStore(state)) {
      var updater = new OSHDBUpdater(store, (type, cellId, grid) -> {}, false);

      var count = updater.updateEntities(just(Tuples.of(NODE, just(
              node(1L, 1, 1000, 100, 1, List.of(), 0, 0)))))
          .count().block();
      assertEquals(1L, count);
      var nodes = store.entities(NODE, Set.of(1L));
      assertEquals(1, nodes.size());
      var node = nodes.get(1L);
      assertNotNull(node);
      System.out.println(node.getOSHEntity().toString());

      updater.updateEntities(just(Tuples.of(WAY, just(
          way(1,1,2000, 200, 1, List.of(), new OSMMember[]{
              new OSMMember(1, NODE,-1)})))))
          .count().block();
      assertEquals(1L, count);

      var ways = store.entities(WAY, Set.of(1L));
      assertEquals(1, ways.size());
      var way = ways.get(1L);
      assertNotNull(way);

      count = updater.updateEntities(just(Tuples.of(NODE, just(
              node(1L, 2, 2000, 200, 2, List.of(), 10, 10)))))
          .count().block();
      assertEquals(2L, count); // major node, minor way

      nodes = store.entities(NODE, Set.of(1L));
      assertEquals(1, nodes.size());
      node = nodes.get(1L);
      assertNotNull(node);
      assertEquals(2, Iterables.size(node.getOSHEntity().getVersions()));

    }
  }

}