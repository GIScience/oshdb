package org.heigit.ohsome.oshdb.source.osc;

import static java.time.Duration.ZERO;
import static java.time.Duration.ofSeconds;
import static org.heigit.ohsome.oshdb.source.osc.ReplicationEndpoint.OSM_ORG_MINUTELY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.Collections;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.tagtranslator.MemoryTagTranslator;
import org.junit.jupiter.api.Test;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

class ReplicationStateTest {

  @Test
  void serverState() throws IOException {
    var serverState = ReplicationState.getServerState(OSM_ORG_MINUTELY);
    assertNotNull(serverState);
    System.out.println("serverState = " + serverState);
  }

  @Test
  void state() throws IOException {
    var state = ReplicationState.getState(OSM_ORG_MINUTELY, 5487541);
    assertNotNull(state);
    assertEquals(5487541, state.getSequenceNumber());
    var tagTranslator = new MemoryTagTranslator();
    var entities = state.entities(tagTranslator)
        .concatMap(tuple -> tuple.getT2().count().map(count -> Tuples.of(tuple.getT1(), count)))
        .collectMap(Tuple2::getT1, Tuple2::getT2)
        .blockOptional().orElseGet(Collections::emptyMap);
    assertEquals(30L, entities.getOrDefault(OSMType.NODE, -1L));
    assertEquals(15L, entities.getOrDefault(OSMType.WAY, -1L));
  }

  @Test
  void localState() throws IOException {
    var endpoint = new ReplicationEndpoint("file:///data/", ofSeconds(1), ZERO);
    var state = endpoint.serverState();
    assertNotNull(state);
  }

}