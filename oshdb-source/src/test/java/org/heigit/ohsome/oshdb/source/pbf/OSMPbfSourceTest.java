package org.heigit.ohsome.oshdb.source.pbf;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.Collections;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.tagtranslator.MemoryTagTranslator;
import org.junit.jupiter.api.Test;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

class OSMPbfSourceTest {

  private static final Path SAMPLE_PBF_PATH = Path.of("../data/sample.pbf");

  @Test
  void entities() {
    var tagTranslator = new MemoryTagTranslator();
    var source = new OSMPbfSource(SAMPLE_PBF_PATH);
    var map = source.entities(tagTranslator)
        .concatMap(tuple -> tuple.getT2().count().map(count -> Tuples.of(tuple.getT1(), count)))
        .windowUntilChanged(Tuple2::getT1)
        .concatMap(wnd -> wnd.reduce((t1, t2) -> Tuples.of(t1.getT1(), t1.getT2() + t2.getT2())))
        .collectMap(Tuple2::getT1, Tuple2::getT2)
        .blockOptional().orElseGet(Collections::emptyMap);
    assertEquals(290, map.getOrDefault(OSMType.NODE, 0L));
    assertEquals(44, map.getOrDefault(OSMType.WAY, 0L));
    assertEquals(5, map.getOrDefault(OSMType.RELATION, 0L));

  }
}