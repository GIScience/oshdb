package org.heigit.ohsome.oshdb.api.tests;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbcImprove;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.contribution.OSMContributionView;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * Test flat map method of the MapAggregator class of the OSHDB API.
 */
class TestFlatMapAggregate {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox = bboxWgs84Coordinates(8.0, 49.0, 9.0, 50.0);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  TestFlatMapAggregate() throws Exception {
    oshdb = new OSHDBJdbcImprove("./src/test/resources/test-data");
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution(OSHDBTimestamps timestamps)
      throws Exception {
    return OSMContributionView.on(oshdb)
        .areaOfInterest(bbox)
        .filter("type:node and highway=*")
        .timestamps(timestamps)
        .view();
  }

  @Test
  void test() throws Exception {
    SortedMap<Long, Set<Entry<Integer, Integer>>> result =
        createMapReducerOSMContribution(timestamps72)
        .flatMap(
            contribution -> Stream.of(contribution)
                .map(OSMContribution::getEntityAfter)
                .filter(e -> e.getId() == 617308093)
                .flatMap(e -> e.getTags().stream()
                    .map(tag -> Map.entry(e.getId(), Map.entry(tag.getKey(), tag.getValue())))))
        .aggregateBy(Entry::getKey)
        .map(Entry::getValue)
        .reduce(
            HashSet::new,
            (x, y) -> {
              x.add(y);
              return x;
            },
            (x, y) -> {
              Set<Entry<Integer, Integer>> ret = new HashSet<>(x);
              ret.addAll(y);
              return ret;
            });

    assertEquals(1, result.entrySet().size());
    assertEquals(2, result.get(617308093L).size());
  }
}
