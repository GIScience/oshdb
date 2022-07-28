package org.heigit.ohsome.oshdb.api.tests;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
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

  private static final double DELTA = 1e-8;

  TestFlatMapAggregate() throws Exception {
    oshdb = new OSHDBH2("../data/test-data");
  }

  private OSMContributionView createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.view()
        .areaOfInterest(bbox)
        .filter("type:node and highway=*");
  }

  @Test
  void test() throws Exception {
    var result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .on(oshdb)
        .flatMap(
            contribution -> Stream.of(contribution)
            .map(OSMContribution::getEntityAfter)
            .filter(entity -> entity.getId() == 617308093)
            .flatMap(entity -> entity.getTags().stream()
                .map(tag -> Map.entry(entity.getId(), Map.entry(tag.getKey(), tag.getValue())))))
        .aggregateBy(Entry::getKey)
        .map(Entry::getValue)
        .reduce(() -> new HashSet<>(),
            (x, y) -> {
              x.add(y);
              return x; },
            (x, y) -> {
              var ret = new HashSet<>(x);
              ret.addAll(y);
              return ret; });

    assertEquals(1, result.entrySet().size());
    assertEquals(2, result.get(617308093L).size());
  }
}
