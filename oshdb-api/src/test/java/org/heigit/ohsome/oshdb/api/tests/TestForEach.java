package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.ConcurrentHashMap;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.mapreducer.contribution.OSMContributionView;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * Test forEach method of the OSHDB API.
 */
class TestForEach {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox =
      OSHDBBoundingBox.bboxWgs84Coordinates(8.651133, 49.387611, 8.6561, 49.390513);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  TestForEach() throws Exception {
    oshdb = new OSHDBH2("../data/test-data");
  }

  private OSMContributionView createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.view()
        .areaOfInterest(bbox)
        .filter("type:way and building=yes");
  }

  @Test
  void testForEach() throws Exception {
    ConcurrentHashMap<Long, Boolean> result = new ConcurrentHashMap<>();
    this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .on(oshdb)
        .forEach(contribution -> {
          result.put(contribution.getEntityAfter().getId(), true);
        });
    assertEquals(42, result.entrySet().size());
  }

  @Test
  void testForEachGroupedById() throws Exception {
    ConcurrentHashMap<Long, Boolean> result = new ConcurrentHashMap<>();
    this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .on(oshdb)
        .groupByEntity()
        .forEach(contributions -> {
          contributions.forEach(contribution -> {
            result.put(contribution.getEntityAfter().getId(), true);
          });
        });
    assertEquals(42, result.entrySet().size());
  }

  @Test
  void testForEachAggregatedByTimestamp() throws Exception {
    ConcurrentHashMap<Long, Boolean> result = new ConcurrentHashMap<>();
    this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateByTimestamp()
        .forEach((ts, contribution) -> result.put(contribution.getEntityAfter().getId(), true));
    assertEquals(42, result.entrySet().size());
  }
}
