package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.ConcurrentHashMap;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.mapreducer.contribution.OSMContributionView;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * Tests the stream method of the OSHDB API.
 */
class TestStream {
  private final OSHDBH2 oshdb;

  private final OSHDBBoundingBox bbox =
      OSHDBBoundingBox.bboxWgs84Coordinates(8.651133, 49.387611, 8.6561, 49.390513);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  TestStream() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data").multithreading(false);
  }

  private OSHDBView<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb)
        .areaOfInterest(bbox)
        .filter("type:way and building=yes");
  }

  @Test
  void testForEach() throws Exception {
    ConcurrentHashMap<Long, Boolean> result = new ConcurrentHashMap<>();
    this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .view()
        .stream()
        .forEach(contribution -> {
          result.put(contribution.getEntityAfter().getId(), true);
        });
    assertEquals(42, result.entrySet().size());
  }

  @Test
  void testForEachAggregatedByTimestamp() throws Exception {
    ConcurrentHashMap<Long, Boolean> result = new ConcurrentHashMap<>();
    this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .view()
        .aggregateByTimestamp()
        .stream()
        .forEach(entry ->
          result.put(entry.getValue().getEntityAfter().getId(), true)
        );
    assertEquals(42, result.entrySet().size());
  }
}
