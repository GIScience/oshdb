package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.ConcurrentHashMap;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * Test for each method of the OSHDB API.
 */
class TestForEach {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox =
      OSHDBBoundingBox.bboxWgs84Coordinates(8.651133, 49.387611, 8.6561, 49.390513);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  TestForEach() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data");
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView
        .on(oshdb)
        .osmType(OSMType.WAY)
        .osmTag("building", "yes")
        .areaOfInterest(bbox);
  }

  @Test
  void testForEach() throws Exception {
    ConcurrentHashMap<Long, Boolean> result = new ConcurrentHashMap<>();
    this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
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
        .aggregateByTimestamp()
        .forEach((ts, contributions) -> {
          contributions.forEach(contribution -> {
            result.put(contribution.getEntityAfter().getId(), true);
          });
        });
    assertEquals(42, result.entrySet().size());
  }
}
