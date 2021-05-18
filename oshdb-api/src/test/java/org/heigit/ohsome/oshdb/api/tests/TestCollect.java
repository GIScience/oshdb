package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Iterables;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.Test;

/**
 * Tests the collect method of the OSHDB API.
 */
public class TestCollect {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox =
      OSHDBBoundingBox.bboxLonLatCoordinates(8.651133, 49.387611, 8.6561, 49.390513);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  public TestCollect() throws Exception {
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
  public void testCollect() throws Exception {
    List<OSMContribution> result = this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .collect();
    assertEquals(42, result
        .stream()
        .map(contribution -> contribution.getEntityAfter().getId())
        .collect(Collectors.toSet()).size());
  }

  @Test
  public void testMapCollect() throws Exception {
    List<Long> result = this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .map(contribution -> contribution.getEntityAfter().getId())
        .collect();
    assertEquals(42, result
        .stream()
        .collect(Collectors.toSet()).size());
  }

  @Test
  public void testFlatMapCollect() throws Exception {
    List<Long> result = this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .flatMap(contribution -> Collections
            .singletonList(contribution.getEntityAfter().getId()))
        .collect();
    assertEquals(42, result
        .stream()
        .collect(Collectors.toSet()).size());
  }

  @Test
  public void testFlatMapCollectGroupedById() throws Exception {
    List<Long> result = this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .groupByEntity()
        .flatMap(contributions -> Collections
            .singletonList(contributions.get(0).getEntityAfter().getId()))
        .collect();
    assertEquals(42, result
        .stream()
        .collect(Collectors.toSet()).size());
  }

  @Test
  public void testAggregatedByTimestamp() throws Exception {
    SortedMap<OSHDBTimestamp, List<Long>> result = this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .aggregateByTimestamp()
        .map(contribution -> contribution.getEntityAfter().getId())
        .collect();
    assertEquals(14, result
        .get(Iterables.get(timestamps72.get(), 60))
        .size());
  }
}
