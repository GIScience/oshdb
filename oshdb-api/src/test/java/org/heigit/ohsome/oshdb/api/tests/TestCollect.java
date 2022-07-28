package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.google.common.collect.Iterables;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.mapreducer.contribution.OSMContributionView;
import org.heigit.ohsome.oshdb.api.mapreducer.reduction.Collector;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * Tests the collect method of the OSHDB API.
 */
class TestCollect {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox =
      OSHDBBoundingBox.bboxWgs84Coordinates(8.651133, 49.387611, 8.6561, 49.390513);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  TestCollect() throws Exception {
    oshdb = new OSHDBH2("../data/test-data");
  }

  private OSMContributionView createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.view()
        .areaOfInterest(bbox)
        .filter("type:way and building=yes");
  }

  @Test
  void testCollect() throws Exception {
    List<Long> result = this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .on(oshdb)
        .map(OSMContribution::getEntityAfter)
        .map(OSMEntity::getId)
        .collect(Collector.toList());
    assertEquals(42, result
        .stream()
        .collect(Collectors.toSet()).size());
  }

  @Test
  void testMapCollect() throws Exception {
    List<Long> result = this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .on(oshdb)
        .map(contribution -> contribution.getEntityAfter().getId())
        .reduce(Collector::toList);
    assertEquals(42, result
        .stream()
        .collect(Collectors.toSet()).size());
  }

  @Test
  void testFlatMapCollect() throws Exception {
    List<Long> result = this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .on(oshdb)
        .flatMapIterable(contribution -> Collections
            .singletonList(contribution.getEntityAfter().getId()))
        .reduce(Collector::toList);
    assertEquals(42, result
        .stream()
        .collect(Collectors.toSet()).size());
  }

  @Test
  void testFlatMapCollectGroupedById() throws Exception {
    List<Long> result = this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .on(oshdb)
        .groupByEntity()
        .flatMapIterable(contributions -> Collections
            .singletonList(contributions.get(0).getEntityAfter().getId()))
        .reduce(Collector::toList);
    assertEquals(42, result
        .stream()
        .collect(Collectors.toSet()).size());
  }

  @Test
  void testAggregatedByTimestamp() throws Exception {
    Map<OSHDBTimestamp, List<Long>> result = this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateByTimestamp()
        .map(contribution -> contribution.getEntityAfter().getId())
        .reduce(Collector::toList);
    assertEquals(14, result
        .get(Iterables.get(timestamps72.get(), 60))
        .size());
  }
}
