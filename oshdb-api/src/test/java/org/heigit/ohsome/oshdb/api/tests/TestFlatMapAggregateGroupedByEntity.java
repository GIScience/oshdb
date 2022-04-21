package org.heigit.ohsome.oshdb.api.tests;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.celliterator.ContributionType;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * Test flat map method with groupByEntity of the MapAggregator class of the OSHDB API.
 */
class TestFlatMapAggregateGroupedByEntity {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox = bboxWgs84Coordinates(8.0, 49.0, 9.0, 50.0);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  private static final double DELTA = 1e-8;

  public TestFlatMapAggregateGroupedByEntity() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data");
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView
        .on(oshdb)
        .osmType(OSMType.NODE)
        .osmTag("highway")
        .areaOfInterest(bbox);
  }

  @Test
  void test() throws Exception {
    SortedMap<Long, Integer> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .groupByEntity()
        .flatMap(contributions -> {
          if (contributions.get(0).getEntityAfter().getId() != 617308093) {
            return new ArrayList<>();
          }
          List<Entry<Long, Integer>> ret = new ArrayList<>();
          ret.add(new SimpleImmutableEntry<>(
              contributions.get(0).getEntityAfter().getId(),
              (int) contributions
                  .stream()
                  .filter(c -> c.getContributionTypes().contains(ContributionType.GEOMETRY_CHANGE))
                  .count()
          ));
          ret.add(new SimpleImmutableEntry<>(
              contributions.get(0).getEntityAfter().getId(),
              2
          ));
          return ret;
        })
        .aggregateBy(Entry::getKey)
        .map(Entry::getValue)
        .reduce(
            () -> 0,
            Integer::sum,
            Integer::sum
        );

    assertEquals(1, result.entrySet().size());
    assertEquals(5 + 2, result.get(617308093L).intValue());
  }
}
