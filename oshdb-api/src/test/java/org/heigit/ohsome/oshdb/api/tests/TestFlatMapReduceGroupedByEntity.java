package org.heigit.ohsome.oshdb.api.tests;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.mapreducer.aggregation.Agg;
import org.heigit.ohsome.oshdb.api.mapreducer.contribution.OSMContributionView;
import org.heigit.ohsome.oshdb.api.mapreducer.snapshot.OSMEntitySnapshotView;
import org.heigit.ohsome.oshdb.util.celliterator.ContributionType;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * Test flatMap method with groupByEntity of the MapReducer class of the OSHDB API.
 */
abstract class TestFlatMapReduceGroupedByEntity {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox = bboxWgs84Coordinates(8.0, 49.0, 9.0, 50.0);
  private final OSHDBTimestamps timestamps6 = new OSHDBTimestamps("2010-01-01", "2015-01-01",
      OSHDBTimestamps.Interval.YEARLY);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  TestFlatMapReduceGroupedByEntity(OSHDBDatabase oshdb) throws Exception {
    this.oshdb = oshdb;
  }

  private OSMContributionView createMapReducerOSMContribution() throws Exception {
    return new OSMContributionView(oshdb, null)
        .areaOfInterest(bbox)
        .filter("type:node and highway=*");
  }

  private OSMEntitySnapshotView createMapReducerOSMEntitySnapshot() throws Exception {
    return new OSMEntitySnapshotView(oshdb, null)
        .areaOfInterest(bbox)
        .filter("type:node and highway=*");
  }

  @Test
  void testOSMContributionView() throws Exception {
    Number result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .view()
        .groupByEntity()
        .flatMapIterable(contributions -> {
          if (contributions.get(0).getEntityAfter().getId() != 617308093) {
            return new ArrayList<>();
          }
          List<Integer> ret = new ArrayList<>();
          contributions
              .stream()
              .filter(c -> c.getContributionTypes().contains(ContributionType.GEOMETRY_CHANGE))
              .map(ignored -> 1)
              .forEach(ret::add);
          ret.add(2); // just add another "2" for good measure ;-)
          return ret;
        })
        .reduce(
            () -> 0,
            Integer::sum,
            Integer::sum
        );

    assertEquals(5 + 2, result.intValue());
  }

  @Test
  void testOSMEntitySnapshotView() throws Exception {
    Number result = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps6)
        .view()
        .groupByEntity()
        .flatMapIterable(snapshots -> {
          if (snapshots.get(0).getEntity().getId() != 617308093) {
            return new ArrayList<>();
          }
          List<Integer> ret = new ArrayList<>();
          for (int i = 1; i < snapshots.size(); i++) {
            if (!snapshots.get(i - 1).getGeometry().equals(snapshots.get(i).getGeometry())) {
              ret.add(1);
            }
          }
          ret.add(2); // just add another "2" for good measure ;-)
          return ret;
        })
        .reduce(Agg::sumInt);

    assertEquals(2 + 2, result.intValue());
  }
}
