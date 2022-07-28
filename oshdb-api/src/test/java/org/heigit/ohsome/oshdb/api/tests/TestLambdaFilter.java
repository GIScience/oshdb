package org.heigit.ohsome.oshdb.api.tests;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Set;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.mapreducer.contribution.OSMContributionView;
import org.heigit.ohsome.oshdb.api.mapreducer.reduction.Reduce;
import org.heigit.ohsome.oshdb.util.celliterator.ContributionType;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * Tests lambda functions as filters.
 */
class TestLambdaFilter {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox = bboxWgs84Coordinates(8.0, 49.0, 9.0, 50.0);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  private static final double DELTA = 1e-8;

  TestLambdaFilter() throws Exception {
    oshdb = new OSHDBH2("../data/test-data");
  }

  private OSMContributionView createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.view()
        .areaOfInterest(bbox)
        .filter("type:node and highway=*");
  }

  @Test
  void testFilter() throws Exception {
    Set<Integer> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .filter("id:617308093")
        .on(oshdb)
        .filter(contribution -> contribution
            .getContributionTypes()
            .contains(ContributionType.GEOMETRY_CHANGE))
        .map(OSMContribution::getContributorUserId)
        .reduce(Reduce::uniq);

    // should be 3: first version doesn't have the highway tag, remaining 7 versions have 5
    // different contributor user ids, but last two didn't modify the node's coordinates
    assertEquals(3, result.size());
  }

  @Test
  void testAggregateFilter() throws Exception {
    Map<Long, Set<Integer>> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .filter("id:617308093")
        .on(oshdb)
        .aggregateBy(contribution -> contribution.getEntityAfter().getId())
        .filter(contribution -> contribution
            .getContributionTypes()
            .contains(ContributionType.GEOMETRY_CHANGE))
        .map(OSMContribution::getContributorUserId)
        .reduce(Reduce::uniq);

    assertEquals(1, result.entrySet().size());
    // should be 3: first version doesn't have the highway tag, remaining 7 versions have 5
    // different contributor user ids, but last two didn't modify the node's coordinates
    assertEquals(3, result.get(617308093L).size());
  }
}
