package org.heigit.ohsome.oshdb.api.tests;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.junit.Assert.assertEquals;

import java.util.Set;
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
import org.junit.Test;

/**
 * Tests lambda functions as filters.
 */
public class TestLambdaFilter {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox = bboxWgs84Coordinates(8.0, 49.0, 9.0, 50.0);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  private static final double DELTA = 1e-8;

  public TestLambdaFilter() throws Exception {
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
  public void testFilter() throws Exception {
    Set<Integer> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .osmEntityFilter(entity -> entity.getId() == 617308093)
        .filter(contribution -> contribution
            .getContributionTypes()
            .contains(ContributionType.GEOMETRY_CHANGE))
        .map(OSMContribution::getContributorUserId)
        .uniq();

    // should be 3: first version doesn't have the highway tag, remaining 7 versions have 5
    // different contributor user ids, but last two didn't modify the node's coordinates
    assertEquals(3, result.size());
  }

  @Test
  public void testAggregateFilter() throws Exception {
    SortedMap<Long, Set<Integer>> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .osmEntityFilter(entity -> entity.getId() == 617308093)
        .aggregateBy(contribution -> contribution.getEntityAfter().getId())
        .filter(contribution -> contribution
            .getContributionTypes()
            .contains(ContributionType.GEOMETRY_CHANGE))
        .map(OSMContribution::getContributorUserId)
        .uniq();

    assertEquals(1, result.entrySet().size());
    // should be 3: first version doesn't have the highway tag, remaining 7 versions have 5
    // different contributor user ids, but last two didn't modify the node's coordinates
    assertEquals(3, result.get(617308093L).size());
  }
}
