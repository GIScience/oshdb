/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.celliterator.ContributionType;
import org.junit.Test;

import java.util.Set;
import java.util.SortedMap;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestFilter {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8, 49, 9, 50);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01", OSHDBTimestamps.Interval.MONTHLY);

  private final double DELTA = 1e-8;

  public TestFilter() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data");
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb).osmType(OSMType.NODE).osmTag("highway").areaOfInterest(bbox);
  }

  @Test
  public void testFilter() throws Exception {
    Set<Integer> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .osmEntityFilter(entity -> entity.getId() == 617308093)
        .filter(contribution -> contribution.getContributionTypes().contains(ContributionType.GEOMETRY_CHANGE))
        .map(OSMContribution::getContributorUserId)
        .uniq();

    // should be 3: first version doesn't have the highway tag, remaining 7 versions have 5 different contributor user ids, but last two didn't modify the node's coordinates
    assertEquals(3, result.size());
  }

  @Test
  public void testAggregateFilter() throws Exception {
    SortedMap<Long, Set<Integer>> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .osmEntityFilter(entity -> entity.getId() == 617308093)
        .aggregateBy(contribution -> contribution.getEntityAfter().getId())
        .filter(contribution -> contribution.getContributionTypes().contains(ContributionType.GEOMETRY_CHANGE))
        .map(OSMContribution::getContributorUserId)
        .uniq();

    assertEquals(1, result.entrySet().size());
    // should be 3: first version doesn't have the highway tag, remaining 7 versions have 5 different contributor user ids, but last two didn't modify the node's coordinates
    assertEquals(3, result.get(617308093L).size());
  }
}
