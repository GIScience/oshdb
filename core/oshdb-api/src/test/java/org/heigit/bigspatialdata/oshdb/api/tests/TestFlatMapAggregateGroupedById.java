/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.api.tests;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.ContributionType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestFlatMapAggregateGroupedById {
  private final OSHDB oshdb;

  private final BoundingBox bbox = new BoundingBox(8, 9, 49, 50);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps(2010, 2015, 1, 12);

  private final double DELTA = 1e-8;

  public TestFlatMapAggregateGroupedById() throws Exception {
    oshdb = new OSHDB_H2("./src/test/resources/test-data");
  }
  private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb).osmTypes(OSMType.NODE).filterByTag("highway").areaOfInterest(bbox);
  }

  @Test
  public void test() throws Exception {
    SortedMap<Long, Integer> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .groupById()
        .flatMap(contributions -> {
            if (contributions.get(0).getEntityAfter().getId() != 617308093)
              return new ArrayList<>();
            List<Pair<Long, Integer>> ret = new ArrayList<>();
            ret.add(new ImmutablePair<>(
                contributions.get(0).getEntityAfter().getId(),
                (int)contributions.stream().filter(c -> c.getContributionTypes().contains(ContributionType.GEOMETRY_CHANGE)).count()
            ));
            ret.add(new ImmutablePair<>(
                contributions.get(0).getEntityAfter().getId(),
                2
            ));
            return ret;
        })
        .aggregate(Pair::getKey)
        .map(Pair::getValue)
        .reduce(
            () -> 0,
            (x,y) -> x + y,
            (x,y) -> x + y
        );

    assertEquals(1, result.entrySet().size());
    assertEquals(5+2, result.get(617308093L).intValue());
  }
}
