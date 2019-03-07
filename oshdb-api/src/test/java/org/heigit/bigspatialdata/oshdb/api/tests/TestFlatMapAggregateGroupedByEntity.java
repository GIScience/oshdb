/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.api.tests;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.celliterator.ContributionType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestFlatMapAggregateGroupedByEntity {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8, 49, 9, 50);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01", OSHDBTimestamps.Interval.MONTHLY);

  private final double DELTA = 1e-8;

  public TestFlatMapAggregateGroupedByEntity() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data");
  }
  private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb).osmType(OSMType.NODE).osmTag("highway").areaOfInterest(bbox);
  }

  @Test
  public void test() throws Exception {
    SortedMap<Long, Integer> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .groupByEntity()
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
        .aggregateBy(Pair::getKey)
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
