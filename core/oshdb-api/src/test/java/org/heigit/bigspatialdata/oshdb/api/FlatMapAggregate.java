/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.api;

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

import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class FlatMapAggregate {
  private final OSHDB oshdb;

  private final BoundingBox bbox = new BoundingBox(8, 9, 49, 50);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps(2010, 2015, 1, 12);

  private final double DELTA = 1e-8;

  public FlatMapAggregate() throws Exception {
    oshdb = new OSHDB_H2("./src/test/resources/test-data;ACCESS_MODE_DATA=r");
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb).osmTypes(OSMType.NODE).filterByTagKey("highway").areaOfInterest(bbox);
  }

  @Test
  public void test() throws Exception {
    SortedMap<Long, Set<Pair<Integer, Integer>>> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .flatMapAggregate(
            contribution -> {
              if (contribution.getEntityAfter().getId() != 617308093)
                return new ArrayList<>();
              List<Pair<Long, Pair<Integer, Integer>>> ret = new ArrayList<>();
              int[] tags = contribution.getEntityAfter().getTags();
              for (int i=0; i<tags.length; i+=2)
                ret.add(new ImmutablePair<>(
                    contribution.getEntityAfter().getId(),
                    new ImmutablePair<>(tags[i], tags[i+1])
                ));
              return ret;
            },
            HashSet::new,
            (x,y) -> { x.add(y); return x; },
            (x,y) -> { Set<Pair<Integer, Integer>> ret = new HashSet<>(x); ret.addAll(y); return ret; }
        );

    assertEquals(1, result.entrySet().size());
    assertEquals(2, result.get(617308093L).size());
  }
}
