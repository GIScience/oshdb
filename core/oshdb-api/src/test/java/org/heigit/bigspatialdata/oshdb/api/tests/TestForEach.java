/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_Implementation;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.utils.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestForEach {
  private final OSHDB_Implementation oshdb;

  private final BoundingBox bbox = new BoundingBox(8.651133,8.6561,49.387611,49.390513);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01", OSHDBTimestamps.Interval.MONTHLY);

  public TestForEach() throws Exception {
    oshdb = new OSHDB_H2("./src/test/resources/test-data");
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb).osmTypes(OSMType.WAY).where("building", "yes").areaOfInterest(bbox);
  }

  @Test
  public void testForEach() throws Exception {
    ConcurrentHashMap<Long, Boolean> result = new ConcurrentHashMap<>();
    this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .forEach(contribution -> {
          result.put(contribution.getEntityAfter().getId(), true);
        });
    assertEquals(42, result.entrySet().size());
  }

  @Test
  public void testForEachGroupedById() throws Exception {
    ConcurrentHashMap<Long, Boolean> result = new ConcurrentHashMap<>();
    this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .groupById()
        .forEach(contributions -> {
          contributions.forEach(contribution -> {
            result.put(contribution.getEntityAfter().getId(), true);
          });
        });
    assertEquals(42, result.entrySet().size());
  }

  @Test
  public void testForEachAggregatedByTimestamp() throws Exception {
    ConcurrentHashMap<Long, Boolean> result = new ConcurrentHashMap<>();
    this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .aggregateByTimestamp()
        .forEach((ts, contributions) -> {
          contributions.forEach(contribution -> {
            result.put(contribution.getEntityAfter().getId(), true);
          });
        });
    assertEquals(42, result.entrySet().size());
  }
}
