/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class Collect {
  private final OSHDB oshdb;

  private final BoundingBox bbox = new BoundingBox(8.651133,8.6561,49.387611,49.390513);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps(2010, 2015, 1, 12);

  public Collect() throws Exception {
    oshdb = new OSHDB_H2("./src/test/resources/test-data;ACCESS_MODE_DATA=r");
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb).osmTypes(OSMType.WAY).filterByTag("building", "yes").areaOfInterest(bbox);
  }

  @Test
  public void testCollect() throws Exception {
    List<OSMContribution> result = this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .collect();
    assertEquals(42, result.stream().map(contribution -> contribution.getEntityAfter().getId()).collect(Collectors.toSet()).size());
  }

  @Test
  public void testMapCollect() throws Exception {
    List<Long> result = this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .map(contribution -> contribution.getEntityAfter().getId())
        .collect();
    assertEquals(42, result.stream().collect(Collectors.toSet()).size());
  }

  @Test
  public void testFlatMapCollect() throws Exception {
    List<Long> result = this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .flatMap(contribution -> Collections.singletonList(contribution.getEntityAfter().getId()))
        .collect();
    assertEquals(42, result.stream().collect(Collectors.toSet()).size());
  }

  @Test
  public void testFlatMapCollectGroupedById() throws Exception {
    List<Long> result = this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .groupById()
        .flatMap(contributions -> Collections.singletonList(contributions.get(0).getEntityAfter().getId()))
        .collect();
    assertEquals(42, result.stream().collect(Collectors.toSet()).size());
  }

  @Test
  public void testAggregatedByTimestamp() throws Exception {
    SortedMap<OSHDBTimestamp, List<Long>> result = this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .aggregateByTimestamp()
        .map(contribution -> contribution.getEntityAfter().getId())
        .collect();
    assertEquals(14, result.get(timestamps72.getOSHDBTimestamps().get(60)).size());
  }

}
