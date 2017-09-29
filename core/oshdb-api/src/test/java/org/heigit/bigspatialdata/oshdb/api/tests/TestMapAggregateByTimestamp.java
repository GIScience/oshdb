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
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestMapAggregateByTimestamp {
  private final OSHDB oshdb;

  private final BoundingBox bbox = new BoundingBox(8.651133,8.6561,49.387611,49.390513);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps(2014, 2014, 1, 1);
  private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps(2014, 2015, 1, 1);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps(2010, 2015, 1, 12);

  private final double DELTA = 1e-8;

  public TestMapAggregateByTimestamp() throws Exception {
    oshdb = new OSHDB_H2("./src/test/resources/test-data");
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb).osmTypes(OSMType.WAY).filterByTag("building", "yes").areaOfInterest(bbox);
  }
  private MapReducer<OSMEntitySnapshot> createMapReducerOSMEntitySnapshot() throws Exception {
    return OSMEntitySnapshotView.on(oshdb).osmTypes(OSMType.WAY).filterByTag("building", "yes").areaOfInterest(bbox);
  }

  @Test
  public void testOSMContribution() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Integer> result1 = createMapReducerOSMContribution()
        .timestamps(timestamps2)
        .aggregateByTimestamp()
        .sum(contribution -> 1);

    assertEquals(1, result1.entrySet().size());
    assertEquals(14, result1.get(result1.firstKey()).intValue());
    // multiple timestamps
    SortedMap<OSHDBTimestamp, Integer> result2 = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .aggregateByTimestamp()
        .sum(contribution -> 1);

    assertEquals(71, result2.entrySet().size());
    assertEquals(0, result2.get(result2.firstKey()).intValue());
    assertEquals(0, result2.get(result2.lastKey()).intValue());
    assertEquals(39, result2.entrySet().stream().map(Map.Entry::getValue).reduce(-1, Math::max).intValue());
  }

  @Test
  public void testOSMEntitySnapshot() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Integer> result1 = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps1)
        .aggregateByTimestamp()
        .sum(snapshot -> 1);

    assertEquals(1, result1.entrySet().size());
    assertEquals(42, result1.get(result1.firstKey()).intValue());
    // multiple timestamps
    SortedMap<OSHDBTimestamp, Integer> result2 = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps72)
        .aggregateByTimestamp()
        .sum(snapshot -> 1);

    assertEquals(72, result2.entrySet().size());
    assertEquals(0, result2.get(result2.firstKey()).intValue());
    assertEquals(42, result2.get(result2.lastKey()).intValue());
  }


}
