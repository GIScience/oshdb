/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_Database;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestMapAggregateByTimestamp {
  private final OSHDB_Database oshdb;

  private final BoundingBox bbox = new BoundingBox(8.651133,8.6561,49.387611,49.390513);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2014-01-01");
  private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps("2014-01-01", "2014-12-30");
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01", OSHDBTimestamps.Interval.MONTHLY);

  private final double DELTA = 1e-8;

  public TestMapAggregateByTimestamp() throws Exception {
    oshdb = new OSHDB_H2("./src/test/resources/test-data");
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb).osmTypes(OSMType.WAY).where("building", "yes").areaOfInterest(bbox);
  }
  private MapReducer<OSMEntitySnapshot> createMapReducerOSMEntitySnapshot() throws Exception {
    return OSMEntitySnapshotView.on(oshdb).osmTypes(OSMType.WAY).where("building", "yes").areaOfInterest(bbox);
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

    // two timestamps
    SortedMap<OSHDBTimestamp, Integer> result2 = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps2)
        .aggregateByTimestamp()
        .sum(snapshot -> 1);

    assertEquals(2, result2.entrySet().size());

    // multiple timestamps
    SortedMap<OSHDBTimestamp, Integer> result72 = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps72)
        .aggregateByTimestamp()
        .sum(snapshot -> 1);

    assertEquals(72, result72.entrySet().size());
    assertEquals(0, result72.get(result72.firstKey()).intValue());
    assertEquals(42, result72.get(result72.lastKey()).intValue());
  }


}
