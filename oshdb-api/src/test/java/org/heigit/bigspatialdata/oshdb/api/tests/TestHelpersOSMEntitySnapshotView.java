/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.generic.WeightedValue;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;

/**
 *
 */
public class TestHelpersOSMEntitySnapshotView {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8.651133,49.387611,8.6561,49.390513);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2014-01-01");
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01", OSHDBTimestamps.Interval.MONTHLY);

  private final double DELTA = 1e-8;

  public TestHelpersOSMEntitySnapshotView() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data");
  }

  private MapReducer<OSMEntitySnapshot> createMapReducer() throws Exception {
    return OSMEntitySnapshotView.on(oshdb).osmType(OSMType.WAY).osmTag("building", "yes").areaOfInterest(bbox);
  }

  @Test
  public void testSum() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Number> result1 = this.createMapReducer()
        .timestamps(timestamps1)
        .aggregateByTimestamp()
        .sum(snapshot -> 1);

    assertEquals(1, result1.entrySet().size());
    assertEquals(42, result1.get(result1.firstKey()));

    // many timestamps
    SortedMap<OSHDBTimestamp, Number> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .osmType(OSMType.WAY)
        .osmTag("building", "yes")
        .aggregateByTimestamp()
        .sum(snapshot -> 1);

    assertEquals(72, result2.entrySet().size());
    assertEquals(0, result2.get(result2.firstKey()));
    assertEquals(42, result2.get(result2.lastKey()));

    // total
    Number result3 = this.createMapReducer()
        .timestamps(timestamps1)
        .osmType(OSMType.WAY)
        .osmTag("building", "yes")
        .sum(snapshot -> 1);

    assertEquals(42, result3);

    // custom aggregation identifier
    SortedMap<Boolean, Number> result4 = this.createMapReducer()
        .timestamps(timestamps1)
        .osmType(OSMType.WAY)
        .osmTag("building", "yes")
        .aggregateBy(snapshot -> snapshot.getEntity().getId() % 2 == 0)
        .sum(snapshot -> 1);

    assertEquals(21, result4.get(true));
    assertEquals(21, result4.get(false));
  }

  @Test
  public void testCount() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Integer> result1 = this.createMapReducer()
        .timestamps(timestamps1)
        .osmType(OSMType.WAY)
        .osmTag("building", "yes")
        .aggregateByTimestamp()
        .count();

    assertEquals(1, result1.entrySet().size());
    assertEquals(42, result1.get(result1.firstKey()).intValue());

    // many timestamps
    SortedMap<OSHDBTimestamp, Integer> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .osmType(OSMType.WAY)
        .osmTag("building", "yes")
        .aggregateByTimestamp()
        .count();

    assertEquals(72, result2.entrySet().size());
    assertEquals(0, result2.get(result2.firstKey()).intValue());
    assertEquals(42, result2.get(result2.lastKey()).intValue());

    // total
    Integer result3 = this.createMapReducer()
        .timestamps(timestamps1)
        .osmType(OSMType.WAY)
        .osmTag("building", "yes")
        .count();

    assertEquals(42, result3.intValue());

    // custom aggregation identifier
    SortedMap<Boolean, Integer> result4 = this.createMapReducer()
        .timestamps(timestamps1)
        .osmType(OSMType.WAY)
        .osmTag("building", "yes")
        .aggregateBy(snapshot -> snapshot.getEntity().getId() % 2 == 0)
        .count();

    assertEquals(21, result4.get(true).intValue());
    assertEquals(21, result4.get(false).intValue());
  }

  @Test
  public void testAverage() throws Exception {
    // single timestamp
    Double result1 = this.createMapReducer()
        .timestamps(timestamps1)
        .osmType(OSMType.WAY)
        .osmTag("building", "yes")
        .map(snapshot -> snapshot.getEntity().getId() % 2)
        .average();

    assertEquals(0.5, result1.doubleValue(), DELTA);

    // many timestamps
    SortedMap<OSHDBTimestamp, Double> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .osmType(OSMType.WAY)
        .osmTag("building", "yes")
        .aggregateByTimestamp()
        .map(snapshot -> snapshot.getEntity().getId() % 2)
        .average();

    assertEquals(72, result2.entrySet().size());
    assertEquals(Double.NaN, result2.get(result2.firstKey()), DELTA);
    assertEquals(0.5, result2.get(result2.lastKey()).doubleValue(), DELTA);

    // custom aggregation identifier
    SortedMap<Boolean, Double> result4 = this.createMapReducer()
        .timestamps(timestamps1)
        .osmType(OSMType.WAY)
        .osmTag("building", "yes")
        .aggregateBy(snapshot -> snapshot.getEntity().getId() % 2 == 0)
        .average(snapshot -> snapshot.getEntity().getId() % 2);

    assertEquals(0.0, result4.get(true).doubleValue(), DELTA);
    assertEquals(1.0, result4.get(false).doubleValue(), DELTA);
  }

  @Test
  public void testWeightedAverage() throws Exception {
    // single timestamp
    Double result1 = this.createMapReducer()
        .timestamps(timestamps1)
        .osmType(OSMType.WAY)
        .osmTag("building", "yes")
        .weightedAverage(snapshot -> new WeightedValue<>(snapshot.getEntity().getId() % 2,1 * (snapshot.getEntity().getId() % 2)));

    assertEquals(1.0, result1.doubleValue(), DELTA);

    // many timestamps
    SortedMap<OSHDBTimestamp, Double> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .osmType(OSMType.WAY)
        .osmTag("building", "yes")
        .aggregateByTimestamp()
        .weightedAverage(snapshot -> new WeightedValue<>(snapshot.getEntity().getId() % 2,2 * (snapshot.getEntity().getId() % 2)));

    assertEquals(72, result2.entrySet().size());
    assertEquals(Double.NaN, result2.get(result2.firstKey()), DELTA);
    assertEquals(1.0, result2.get(result2.lastKey()).doubleValue(), DELTA);

    // custom aggregation identifier
    SortedMap<Boolean, Double> result4 = this.createMapReducer()
        .timestamps(timestamps1)
        .osmType(OSMType.WAY)
        .osmTag("building", "yes")
        .aggregateBy(snapshot -> snapshot.getEntity().getId() % 2 == 0)
        .weightedAverage(snapshot -> new WeightedValue<>(snapshot.getEntity().getId() % 2, 2 * (snapshot.getEntity().getId() % 2)));

    assertEquals(Double.NaN, result4.get(true).doubleValue(), DELTA);
    assertEquals(1.0, result4.get(false).doubleValue(), DELTA);
  }

  @Test
  public void testUniq() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Set<Long>> result1 = this.createMapReducer()
        .timestamps(timestamps1)
        .osmType(OSMType.WAY)
        .osmTag("building", "yes")
        .aggregateByTimestamp()
        .uniq(snapshot -> snapshot.getEntity().getId());

    assertEquals(1, result1.entrySet().size());
    assertEquals(42, result1.get(result1.firstKey()).size());

    // many timestamps
    SortedMap<OSHDBTimestamp, Set<Long>> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .osmType(OSMType.WAY)
        .osmTag("building", "yes")
        .aggregateByTimestamp()
        .uniq(snapshot -> snapshot.getEntity().getId());

    assertEquals(72, result2.entrySet().size());
    assertEquals(0, result2.get(result2.firstKey()).size());
    assertEquals(42, result2.get(result2.lastKey()).size());

    // total
    Set<Long> result3 = this.createMapReducer()
        .timestamps(timestamps1)
        .osmType(OSMType.WAY)
        .osmTag("building", "yes")
        .uniq(snapshot -> snapshot.getEntity().getId());

    assertEquals(42, result3.size());

    // custom aggregation identifier
    SortedMap<Boolean, Set<Long>> result4 = this.createMapReducer()
        .timestamps(timestamps1)
        .osmType(OSMType.WAY)
        .osmTag("building", "yes")
        .aggregateBy(snapshot -> snapshot.getEntity().getId() % 2 == 0)
        .uniq(snapshot -> snapshot.getEntity().getId());

    assertEquals(21, result4.get(true).size());
    assertEquals(21, result4.get(false).size());
  }

}
