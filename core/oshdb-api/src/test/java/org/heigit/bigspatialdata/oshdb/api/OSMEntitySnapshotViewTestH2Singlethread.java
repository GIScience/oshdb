/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.api;

import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;
import java.util.SortedMap;
import java.util.logging.Logger;
import static org.junit.Assert.assertEquals;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.json.simple.parser.ParseException;

/**
 *
 */
public class OSMEntitySnapshotViewTestH2Singlethread {

  private static final Logger LOG = Logger.getLogger(OSMEntitySnapshotViewTestH2Singlethread.class.getName());

  MapReducer<OSMEntitySnapshot> mapReducer;

  private final BoundingBox bbox1 = new BoundingBox(8.651133,8.6561,49.387611,49.390513);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps(2014, 2014, 1, 1);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps(2010, 2015, 1, 12);

  private final double DELTA = 1e-8;

  public OSMEntitySnapshotViewTestH2Singlethread() throws SQLException, ClassNotFoundException, IOException, ParseException {
    OSHDB_H2 oshdb = new OSHDB_H2("./src/test/resources/hd");
    oshdb.multithreading(false);
    OSHDB_H2 keytables = new OSHDB_H2("./src/test/resources/keytables");

    mapReducer = OSMEntitySnapshotView.on(oshdb).keytables(keytables);
    //mapReducer.tagInterpreter(DefaultTagInterpreter.fromJDBC(keytables.getConnection()));
  }

  @Test
  public void testSumBBox() throws Exception {
    // set area of interest
    mapReducer.areaOfInterest(bbox1);

    // single timestamp
    SortedMap<OSHDBTimestamp, Number> result1 = mapReducer
        .timestamps(timestamps1)
        .osmTypes(OSMType.WAY)
        .filterByTagValue("building", "yes")
        .sumAggregateByTimestamp(snapshot -> 1);

    assertEquals(1, result1.entrySet().size());
    assertEquals(42, result1.get(result1.firstKey()));

    // many timestamps
    SortedMap<OSHDBTimestamp, Number> result2 = mapReducer
        .timestamps(timestamps72)
        .osmTypes(OSMType.WAY)
        .filterByTagValue("building", "yes")
        .sumAggregateByTimestamp(snapshot -> 1);

    assertEquals(72, result2.entrySet().size());
    assertEquals(0, result2.get(result2.firstKey()));
    assertEquals(42, result2.get(result2.lastKey()));

    // total
    Number result3 = mapReducer
        .timestamps(timestamps1)
        .osmTypes(OSMType.WAY)
        .filterByTagValue("building", "yes")
        .sum(snapshot -> 1);

    assertEquals(42, result3);

    // custom aggregation identifier
    SortedMap<Boolean, Number> result4 = mapReducer
        .timestamps(timestamps1)
        .osmTypes(OSMType.WAY)
        .filterByTagValue("building", "yes")
        .sumAggregate(snapshot -> new ImmutablePair<>(snapshot.getEntity().getId() % 2 == 0, 1));

    assertEquals(21, result4.get(true));
    assertEquals(21, result4.get(false));
  }

  @Test
  public void testCountBBox() throws Exception {
    // set area of interest
    mapReducer.areaOfInterest(bbox1);

    // single timestamp
    SortedMap<OSHDBTimestamp, Integer> result1 = mapReducer
        .timestamps(timestamps1)
        .osmTypes(OSMType.WAY)
        .filterByTagValue("building", "yes")
        .countAggregateByTimestamp();

    assertEquals(1, result1.entrySet().size());
    assertEquals(42, result1.get(result1.firstKey()).intValue());

    // many timestamps
    SortedMap<OSHDBTimestamp, Integer> result2 = mapReducer
        .timestamps(timestamps72)
        .osmTypes(OSMType.WAY)
        .filterByTagValue("building", "yes")
        .countAggregateByTimestamp();

    assertEquals(72, result2.entrySet().size());
    assertEquals(0, result2.get(result2.firstKey()).intValue());
    assertEquals(42, result2.get(result2.lastKey()).intValue());

    // total
    Integer result3 = mapReducer
        .timestamps(timestamps1)
        .osmTypes(OSMType.WAY)
        .filterByTagValue("building", "yes")
        .count();

    assertEquals(42, result3.intValue());

    // custom aggregation identifier
    SortedMap<Boolean, Integer> result4 = mapReducer
        .timestamps(timestamps1)
        .osmTypes(OSMType.WAY)
        .filterByTagValue("building", "yes")
        .countAggregate(snapshot -> snapshot.getEntity().getId() % 2 == 0);

    assertEquals(21, result4.get(true).intValue());
    assertEquals(21, result4.get(false).intValue());
  }

  @Test
  public void testAverageBBox() throws Exception {
    // set area of interest
    mapReducer.areaOfInterest(bbox1);

    // single timestamp
    SortedMap<OSHDBTimestamp, Double> result1 = mapReducer
        .timestamps(timestamps1)
        .osmTypes(OSMType.WAY)
        .filterByTagValue("building", "yes")
        .averageAggregateByTimestamp(snapshot -> snapshot.getEntity().getId() % 2);

    assertEquals(1, result1.entrySet().size());
    assertEquals(0.5, result1.get(result1.firstKey()).doubleValue(), DELTA);

    // many timestamps
    SortedMap<OSHDBTimestamp, Double> result2 = mapReducer
        .timestamps(timestamps72)
        .osmTypes(OSMType.WAY)
        .filterByTagValue("building", "yes")
        .averageAggregateByTimestamp(snapshot -> snapshot.getEntity().getId() % 2);

    assertEquals(72, result2.entrySet().size());
    assertEquals(Double.NaN, result2.get(result2.firstKey()), DELTA);
    assertEquals(0.5, result2.get(result2.lastKey()).doubleValue(), DELTA);

    // custom aggregation identifier
    SortedMap<Boolean, Double> result4 = mapReducer
        .timestamps(timestamps1)
        .osmTypes(OSMType.WAY)
        .filterByTagValue("building", "yes")
        .averageAggregate(snapshot -> new ImmutablePair<>(snapshot.getEntity().getId() % 2 == 0, snapshot.getEntity().getId() % 2));

    assertEquals(0.0, result4.get(true).doubleValue(), DELTA);
    assertEquals(1.0, result4.get(false).doubleValue(), DELTA);
  }

  @Test
  public void testWeightedAverageBBox() throws Exception {
    // set area of interest
    mapReducer.areaOfInterest(bbox1);

    // single timestamp
    SortedMap<OSHDBTimestamp, Double> result1 = mapReducer
        .timestamps(timestamps1)
        .osmTypes(OSMType.WAY)
        .filterByTagValue("building", "yes")
        .weightedAverageAggregateByTimestamp(snapshot -> new MapReducer.WeightedValue<>(snapshot.getEntity().getId() % 2,snapshot.getEntity().getId() % 2));

    assertEquals(1, result1.entrySet().size());
    assertEquals(1.0, result1.get(result1.firstKey()).doubleValue(), DELTA);

    // many timestamps
    SortedMap<OSHDBTimestamp, Double> result2 = mapReducer
        .timestamps(timestamps72)
        .osmTypes(OSMType.WAY)
        .filterByTagValue("building", "yes")
        .weightedAverageAggregateByTimestamp(snapshot -> new MapReducer.WeightedValue<>(snapshot.getEntity().getId() % 2,snapshot.getEntity().getId() % 2));

    assertEquals(72, result2.entrySet().size());
    assertEquals(Double.NaN, result2.get(result2.firstKey()), DELTA);
    assertEquals(1.0, result2.get(result2.lastKey()).doubleValue(), DELTA);

    // custom aggregation identifier
    SortedMap<Boolean, Double> result4 = mapReducer
        .timestamps(timestamps1)
        .osmTypes(OSMType.WAY)
        .filterByTagValue("building", "yes")
        .weightedAverageAggregate(snapshot -> new ImmutablePair<>(snapshot.getEntity().getId() % 2 == 0, new MapReducer.WeightedValue<>(snapshot.getEntity().getId() % 2, snapshot.getEntity().getId() % 2)));

    assertEquals(Double.NaN, result4.get(true).doubleValue(), DELTA);
    assertEquals(1.0, result4.get(false).doubleValue(), DELTA);
  }

  @Test
  public void testUniqBBox() throws Exception {
    // set area of interest
    mapReducer.areaOfInterest(bbox1);

    // single timestamp
    SortedMap<OSHDBTimestamp, Set<Long>> result1 = mapReducer
        .timestamps(timestamps1)
        .osmTypes(OSMType.WAY)
        .filterByTagValue("building", "yes")
        .uniqAggregateByTimestamp(snapshot -> snapshot.getEntity().getId());

    assertEquals(1, result1.entrySet().size());
    assertEquals(42, result1.get(result1.firstKey()).size());

    // many timestamps
    SortedMap<OSHDBTimestamp, Set<Long>> result2 = mapReducer
        .timestamps(timestamps72)
        .osmTypes(OSMType.WAY)
        .filterByTagValue("building", "yes")
        .uniqAggregateByTimestamp(snapshot -> snapshot.getEntity().getId());

    assertEquals(72, result2.entrySet().size());
    assertEquals(0, result2.get(result2.firstKey()).size());
    assertEquals(42, result2.get(result2.lastKey()).size());

    // total
    Set<Long> result3 = mapReducer
        .timestamps(timestamps1)
        .osmTypes(OSMType.WAY)
        .filterByTagValue("building", "yes")
        .uniq(snapshot -> snapshot.getEntity().getId());

    assertEquals(42, result3.size());

    // custom aggregation identifier
    SortedMap<Boolean, Set<Long>> result4 = mapReducer
        .timestamps(timestamps1)
        .osmTypes(OSMType.WAY)
        .filterByTagValue("building", "yes")
        .uniqAggregate(snapshot -> new ImmutablePair<>(snapshot.getEntity().getId() % 2 == 0, snapshot.getEntity().getId()));

    assertEquals(21, result4.get(true).size());
    assertEquals(21, result4.get(false).size());
  }

}
