/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestMapAggregateByTimestamp {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8.651133,49.387611,8.6561,49.390513);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2014-01-01");
  private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps("2014-01-01", "2014-12-30");
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01", OSHDBTimestamps.Interval.MONTHLY);

  private final double DELTA = 1e-8;

  public TestMapAggregateByTimestamp() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data");
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb).osmType(OSMType.WAY).osmTag("building", "yes").areaOfInterest(bbox);
  }
  private MapReducer<OSMEntitySnapshot> createMapReducerOSMEntitySnapshot() throws Exception {
    return OSMEntitySnapshotView.on(oshdb).osmType(OSMType.WAY).osmTag("building", "yes").areaOfInterest(bbox);
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
  public void testOSMContributionCustomDefault() throws Exception {
    // check if it produces the same result as the automatic aggregateByTimestamp()
    SortedMap<OSHDBTimestamp, Integer> resultAuto = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .aggregateByTimestamp()
        .sum(contribution -> 1);
    SortedMap<OSHDBTimestamp, Integer> resultCustom = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .aggregateByTimestamp(OSMContribution::getTimestamp)
        .sum(contribution -> 1);

    assertEquals(resultAuto.entrySet().size(), resultCustom.entrySet().size());
    for (OSHDBTimestamp t : resultAuto.keySet()) {
      assertEquals(
          resultAuto.get(t).intValue(),
          resultCustom.get(t).intValue()
      );
    }
  }

  @Test
  public void testOSMContributionCustom() throws Exception {
    // most basic custom timestamp index possible -> map all to one single timestamp
    SortedMap<OSHDBTimestamp, Integer> resultCustom = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .aggregateByTimestamp(ignored -> timestamps1.get().first())
        .sum(snapshot -> 1);

    assertEquals(71, resultCustom.entrySet().size());
    assertEquals(1, resultCustom.entrySet().stream().filter(entry -> entry.getValue() > 0).count());
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

  @Test
  public void testOSMEntitySnapshotCustomDefault() throws Exception {
    // check if it produces the same result as the automatic aggregateByTimestamp()
    SortedMap<OSHDBTimestamp, Integer> resultAuto = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps72)
        .aggregateByTimestamp()
        .sum(snapshot -> 1);
    SortedMap<OSHDBTimestamp, Integer> resultCustom = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps72)
        .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
        .sum(snapshot -> 1);

    assertEquals(resultAuto.entrySet().size(), resultCustom.entrySet().size());
    for (OSHDBTimestamp t : resultAuto.keySet()) {
      assertEquals(
          resultAuto.get(t).intValue(),
          resultCustom.get(t).intValue()
      );
    }
  }

  @Test
  public void testOSMEntitySnapshotCustom() throws Exception {
    // most basic custom timestamp index possible -> map all to one single timestamp
    SortedMap<OSHDBTimestamp, Integer> resultCustom = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps72)
        .aggregateByTimestamp(ignored -> timestamps1.get().first())
        .sum(snapshot -> 1);

    assertEquals(72, resultCustom.entrySet().size());
    assertEquals(1, resultCustom.entrySet().stream().filter(entry -> entry.getValue() > 0).count());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testUnsupportedUsage() throws Exception {
    //noinspection ResultOfMethodCallIgnored – we test for a thrown exception here
    createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .groupByEntity()
        .aggregateByTimestamp()
        .collect();
  }

  @Test
  public void testMapperFunctions() throws Exception {
    // check if it produces the same result whether the map function was set before or after aggr.
    SortedMap<OSHDBTimestamp, Number> result1 = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .aggregateByTimestamp()
        .map(x -> 7)
        .sum();
    SortedMap<OSHDBTimestamp, Number> result2 = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .map(x -> 7)
        .aggregateByTimestamp()
        .sum();

    assertEquals(result1, result2);
    assertEquals(result1.entrySet().size(), result2.entrySet().size());
    for (OSHDBTimestamp t : result1.keySet()) {
      assertEquals(
          result1.get(t).intValue(),
          result2.get(t).intValue()
      );
    }
  }

  @Test
  public void testCombinedWithAggregateByIndex() throws Exception {
    SortedMap<OSHDBCombinedIndex<OSHDBTimestamp, OSMType>, Integer> result =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps1)
            .aggregateByTimestamp()
            .aggregateBy(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getType())
            .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
            .count();
    assertEquals(1, result.entrySet().size());
    assertEquals(timestamps1.get().first(), result.firstKey().getFirstIndex());
    assertEquals(OSMType.WAY, result.firstKey().getSecondIndex());
    assertEquals(42, (int)result.values().toArray(new Integer[] {})[0]);

    SortedMap<OSHDBTimestamp, SortedMap<OSMType, Integer>> nestedResult1 = OSHDBCombinedIndex.nest(result);
    assertEquals(42, (int)nestedResult1.get(timestamps1.get().first()).get(OSMType.WAY));
  }

  @Test
  public void testCombinedWithAggregateByIndexAuto() throws Exception {
    SortedMap<OSHDBCombinedIndex<OSHDBTimestamp, OSMType>, List<Long>> result =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps2)
            .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
            .aggregateBy(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getType())
            .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
            .collect();
    SortedMap<OSHDBCombinedIndex<OSHDBTimestamp, OSMType>, List<Long>> resultAuto =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps2)
            .aggregateByTimestamp()
            .aggregateBy(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getType())
            .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
            .collect();
    assertEquals(result, resultAuto);
  }

  @Test
  public void testCombinedWithAggregateByIndexOrder() throws Exception {
    SortedMap<OSHDBCombinedIndex<OSMType, OSHDBTimestamp>, List<Long>> resultIT =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps2)
            .aggregateBy(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getType())
            .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
            .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
            .collect();
    SortedMap<OSHDBCombinedIndex<OSHDBTimestamp, OSMType>, List<Long>> resultTI =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps2)
            .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
            .aggregateBy(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getType())
            .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
            .collect();
    assertEquals(resultIT.entrySet().size(), resultTI.entrySet().size());
    for (OSHDBCombinedIndex<OSMType, OSHDBTimestamp> idx : resultIT.keySet()) {
      assertEquals(
          resultIT.get(idx),
          resultTI.get(new OSHDBCombinedIndex<>(idx.getSecondIndex(), idx.getFirstIndex()))
      );
    }
  }
}
