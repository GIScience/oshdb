package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.ohsome.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBInvalidTimestampException;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * Test aggregateByTimestamp method of the OSHDB API.
 */
class MapAggregateByTimestampTest {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox =
      OSHDBBoundingBox.bboxWgs84Coordinates(8.651133, 49.387611, 8.6561, 49.390513);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2014-01-01");
  private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps("2014-01-01", "2014-12-30");
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  MapAggregateByTimestampTest() throws Exception {
    oshdb = new OSHDBH2("../data/test-data");
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView
        .on(oshdb)
        .areaOfInterest(bbox)
        .filter("type:way and building=yes");
  }

  private MapReducer<OSMEntitySnapshot> createMapReducerOSMEntitySnapshot() throws Exception {
    return OSMEntitySnapshotView
        .on(oshdb)
        .areaOfInterest(bbox)
        .filter("type:way and building=yes");
  }

  @Test
  void testOSMContribution() throws Exception {
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
    assertEquals(39, result2
        .entrySet()
        .stream()
        .map(Map.Entry::getValue)
        .reduce(-1, Math::max).intValue());
  }

  @Test
  void testOSMContributionCustomDefault() throws Exception {
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
  void testOSMContributionCustom() throws Exception {
    // most basic custom timestamp index possible -> map all to one single timestamp
    SortedMap<OSHDBTimestamp, Integer> resultCustom = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .aggregateByTimestamp(ignored -> timestamps1.get().first())
        .sum(snapshot -> 1);

    assertEquals(71, resultCustom.entrySet().size());
    assertEquals(1, resultCustom.entrySet().stream().filter(entry -> entry.getValue() > 0).count());
  }

  @Test
  void testOSMEntitySnapshot() throws Exception {
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
  void testOSMEntitySnapshotCustomDefault() throws Exception {
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
  void testOSMEntitySnapshotCustom() throws Exception {
    // most basic custom timestamp index possible -> map all to one single timestamp
    SortedMap<OSHDBTimestamp, Integer> resultCustom = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps72)
        .aggregateByTimestamp(ignored -> timestamps1.get().first())
        .sum(snapshot -> 1);

    assertEquals(72, resultCustom.entrySet().size());
    assertEquals(1, resultCustom.entrySet().stream().filter(entry -> entry.getValue() > 0).count());
  }

  @SuppressWarnings("ResultOfMethodCallIgnored") // we test for a thrown exception here
  @Test()
  void testInvalidUsage() throws Exception {
    assertThrows(OSHDBInvalidTimestampException.class, () -> {
      // indexing function returns a timestamp outside the requested time range -> should throw
      createMapReducerOSMContribution()
          .timestamps(timestamps2)
          .groupByEntity()
          .aggregateByTimestamp(ignored -> timestamps72.get().first())
          .collect();
    });
  }

  @SuppressWarnings("ResultOfMethodCallIgnored") // we test for a thrown exception here
  @Test()
  void testUnsupportedUsage() throws Exception {
    assertThrows(UnsupportedOperationException.class, () -> {
      createMapReducerOSMContribution()
          .timestamps(timestamps72)
          .groupByEntity()
          .aggregateByTimestamp()
          .collect();
    });
  }

  @Test
  void testMapperFunctions() throws Exception {
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
  void testCombinedWithAggregateByIndex() throws Exception {
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
    assertEquals(42, (int) result.values().toArray(new Integer[] {})[0]);

    SortedMap<OSHDBTimestamp, SortedMap<OSMType, Integer>> nestedResult1 = OSHDBCombinedIndex
        .nest(result);
    assertEquals(42, (int) nestedResult1.get(timestamps1.get().first()).get(OSMType.WAY));
  }

  @Test
  void testCombinedWithAggregateByIndexAuto() throws Exception {
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
  void testCombinedWithAggregateByIndexOrder() throws Exception {
    SortedMap<OSHDBCombinedIndex<OSMType, OSHDBTimestamp>, List<Long>> resultIndexTime =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps2)
            .aggregateBy(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getType())
            .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
            .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
            .collect();
    SortedMap<OSHDBCombinedIndex<OSHDBTimestamp, OSMType>, List<Long>> resultTimeIndex =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps2)
            .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
            .aggregateBy(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getType())
            .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
            .collect();
    assertEquals(resultIndexTime.entrySet().size(), resultTimeIndex.entrySet().size());
    for (OSHDBCombinedIndex<OSMType, OSHDBTimestamp> idx : resultIndexTime.keySet()) {
      assertEquals(
          resultIndexTime.get(idx),
          resultTimeIndex.get(new OSHDBCombinedIndex<>(idx.getSecondIndex(), idx.getFirstIndex()))
      );
    }
  }
}
