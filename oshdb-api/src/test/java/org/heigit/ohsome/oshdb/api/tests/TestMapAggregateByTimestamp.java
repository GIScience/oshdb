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
import org.heigit.ohsome.oshdb.api.mapreducer.CombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.contribution.OSMContributionView;
import org.heigit.ohsome.oshdb.api.mapreducer.reduction.Collector;
import org.heigit.ohsome.oshdb.api.mapreducer.reduction.Reduce;
import org.heigit.ohsome.oshdb.api.mapreducer.snapshot.OSMEntitySnapshotView;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBInvalidTimestampException;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * Test aggregateByTimestamp method of the OSHDB API.
 */
class TestMapAggregateByTimestamp {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox =
      OSHDBBoundingBox.bboxWgs84Coordinates(8.651133, 49.387611, 8.6561, 49.390513);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2014-01-01");
  private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps("2014-01-01", "2014-12-30");
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  TestMapAggregateByTimestamp() throws Exception {
    oshdb = new OSHDBH2("../data/test-data");
  }

  private OSMContributionView createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.view()
        .areaOfInterest(bbox)
        .filter("type:way and building=yes");
  }

  private OSMEntitySnapshotView createMapReducerOSMEntitySnapshot() throws Exception {
    return OSMEntitySnapshotView.view()
        .areaOfInterest(bbox)
        .filter("type:way and building=yes");
  }

  @Test
  void testOSMContribution() throws Exception {
    // single timestamp
    Map<OSHDBTimestamp, Long> result1 = createMapReducerOSMContribution()
        .timestamps(timestamps2)
        .on(oshdb)
        .aggregateByTimestamp()
        .map(contribution -> 1)
        .reduce(Reduce::sumInt);

    assertEquals(1, result1.entrySet().size());
    assertEquals(14, result1.get(timestamps2.get().first()).intValue());

    // multiple timestamps
    Map<OSHDBTimestamp, Long> result2 = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateByTimestamp()
        .map(contribution -> 1)
        .reduce(Reduce::sumInt);

    assertEquals(5, result2.entrySet().size());
    assertEquals(null, result2.get(timestamps72.get().first()));
    assertEquals(null, result2.get(timestamps72.get().last()));
    assertEquals(39, result2
        .entrySet()
        .stream()
        .map(Map.Entry::getValue)
        .reduce(-1L, Math::max).intValue());
  }

  @Test
  void testOSMContributionCustomDefault() throws Exception {
    // check if it produces the same result as the automatic aggregateByTimestamp()
    Map<OSHDBTimestamp, Long> resultAuto = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateByTimestamp()
        .map(contribution -> 1)
        .reduce(Reduce::sumInt);
    Map<OSHDBTimestamp, Long> resultCustom = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateByTimestamp(OSMContribution::getTimestamp)
        .map(contribution -> 1)
        .reduce(Reduce::sumInt);;

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
    Map<OSHDBTimestamp, Long> resultCustom = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateByTimestamp(ignored -> timestamps1.get().first())
        .map(snapshot -> 1)
        .reduce(Reduce::sumInt);

    assertEquals(1, resultCustom.entrySet().size());
    assertEquals(1, resultCustom.entrySet().stream().filter(entry -> entry.getValue() > 0).count());
  }

  @Test
  void testOSMEntitySnapshot() throws Exception {
    // single timestamp
    Map<OSHDBTimestamp, Long> result1 = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps1)
        .on(oshdb)
        .aggregateByTimestamp()
        .map(snapshot -> 1)
        .reduce(Reduce::sumInt);

    assertEquals(1, result1.entrySet().size());
    assertEquals(42, result1.get(timestamps1.get().first()).intValue());

    // two timestamps
    Map<OSHDBTimestamp, Long> result2 = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps2)
        .on(oshdb)
        .aggregateByTimestamp()
        .map(snapshot -> 1)
        .reduce(Reduce::sumInt);

    assertEquals(2, result2.entrySet().size());

    // multiple timestamps
    Map<OSHDBTimestamp, Long> result72 = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateByTimestamp()
        .map(snapshot -> 1)
        .reduce(Reduce::sumInt);

    assertEquals(51, result72.entrySet().size());
    assertEquals(null, result72.get(timestamps72.get().first()));
    assertEquals(42, result72.get(timestamps72.get().last()).intValue());
  }

  @Test
  void testOSMEntitySnapshotCustomDefault() throws Exception {
    // check if it produces the same result as the automatic aggregateByTimestamp()
    Map<OSHDBTimestamp, Long> resultAuto = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateByTimestamp()
        .map(snapshot -> 1)
        .reduce(Reduce::sumInt);
    Map<OSHDBTimestamp, Long> resultCustom = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
        .map(snapshot -> 1)
        .reduce(Reduce::sumInt);

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
    Map<OSHDBTimestamp, Long> resultCustom = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateByTimestamp(ignored -> timestamps1.get().first())
        .map(snapshot -> 1)
        .reduce(Reduce::sumInt);

    assertEquals(1, resultCustom.entrySet().size());
    assertEquals(1, resultCustom.entrySet().stream().filter(entry -> entry.getValue() > 0).count());
  }

//  @SuppressWarnings("ResultOfMethodCallIgnored") // we test for a thrown exception here
//  @Test()
//  void testInvalidUsage() throws Exception {
//    assertThrows(OSHDBInvalidTimestampException.class, () ->
//      // indexing function returns a timestamp outside the requested time range -> should throw
//      createMapReducerOSMContribution()
//          .timestamps(timestamps2)
//          .on(oshdb)
//          .groupByEntity()
//          .aggregateByTimestamp(ignored -> timestamps72.get().first())
//          .reduce(Collector::toList));
//  }

//  @SuppressWarnings("ResultOfMethodCallIgnored") // we test for a thrown exception here
//  @Test()
//  void testUnsupportedUsage() throws Exception {
//    assertThrows(UnsupportedOperationException.class, () -> {
//      createMapReducerOSMContribution()
//          .timestamps(timestamps72)
//          .groupByEntity()
//          .aggregateByTimestamp()
//          .collect();
//    });
//  }

//  @Test
//  void testMapperFunctions() throws Exception {
//    // check if it produces the same result whether the map function was set before or after aggr.
//    Map<OSHDBTimestamp, Long> result1 = createMapReducerOSMContribution()
//        .timestamps(timestamps72)
//        .on(oshdb)
//        .aggregateByTimestamp()
//        .map(x -> 7)
//        .reduce(Reduce::sumInt);
//    Map<OSHDBTimestamp, Long> result2 = createMapReducerOSMContribution()
//        .timestamps(timestamps72)
//        .on(oshdb)
//        .map(x -> 7)
//        .aggregateByTimestamp()
//        .sum();
//
//    assertEquals(result1, result2);
//    assertEquals(result1.entrySet().size(), result2.entrySet().size());
//    for (OSHDBTimestamp t : result1.keySet()) {
//      assertEquals(
//          result1.get(t).intValue(),
//          result2.get(t).intValue()
//      );
//    }
//  }

  @Test
  void testCombinedWithAggregateByIndex() throws Exception {
    Map<CombinedIndex<OSHDBTimestamp, OSMType>, Long> result =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps1)
            .on(oshdb)
            .aggregateByTimestamp()
            .aggregateBy(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getType())
            .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
            .count();
    assertEquals(1, result.entrySet().size());
    assertEquals(timestamps1.get().first(), result.keySet().iterator().next().u());
    assertEquals(OSMType.WAY, result.keySet().iterator().next().v());
    assertEquals(42L, result.values().toArray(Long[]::new)[0]);

    Map<OSHDBTimestamp, Map<OSMType, Long>> nestedResult1 = CombinedIndex.nest(result);
    assertEquals(42L, nestedResult1.get(timestamps1.get().first()).get(OSMType.WAY));
  }

  @Test
  void testCombinedWithAggregateByIndexAuto() throws Exception {
    Map<CombinedIndex<OSHDBTimestamp, OSMType>, List<Long>> result =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps2)
            .on(oshdb)
            .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
            .aggregateBy(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getType())
            .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
            .collect(Collector::toList);
    Map<CombinedIndex<OSHDBTimestamp, OSMType>, List<Long>> resultAuto =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps2)
            .on(oshdb)
            .aggregateByTimestamp()
            .aggregateBy(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getType())
            .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
            .collect(Collector::toList);
    assertEquals(result, resultAuto);
  }

  @Test
  void testCombinedWithAggregateByIndexOrder() throws Exception {
    Map<CombinedIndex<OSMType, OSHDBTimestamp>, List<Long>> resultIndexTime =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps2)
            .on(oshdb)
            .aggregateBy(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getType())
            .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
            .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
            .collect(Collector::toList);
    Map<CombinedIndex<OSHDBTimestamp, OSMType>, List<Long>> resultTimeIndex =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps2)
            .on(oshdb)
            .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
            .aggregateBy(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getType())
            .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
            .collect(Collector::toList);
    assertEquals(resultIndexTime.entrySet().size(), resultTimeIndex.entrySet().size());
    for (CombinedIndex<OSMType, OSHDBTimestamp> idx : resultIndexTime.keySet()) {
      assertEquals(
          resultIndexTime.get(idx),
          resultTimeIndex.get(new CombinedIndex<>(idx.v(), idx.u()))
      );
    }
  }
}
