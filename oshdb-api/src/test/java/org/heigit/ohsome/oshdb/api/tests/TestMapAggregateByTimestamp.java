package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.aggregation.Agg;
import org.heigit.ohsome.oshdb.api.mapreducer.contribution.OSMContributionView;
import org.heigit.ohsome.oshdb.api.mapreducer.snapshot.OSMEntitySnapshotView;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * Test aggregateByTimestamp method of the OSHDB API.
 */
class TestMapAggregateByTimestamp {
  private final OSHDBH2 oshdb;

  private final OSHDBBoundingBox bbox =
      OSHDBBoundingBox.bboxWgs84Coordinates(8.651133, 49.387611, 8.6561, 49.390513);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2014-01-01");
  private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps("2014-01-01", "2014-12-30");
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  TestMapAggregateByTimestamp() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data");
  }

  private OSHDBView<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb)
        .areaOfInterest(bbox)
        .filter("type:way and building=yes");
  }

  private OSHDBView<OSMEntitySnapshot> createMapReducerOSMEntitySnapshot() throws Exception {
    return OSMEntitySnapshotView.on(oshdb)
        .areaOfInterest(bbox)
        .filter("type:way and building=yes");
  }

  @Test
  void testOSMContribution() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Long> result1 = createMapReducerOSMContribution()
        .timestamps(timestamps2)
        .view()
        .aggregateByTimestamp()
        .map(contribution -> 1)
        .aggregate(Agg::sumInt);


    assertEquals(1, result1.entrySet().size());
    assertEquals(14, result1.get(result1.firstKey()).intValue());

    // multiple timestamps
    SortedMap<OSHDBTimestamp, Long> result2 = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .view()
        .aggregateByTimestamp()
        .map(contribution -> 1)
        .aggregate(Agg::sumInt);;

    assertEquals(5, result2.entrySet().size());
    assertEquals(1, result2.get(result2.firstKey()).intValue());
    assertEquals(14, result2.get(result2.lastKey()).intValue());
    assertEquals(39, result2
        .entrySet()
        .stream()
        .map(Map.Entry::getValue)
        .reduce(-1L, Math::max).intValue());
  }

  @Test
  void testOSMContributionCustomDefault() throws Exception {
    // check if it produces the same result as the automatic aggregateByTimestamp()
    SortedMap<OSHDBTimestamp, Long> resultAuto = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .view()
        .aggregateByTimestamp()
        .map(contribution -> 1)
        .aggregate(Agg::sumInt);
    SortedMap<OSHDBTimestamp, Long> resultCustom = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .view()
        .aggregateByTimestamp(OSMContribution::getTimestamp)
        .map(contribution -> 1)
        .aggregate(Agg::sumInt);

    assertEquals(resultAuto.entrySet().size(), resultCustom.entrySet().size());
    for (OSHDBTimestamp t : resultAuto.keySet()) {
      assertEquals(
          resultAuto.get(t).intValue(),
          resultCustom.get(t).intValue()
      );
    }
  }

  @Test
  void testOSMEntitySnapshot() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Long> result1 = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps1)
        .view()
        .aggregateByTimestamp()
        .map(snapshot -> 1)
        .aggregate(Agg::sumInt);

    assertEquals(1, result1.entrySet().size());
    assertEquals(42, result1.get(result1.firstKey()).intValue());

    // two timestamps
    SortedMap<OSHDBTimestamp, Long> result2 = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps2)
        .view()
        .aggregateByTimestamp()
        .map(snapshot -> 1)
        .aggregate(Agg::sumInt);

    assertEquals(2, result2.entrySet().size());

    // multiple timestamps
    SortedMap<OSHDBTimestamp, Long> result72 = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps72)
        .view()
        .aggregateByTimestamp()
        .map(snapshot -> 1)
        .aggregate(Agg::sumInt);

    assertEquals(51, result72.entrySet().size());
    assertEquals(1, result72.get(result72.firstKey()).intValue());
    assertEquals(42, result72.get(result72.lastKey()).intValue());
  }

  @Test
  void testOSMEntitySnapshotCustomDefault() throws Exception {
    // check if it produces the same result as the automatic aggregateByTimestamp()
    SortedMap<OSHDBTimestamp, Long> resultAuto = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps72)
        .view()
        .aggregateByTimestamp()
        .map(snapshot -> 1)
        .aggregate(Agg::sumInt);
    SortedMap<OSHDBTimestamp, Long> resultCustom = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps72)
        .view()
        .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
        .map(snapshot -> 1)
        .aggregate(Agg::sumInt);

    assertEquals(resultAuto.entrySet().size(), resultCustom.entrySet().size());
    for (OSHDBTimestamp t : resultAuto.keySet()) {
      assertEquals(
          resultAuto.get(t).intValue(),
          resultCustom.get(t).intValue()
      );
    }
  }

  @Test
  void testMapperFunctions() throws Exception {
    // check if it produces the same result whether the map function was set before or after aggr.
    SortedMap<OSHDBTimestamp, Long> result1 = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .view()
        .aggregateByTimestamp()
        .map(x -> 7)
        .aggregate(Agg::sumInt);
    SortedMap<OSHDBTimestamp, Long> result2 = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .view()
        .map(x -> 7)
        .aggregateByTimestamp()
        .aggregate(Agg::sumInt);

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
    SortedMap<OSHDBCombinedIndex<OSHDBTimestamp, OSMType>, Long> result =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps1)
            .view()
            .aggregateByTimestamp()
            .aggregateBy(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getType())
            .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
            .count();
    assertEquals(1, result.entrySet().size());
    assertEquals(timestamps1.get().first(), result.firstKey().getFirstIndex());
    assertEquals(OSMType.WAY, result.firstKey().getSecondIndex());
    assertEquals(42, result.values().toArray(Long[]::new)[0].intValue());

    SortedMap<OSHDBTimestamp, SortedMap<OSMType, Long>> nestedResult1 = OSHDBCombinedIndex
        .nest(result);
    assertEquals(42, nestedResult1.get(timestamps1.get().first()).get(OSMType.WAY).intValue());
  }

  @Test
  void testCombinedWithAggregateByIndexAuto() throws Exception {
    SortedMap<OSHDBCombinedIndex<OSHDBTimestamp, OSMType>, List<Long>> result =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps2)
            .view()
            .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
            .aggregateBy(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getType())
            .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
            .collect();
    SortedMap<OSHDBCombinedIndex<OSHDBTimestamp, OSMType>, List<Long>> resultAuto =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps2)
            .view()
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
            .view()
            .aggregateBy(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getType())
            .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
            .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
            .collect();
    SortedMap<OSHDBCombinedIndex<OSHDBTimestamp, OSMType>, List<Long>> resultTimeIndex =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps2)
            .view()
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
