package org.heigit.ohsome.oshdb.api.tests;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.mapreducer.CombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.contribution.OSMContributionView;
import org.heigit.ohsome.oshdb.api.mapreducer.snapshot.OSMEntitySnapshotView;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * Test aggregate by custom index method of the OSHDB API.
 */
class TestMapAggregateByIndex {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox = bboxWgs84Coordinates(8.0, 49.0, 9.0, 50.0);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2015-12-01");
  private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps("2010-01-01", "2015-12-01");
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  TestMapAggregateByIndex() throws Exception {
    oshdb = new OSHDBH2("../data/test-data");
  }

  private OSMContributionView createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.view()
        .areaOfInterest(bbox)
        .filter("type:node and highway=*");
  }

  private OSMEntitySnapshotView createMapReducerOSMEntitySnapshot() throws Exception {
    return OSMEntitySnapshotView.view()
        .areaOfInterest(bbox)
        .filter("type:node and highway=*");
  }

  @Test
  void testOSMContribution() throws Exception {
    Map<Long, Set<Integer>> result = createMapReducerOSMContribution()
        .timestamps(timestamps2)
        .filter("id:617308093")
        .on(oshdb)
        .aggregateBy(contribution -> contribution.getEntityAfter().getId())
        .map(OSMContribution::getContributorUserId)
        .reduce(
            HashSet::new,
            (x, y) -> {
              x.add(y);
              return x;
            },
            (x, y) -> {
              Set<Integer> ret = new HashSet<>(x);
              ret.addAll(y);
              return ret;
            }
            );

    assertEquals(1, result.entrySet().size());
    // should be 5: first version doesn't have the highway tag, remaining 7 versions have 5
    // different contributor user ids
    assertEquals(5, result.get(617308093L).size());
  }

  @Test
  void testOSMEntitySnapshot() throws Exception {
    Map<Long, Set<Integer>> result = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps72)
        .filter("id:617308093")
        .on(oshdb)
        .aggregateBy(snapshot -> snapshot.getEntity().getId())
        .map(snapshot -> snapshot.getEntity().getUserId())
        .reduce(
            HashSet::new,
            (x, y) -> {
              x.add(y);
              return x;
            },
            (x, y) -> {
              Set<Integer> ret = new HashSet<>(x);
              ret.addAll(y);
              return ret;
            }
            );

    assertEquals(1, result.entrySet().size());
    // should be 5: first version doesn't have the highway tag, remaining 7 versions have 5
    // different contributor user ids
    assertEquals(5, result.get(617308093L).size());
  }

  //  @Test
  //  void testZerofill() throws Exception {
  //    // partially empty result
  //    SortedMap<Long, Integer> result = createMapReducerOSMContribution()
  //        .timestamps(timestamps72)
  //        .filter("id:617308093")
  //        .aggregateBy(
  //            contribution -> contribution.getEntityAfter().getId(),
  //            Collections.singletonList(-1L)
  //        )
  //        .count();
  //
  //    assertEquals(2, result.entrySet().size());
  //    assertEquals(true, result.containsKey(-1L));
  //    assertEquals(0, (int) result.get(-1L));
  //    assertEquals(7, (int) result.get(617308093L));
  //
  //    // totally empty result
  //    result = createMapReducerOSMContribution()
  //        .timestamps(timestamps72)
  //        .filter(x -> false)
  //        .aggregateBy(
  //            contribution -> contribution.getEntityAfter().getId(),
  //            Collections.singletonList(-1L)
  //        )
  //        .count();
  //
  //    assertEquals(1, result.entrySet().size());
  //    assertEquals(true, result.containsKey(-1L));
  //    assertEquals(0, (int) result.get(-1L));
  //  }

  @Test
  void testMultiple2() throws Exception {
    Map<CombinedIndex<Long, OSMType>, Long> result =
        createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps1)
        .filter("id:617308093")
        .on(oshdb)
        .aggregateBy(snapshot -> snapshot.getEntity().getId())
        .aggregateBy(snapshot -> snapshot.getEntity().getType())
        .count();

    assertEquals(1, result.entrySet().size());
    assertEquals(1L, result.get(new CombinedIndex<>(617308093L, OSMType.NODE)));
    Map<Long, Map<OSMType, Long>> nestedResult = CombinedIndex.nest(result);
    assertEquals(1L, nestedResult.get(617308093L).get(OSMType.NODE));
  }

  @Test
  void testMultiple3() throws Exception {
    Map<CombinedIndex<CombinedIndex<Long, OSMType>, Integer>, Long> result =
        createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps1)
        .filter("id:617308093")
        .on(oshdb)
        .aggregateBy(snapshot -> snapshot.getEntity().getId())
        .aggregateBy(snapshot -> snapshot.getEntity().getType())
        .aggregateBy(snapshot -> snapshot.getEntity().getUserId())
        .count();

    assertEquals(1, result.entrySet().size());
    Map<Long, Map<OSMType, Map<Integer, Long>>> nestedResult =
        CombinedIndex.nest(CombinedIndex.nest(result));
    assertEquals(1L, nestedResult.get(617308093L).get(OSMType.NODE).get(165061));
  }
}
