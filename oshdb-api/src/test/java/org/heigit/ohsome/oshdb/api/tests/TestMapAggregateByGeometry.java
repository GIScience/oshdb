package org.heigit.ohsome.oshdb.api.tests;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.mapreducer.CombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.contribution.OSMContributionView;
import org.heigit.ohsome.oshdb.api.mapreducer.reduction.Collector;
import org.heigit.ohsome.oshdb.api.mapreducer.snapshot.OSMEntitySnapshotView;
import org.heigit.ohsome.oshdb.util.geometry.Geo;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Polygon;

/**
 * Test aggregateByGeometry method of the OSHDB API.
 */
class TestMapAggregateByGeometry {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox = bboxWgs84Coordinates(8.0, 49.0, 9.0, 50.0);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2015-12-01");
  private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps("2010-01-01", "2015-12-01");

  private static final double DELTA = 1e-4;

  TestMapAggregateByGeometry() throws Exception {
    oshdb = new OSHDBH2("../data/test-data");
  }

  private OSMContributionView createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.view()
        .areaOfInterest(bbox)
        .filter("type:way and highway=*");
  }

  private OSMEntitySnapshotView createMapReducerOSMEntitySnapshot() throws Exception {
    return OSMEntitySnapshotView.view()
        .areaOfInterest(bbox)
        .filter("type:way and highway=*");
  }

  private Map<String, Polygon> getSubRegions() {
    Map<String, Polygon> res = new TreeMap<>();
    res.put("left", OSHDBGeometryBuilder.getGeometry(
        bboxWgs84Coordinates(8, 49, 8.66128, 50)
        ));
    res.put("right", OSHDBGeometryBuilder.getGeometry(
        bboxWgs84Coordinates(8.66128 + 1E-8, 49, 9, 50)
        ));
    res.put("total", OSHDBGeometryBuilder.getGeometry(
        bbox
        ));
    res.put("null island", OSHDBGeometryBuilder.getGeometry(
        bboxWgs84Coordinates(-1.0, -1.0, 1.0, 1.0)
        ));
    return res;
  }

  @Test
  void testOSMContribution() throws Exception {
    var subRegions = getSubRegions();
    Map<String, Integer> resultCount = createMapReducerOSMContribution()
        .timestamps(timestamps2)
        .on(oshdb)
        .aggregateByGeometry(subRegions)
        .reduce(() -> 0, (x, ignored) -> x + 1, Integer::sum);

    assertEquals(3, resultCount.entrySet().size());
    assertTrue(resultCount.get("total") <= resultCount.get("left") + resultCount.get("right"));

    Map<String, Double> resultSumLength = createMapReducerOSMContribution()
        .timestamps(timestamps2)
        .on(oshdb)
        .aggregateByGeometry(subRegions)
        .map(OSMContribution::getGeometryAfter)
        .map(Geo::lengthOf)
        .reduce(() -> 0.0, Double::sum);

    assertEquals(3, resultSumLength.entrySet().size());
    assertEquals(
        resultSumLength.get("total"),
        resultSumLength.get("left") + resultSumLength.get("right"),
        DELTA);
  }

  @Test
  void testOSMEntitySnapshot() throws Exception {
    var subRegions =  getSubRegions();
   // var gs = new GeometrySplitter<>(subRegions);
    Map<String, Integer> resultCount = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps1)
        .on(oshdb)
        .aggregateByGeometry(subRegions)
        .reduce(() -> 0, (x, ignored) -> x + 1, Integer::sum);

    assertEquals(3, resultCount.entrySet().size());
    assertTrue(resultCount.get("total") <= resultCount.get("left") + resultCount.get("right"));

    Map<String, Double> resultSumLength = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps1)
        .on(oshdb)
        .aggregateByGeometry(subRegions)
        .map(OSMEntitySnapshot::getGeometry)
        .map(Geo::lengthOf)
        .reduce(() -> 0.0, Double::sum);

    assertEquals(3, resultSumLength.entrySet().size());
    assertEquals(
        resultSumLength.get("total"),
        resultSumLength.get("left") + resultSumLength.get("right"),
        DELTA);
  }

  //  @Test
  //  void testZerofill() throws Exception {
  //    Map<String, Long> resultZerofilled = createMapReducerOSMEntitySnapshot()
  //        .timestamps(timestamps1)
  //        .on(oshdb)
  //        .aggregate(byGeometry(getSubRegions()))
  //        .count();
  //    assertEquals(4, resultZerofilled.entrySet().size());
  //    assertEquals(3, resultZerofilled.values().stream().filter(x -> x > 0).count());
  //  }

  @Test
  void testCombinedWithAggregateByTimestamp() throws Exception {
    var subRegions =  getSubRegions();
    Map<CombinedIndex<OSHDBTimestamp, String>, Integer> result =
        createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps1)
        .on(oshdb)
        .aggregateByTimestamp()
        .aggregateByGeometry(subRegions)
        .reduce(() -> 0, (x, ignored) -> x + 1, Integer::sum);

    assertEquals(3, result.entrySet().size());
    Set<String> keys = result.keySet().stream()
        .map(CombinedIndex::v)
        .collect(Collectors.toSet());
    assertTrue(keys.contains("left"));
    assertTrue(keys.contains("right"));
    assertTrue(keys.contains("total"));
    assertFalse(keys.contains("null island"));
  }

  @Test
  void testCombinedWithAggregateByTimestampReversed() throws Exception {
    var subRegions =  getSubRegions();
    Map<CombinedIndex<String, OSHDBTimestamp>, Integer> result =
        createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps1)
        .on(oshdb)
        .aggregateByGeometry(subRegions)
        .aggregateByTimestamp()
        .reduce(() -> 0, (x, ignored) -> x + 1, Integer::sum);

    assertEquals(3, result.entrySet().size());
    Set<String> keys = result.keySet().stream()
        .map(CombinedIndex::u)
        .collect(Collectors.toSet());
    assertTrue(keys.contains("left"));
    assertTrue(keys.contains("right"));
    assertTrue(keys.contains("total"));
    assertFalse(keys.contains("null island"));
  }

  @Test
  void testCombinedWithAggregateByTimestampOrder() throws Exception {
    var subRegions =  getSubRegions();
    Map<CombinedIndex<String, OSHDBTimestamp>, List<Long>> resultGeomTime =
        OSMEntitySnapshotView.view()
        .areaOfInterest(bbox)
        .timestamps(timestamps2)
        .filter("type:way and highway=*")
        .on(oshdb)

        .aggregateByGeometry(subRegions)
        .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
        .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
        .reduce(Collector::toList);

    Map<CombinedIndex<OSHDBTimestamp, String>, List<Long>> resultTimeGeom =
        OSMEntitySnapshotView.view()
        .areaOfInterest(bbox)
        .timestamps(timestamps2)
        .filter("type:way and highway=*")
        .on(oshdb)
        .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
        .aggregateByGeometry(subRegions)
        .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
        .reduce(Collector::toList);
    assertEquals(resultGeomTime.entrySet().size(), resultTimeGeom.entrySet().size());
    for (CombinedIndex<String, OSHDBTimestamp> idx : resultGeomTime.keySet()) {
      assertEquals(
          resultGeomTime.get(idx),
          resultTimeGeom.get(new CombinedIndex<>(idx.v(), idx.u())));
    }
  }

  //  @SuppressWarnings("ResultOfMethodCallIgnored") //  we test for a thrown exception here
  //  @Test()
  //  void testCombinedWithAggregateByTimestampUnsupportedOrder3() throws Exception {
  //    assertThrows(UnsupportedOperationException.class, () -> {
  //      createMapReducerOSMEntitySnapshot()
  //      .timestamps(timestamps1)
  //      .on(oshdb)
  //      .groupByEntity()
  //      .aggregate(byGeometry(getSubRegions()))
  //      .collect();
  //    });
  //  }
}
