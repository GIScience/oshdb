package org.heigit.ohsome.oshdb.api.tests;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.ohsome.oshdb.api.mapreducer.OSMEntitySnapshotView;
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
class MapAggregateByGeometryTest {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox = bboxWgs84Coordinates(8.0, 49.0, 9.0, 50.0);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2015-12-01");
  private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps("2010-01-01", "2015-12-01");

  private static final double DELTA = 1e-4;

  MapAggregateByGeometryTest() {
    oshdb = new OSHDBH2("../data/test-data");
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution() {
    return OSMContributionView
        .on(oshdb)
        .areaOfInterest(bbox)
        .filter("type:way and highway=*");
  }

  private MapReducer<OSMEntitySnapshot> createMapReducerOSMEntitySnapshot() {
    return OSMEntitySnapshotView
        .on(oshdb)
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
    SortedMap<String, Integer> resultCount = createMapReducerOSMContribution()
        .timestamps(timestamps2)
        .aggregateByGeometry(getSubRegions())
        .reduce(() -> 0, (x, ignored) -> x + 1, Integer::sum);

    assertEquals(4, resultCount.entrySet().size());
    assertTrue(resultCount.get("total") <= resultCount.get("left") + resultCount.get("right"));

    SortedMap<String, Double> resultSumLength = createMapReducerOSMContribution()
        .timestamps(timestamps2)
        .aggregateByGeometry(getSubRegions())
        .map(OSMContribution::getGeometryAfter)
        .map(Geo::lengthOf)
        .reduce(() -> 0.0, Double::sum);

    assertEquals(4, resultSumLength.entrySet().size());
    assertEquals(
        resultSumLength.get("total"),
        resultSumLength.get("left") + resultSumLength.get("right"),
        DELTA
    );
  }

  @Test
  void testOSMEntitySnapshot() throws Exception {
    SortedMap<String, Integer> resultCount = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps1)
        .aggregateByGeometry(getSubRegions())
        .reduce(() -> 0, (x, ignored) -> x + 1, Integer::sum);

    assertEquals(4, resultCount.entrySet().size());
    assertTrue(resultCount.get("total") <= resultCount.get("left") + resultCount.get("right"));

    SortedMap<String, Double> resultSumLength = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps1)
        .aggregateByGeometry(getSubRegions())
        .map(OSMEntitySnapshot::getGeometry)
        .map(Geo::lengthOf)
        .reduce(() -> 0.0, Double::sum);

    assertEquals(4, resultSumLength.entrySet().size());
    assertEquals(
        resultSumLength.get("total"),
        resultSumLength.get("left") + resultSumLength.get("right"),
        DELTA
    );
  }

  @Test
  void testZerofill() throws Exception {
    SortedMap<String, Integer> resultZerofilled = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps1)
        .aggregateByGeometry(getSubRegions())
        .count();
    assertEquals(4, resultZerofilled.entrySet().size());
    assertEquals(3, resultZerofilled.values().stream().filter(x -> x > 0).count());
  }

  @Test
  void testCombinedWithAggregateByTimestamp() throws Exception {
    SortedMap<OSHDBCombinedIndex<OSHDBTimestamp, String>, Integer> result =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps1)
            .aggregateByTimestamp()
            .aggregateByGeometry(getSubRegions())
            .reduce(() -> 0, (x, ignored) -> x + 1, Integer::sum);

    assertEquals(4, result.entrySet().size());
    Set<String> keys = result.keySet().stream()
        .map(OSHDBCombinedIndex::getSecondIndex)
        .collect(Collectors.toSet());
    assertTrue(keys.contains("left"));
    assertTrue(keys.contains("right"));
    assertTrue(keys.contains("total"));
    assertTrue(keys.contains("null island"));
  }

  @Test
  void testCombinedWithAggregateByTimestampReversed() throws Exception {
    SortedMap<OSHDBCombinedIndex<String, OSHDBTimestamp>, Integer> result =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps1)
            .aggregateByGeometry(getSubRegions())
            .aggregateByTimestamp()
            .reduce(() -> 0, (x, ignored) -> x + 1, Integer::sum);

    assertEquals(4, result.entrySet().size());
    Set<String> keys = result.keySet().stream()
        .map(OSHDBCombinedIndex::getFirstIndex)
        .collect(Collectors.toSet());
    assertTrue(keys.contains("left"));
    assertTrue(keys.contains("right"));
    assertTrue(keys.contains("total"));
    assertTrue(keys.contains("null island"));
  }

  @Test
  void testCombinedWithAggregateByTimestampOrder() throws Exception {
    SortedMap<OSHDBCombinedIndex<String, OSHDBTimestamp>, List<Long>> resultGeomTime =
        OSMEntitySnapshotView
            .on(oshdb)
            .areaOfInterest(bbox)
            .timestamps(timestamps2)
            .aggregateByGeometry(getSubRegions())
            .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
            .filter("type:way and highway=*")
            .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
            .collect();
    SortedMap<OSHDBCombinedIndex<OSHDBTimestamp, String>, List<Long>> resultTimeGeom =
        OSMEntitySnapshotView
            .on(oshdb)
            .areaOfInterest(bbox)
            .timestamps(timestamps2)
            .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
            .aggregateByGeometry(getSubRegions())
            .filter("type:way and highway=*")
            .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
            .collect();
    assertEquals(resultGeomTime.entrySet().size(), resultTimeGeom.entrySet().size());
    for (OSHDBCombinedIndex<String, OSHDBTimestamp> idx : resultGeomTime.keySet()) {
      assertEquals(
          resultGeomTime.get(idx),
          resultTimeGeom.get(new OSHDBCombinedIndex<>(idx.getSecondIndex(), idx.getFirstIndex()))
      );
    }
  }

  @SuppressWarnings("ResultOfMethodCallIgnored") //  we test for a thrown exception here
  @Test()
  void testCombinedWithAggregateByTimestampUnsupportedOrder3() {
    assertThrows(UnsupportedOperationException.class, () ->
      createMapReducerOSMEntitySnapshot()
          .timestamps(timestamps1)
          .groupByEntity()
          .aggregateByGeometry(getSubRegions())
          .collect()
    );
  }
}
