package org.heigit.ohsome.oshdb.api.tests;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbcImprove;
import org.heigit.ohsome.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.contribution.OSMContributionView;
import org.heigit.ohsome.oshdb.api.mapreducer.snapshot.OSMEntitySnapshotView;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
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
    oshdb = new OSHDBJdbcImprove("./src/test/resources/test-data");
  }

  private OSHDBView<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb)
        .areaOfInterest(bbox)
        .filter("type:way and highway=*");
  }

  private OSHDBView<OSMEntitySnapshot> createMapReducerOSMEntitySnapshot() throws Exception {
    return OSMEntitySnapshotView.on(oshdb)
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
        .view()
        .aggregateByGeometry(getSubRegions())
        .reduce(() -> 0, (x, ignored) -> x + 1, Integer::sum);

    assertEquals(3, resultCount.entrySet().size());
    assertTrue(resultCount.get("total") <= resultCount.get("left") + resultCount.get("right"));

    SortedMap<String, Double> resultSumLength = createMapReducerOSMContribution()
        .timestamps(timestamps2)
        .view()
        .aggregateByGeometry(getSubRegions())
        .map(contrib -> Geo.lengthOf(contrib.getGeometryAfter()))
//        .map(OSMContribution::getGeometryAfter)
//        .map(Geo::lengthOf)
        .reduce(() -> 0.0, Double::sum);

    assertEquals(3, resultSumLength.entrySet().size());
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
        .view()
        .aggregateByGeometry(getSubRegions())
        .reduce(() -> 0, (x, ignored) -> x + 1, Integer::sum);

    assertEquals(3, resultCount.entrySet().size());
    assertTrue(resultCount.get("total") <= resultCount.get("left") + resultCount.get("right"));

    SortedMap<String, Double> resultSumLength = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps1)
        .view()
        .aggregateByGeometry(getSubRegions())
        .map(OSMEntitySnapshot::getGeometry)
        .map(Geo::lengthOf)
        .reduce(() -> 0.0, Double::sum);

    assertEquals(3, resultSumLength.entrySet().size());
    assertEquals(
        resultSumLength.get("total"),
        resultSumLength.get("left") + resultSumLength.get("right"),
        DELTA
    );
  }

  @Test
  void testCombinedWithAggregateByTimestamp() throws Exception {
    SortedMap<OSHDBCombinedIndex<OSHDBTimestamp, String>, Integer> result =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps1)
            .view()
            .aggregateByTimestamp()
            .aggregateByGeometry(getSubRegions())
            .reduce(() -> 0, (x, ignored) -> x + 1, Integer::sum);

    assertEquals(3, result.entrySet().size());
    Set<String> keys = result.keySet().stream()
        .map(OSHDBCombinedIndex::getSecondIndex)
        .collect(Collectors.toSet());
    assertTrue(keys.contains("left"));
    assertTrue(keys.contains("right"));
    assertTrue(keys.contains("total"));
  }

  @Test
  void testCombinedWithAggregateByTimestampReversed() throws Exception {
    SortedMap<OSHDBCombinedIndex<String, OSHDBTimestamp>, Integer> result =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps1)
            .view()
            .aggregateByGeometry(getSubRegions())
            .aggregateByTimestamp()
            .reduce(() -> 0, (x, ignored) -> x + 1, Integer::sum);

    assertEquals(3, result.entrySet().size());
    Set<String> keys = result.keySet().stream()
        .map(OSHDBCombinedIndex::getFirstIndex)
        .collect(Collectors.toSet());
    assertTrue(keys.contains("left"));
    assertTrue(keys.contains("right"));
    assertTrue(keys.contains("total"));
  }

  @Test
  void testCombinedWithAggregateByTimestampOrder() throws Exception {
    SortedMap<OSHDBCombinedIndex<String, OSHDBTimestamp>, List<Long>> resultGeomTime =
        OSMEntitySnapshotView.on(oshdb)
            .areaOfInterest(bbox)
            .timestamps(timestamps2)
            .filter("type:way and highway=*")
            .view()
            .aggregateByGeometry(getSubRegions())
            .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
            .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
            .collect();
    SortedMap<OSHDBCombinedIndex<OSHDBTimestamp, String>, List<Long>> resultTimeGeom =
        OSMEntitySnapshotView.on(oshdb)
            .areaOfInterest(bbox)
            .timestamps(timestamps2)
            .filter("type:way and highway=*")
            .view()
            .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
            .aggregateByGeometry(getSubRegions())
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
}
