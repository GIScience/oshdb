/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.api.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.vividsolutions.jts.geom.Polygon;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.Geo;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.junit.Test;

/**
 *
 */
public class TestMapAggregateByGeometry {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8, 49, 9, 50);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2015-12-01");
  private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps("2010-01-01", "2015-12-01");

  private final double DELTA = 1e-4;

  public TestMapAggregateByGeometry() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data");
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb).osmType(OSMType.WAY).osmTag("highway").areaOfInterest(bbox);
  }
  private MapReducer<OSMEntitySnapshot> createMapReducerOSMEntitySnapshot() throws Exception {
    return OSMEntitySnapshotView.on(oshdb).osmType(OSMType.WAY).osmTag("highway").areaOfInterest(bbox);
  }
  private Map<String, Polygon> getSubRegions() {
    Map<String, Polygon> res = new TreeMap<>();
    res.put("left", OSHDBGeometryBuilder.getGeometry(
        new OSHDBBoundingBox(8, 49, 8.66128, 50)
    ));
    res.put("right", OSHDBGeometryBuilder.getGeometry(
        new OSHDBBoundingBox(8.66128+1E-8, 49, 9, 50)
    ));
    res.put("total", OSHDBGeometryBuilder.getGeometry(
        bbox
    ));
    res.put("null island", OSHDBGeometryBuilder.getGeometry(
        new OSHDBBoundingBox(-1, -1, 1, 1)
    ));
    return res;
  }

  @Test
  public void testOSMContribution() throws Exception {
    SortedMap<String, Integer> resultCount = createMapReducerOSMContribution()
        .timestamps(timestamps2)
        .aggregateByGeometry(getSubRegions())
        .reduce(() -> 0, (x,ignored) -> x+1, (x,y) -> x+y);

    assertEquals(4, resultCount.entrySet().size());
    assertTrue(resultCount.get("total") <= resultCount.get("left") + resultCount.get("right"));

    SortedMap<String, Double> resultSumLength = createMapReducerOSMContribution()
        .timestamps(timestamps2)
        .aggregateByGeometry(getSubRegions())
        .map(OSMContribution::getGeometryAfter)
        .map(Geo::lengthOf)
        .reduce(() -> 0.0, (x,y) -> x+y);

    assertEquals(4, resultSumLength.entrySet().size());
    assertEquals(
        resultSumLength.get("total"),
        resultSumLength.get("left") + resultSumLength.get("right"),
        DELTA
    );
  }

  @Test
  public void testOSMEntitySnapshot() throws Exception {
    SortedMap<String, Integer> resultCount = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps1)
        .aggregateByGeometry(getSubRegions())
        .reduce(() -> 0, (x,ignored) -> x+1, (x,y) -> x+y);

    assertEquals(4, resultCount.entrySet().size());
    assertTrue(resultCount.get("total") <= resultCount.get("left") + resultCount.get("right"));

    SortedMap<String, Double> resultSumLength = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps1)
        .aggregateByGeometry(getSubRegions())
        .map(OSMEntitySnapshot::getGeometry)
        .map(Geo::lengthOf)
        .reduce(() -> 0.0, (x,y) -> x+y);

    assertEquals(4, resultSumLength.entrySet().size());
    assertEquals(
        resultSumLength.get("total"),
        resultSumLength.get("left") + resultSumLength.get("right"),
        DELTA
    );
  }

  @Test
  public void testZerofill() throws Exception {
    SortedMap<String, Integer> resultZerofilled = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps1)
        .aggregateByGeometry(getSubRegions())
        .count();
    assertEquals(4, resultZerofilled.entrySet().size());
    assertEquals(3, resultZerofilled.values().stream().filter(x -> x > 0).count());
  }

  @Test
  public void testCombinedWithAggregateByTimestamp() throws Exception {
    SortedMap<OSHDBCombinedIndex<OSHDBTimestamp, String>, Integer> result = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps1)
        .aggregateByTimestamp()
        .aggregateByGeometry(getSubRegions())
        .reduce(() -> 0, (x,ignored) -> x+1, (x,y) -> x+y);

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
  public void testCombinedWithAggregateByTimestampOrder() throws Exception {
    SortedMap<OSHDBCombinedIndex<String, OSHDBTimestamp>, List<Long>> resultGT =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps2)
            .aggregateByGeometry(getSubRegions())
            .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
            .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
            .collect();
    SortedMap<OSHDBCombinedIndex<OSHDBTimestamp, String>, List<Long>> resultTG =
        createMapReducerOSMEntitySnapshot()
            .timestamps(timestamps2)
            .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
            .aggregateByGeometry(getSubRegions())
            .map(osmEntitySnapshot -> osmEntitySnapshot.getEntity().getId())
            .collect();
    assertEquals(resultGT.entrySet().size(), resultTG.entrySet().size());
    for (OSHDBCombinedIndex<String, OSHDBTimestamp> idx : resultGT.keySet()) {
      assertEquals(
          resultGT.get(idx),
          resultTG.get(new OSHDBCombinedIndex<>(idx.getSecondIndex(), idx.getFirstIndex()))
      );
    }
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCombinedWithAggregateByTimestampUnsupportedOrder1() throws Exception {
    //noinspection ResultOfMethodCallIgnored – we test for a thrown exception here
    createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps1)
        .map(ignored -> null)
        .aggregateByTimestamp()
        .aggregateByGeometry(getSubRegions())
        .collect();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCombinedWithAggregateByTimestampUnsupportedOrder3() throws Exception {
    //noinspection ResultOfMethodCallIgnored – we test for a thrown exception here
    createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps1)
        .groupByEntity()
        .aggregateByGeometry(getSubRegions())
        .collect();
  }
}
