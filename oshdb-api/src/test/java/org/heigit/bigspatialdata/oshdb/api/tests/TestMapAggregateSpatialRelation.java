package org.heigit.bigspatialdata.oshdb.api.tests;

import static org.junit.Assert.*;

import java.util.List;
import java.util.TreeMap;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer.Pair;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps.Interval;
import org.junit.Test;

public class TestMapAggregateSpatialRelation {

  private final OSHDBJdbc oshdb;

  // create bounding box from coordinates
  private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8, 49, 9, 50);

  private final OSHDBTimestamps timestamps2017 = new OSHDBTimestamps("2017-01-01", "2017-01-02", Interval.DAILY);
  private final OSHDBTimestamps timestamps2016 = new OSHDBTimestamps("2016-01-01", "2016-01-02", Interval.DAILY);

  public TestMapAggregateSpatialRelation() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data").multithreading(true);
  }

  // Covers / CoveredBy ------------------------------------------------------------------------

  @Test
  public void test_covers_coveredby() throws Exception {

    // todo: create test data test_polygon_covers_polygon()

    // todo: create test data test_polygon_covers_line()

    // todo: create test data for test_line_covers_line()

    // todo: create test data for test_line_covers_node()
  }

  // Inside / contains --------------------------------------------------------------------------
  @Test
  public void test_inside_contains() throws Exception {

    TreeMap<OSHDBTimestamp, Integer> result_contains = (TreeMap<OSHDBTimestamp, Integer>) OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .aggregateByTimestamp()
        .filter(x -> x.getEntity().getId() == 36493984)
        .containedFeatures(
            mapReduce -> mapReduce.osmType(OSMType.NODE).collect())
        .flatMap(x -> x.getValue())
        .count();

    assertEquals(3, result_contains.firstEntry().getValue().intValue());

    TreeMap<OSHDBTimestamp, Integer> result_inside = (TreeMap<OSHDBTimestamp, Integer>)OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.NODE)
        .aggregateByTimestamp()
        .inside(
            mapReduce -> mapReduce
                .filter(x -> ((OSMEntitySnapshot) x).getEntity().getId() == 36493984).collect())
        .count();
    assertEquals(3, result_inside.firstEntry().getValue().intValue());

    // Test nodes inside poylgon
    TreeMap<OSHDBTimestamp, Integer> result_contains2 = (TreeMap<OSHDBTimestamp, Integer>) OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .osmTag("building")
        .aggregateByTimestamp()
        .containedFeatures(
            mapReduce -> mapReduce.osmType(OSMType.NODE).collect())
        .flatMap(x -> x.getValue())
        .count();

    TreeMap<OSHDBTimestamp, Integer> result_inside2 = (TreeMap<OSHDBTimestamp, Integer>) OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.NODE)
        .aggregateByTimestamp()
        .inside(
            mapReduce -> mapReduce.osmType(OSMType.WAY).osmTag("building").collect())
        .count();
    assertEquals(result_inside2, result_contains2);

    // Line inside polygon
    // todo: create test data test_line_inside_polygon()

  }

  // Equals  -----------------------------------------------------------------------------------

  @Test
  public void test_equals() throws Exception {

    // Polygon equals polygon
    // todo: create test data for test_polygon_equals_polygon()

    // Line equals line
    // todo: create test data for test_line_equals_line()

    // Node equals node
    TreeMap<OSHDBTimestamp, Integer> result3 = (TreeMap<OSHDBTimestamp, Integer>) OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2016)
        .areaOfInterest(bbox)
        .osmType(OSMType.NODE)
        .aggregateByTimestamp()
        .equals()
        .count();
    assertEquals(2, result3.firstEntry().getValue().intValue());
  }

  // Overlaps --------------------------------------------------------------------------------------

  @Test
  public void test_overlaps() throws Exception {

    // Line overlaps line
    TreeMap<OSHDBTimestamp, Integer> result = (TreeMap<OSHDBTimestamp, Integer>) OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 36493984)
        .aggregateByTimestamp()
        .overlappedFeatures(
            mapReduce -> mapReduce.osmType(OSMType.WAY).collect())
        .flatMap(Pair::getValue)
        .count();
    assertEquals(2, result.firstEntry().getValue().intValue());

    // Line overlaps polygon
    TreeMap<OSHDBTimestamp, Integer> result2 = (TreeMap<OSHDBTimestamp, Integer>) OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 172510842)
        .aggregateByTimestamp()
        .overlappedFeatures(
            mapReduce -> mapReduce.osmType(OSMType.WAY).collect())
        .flatMap(Pair::getValue)
        .count();
    assertEquals(2, result2.firstEntry().getValue().intValue());

    // todo: create test data polygon_overlaps_polygon
  }


  // Touches -----------------------------------------------------------------------------

  @Test
  public void test_touches() throws Exception {

    // Polygon touches polygon
    TreeMap<OSHDBTimestamp, Integer> result = (TreeMap<OSHDBTimestamp, Integer>) OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .osmTag("building")
        .filter(x -> x.getEntity().getId() == 130518815)
        .aggregateByTimestamp()
        .touchingFeatures(
            mapReduce -> mapReduce.osmTag("building").collect())
        .count();
    assertEquals(1, result.firstEntry().getValue().intValue());

    TreeMap<OSHDBTimestamp, List<OSMEntitySnapshot>> result2 = (TreeMap<OSHDBTimestamp, List<OSMEntitySnapshot>>) OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .aggregateByTimestamp()
        .osmType(OSMType.WAY)
        .osmTag("building")
        .filter(x -> x.getEntity().getId() == 172510837)
        .touchingFeatures(
            mapReduce -> mapReduce.osmTag("building").collect())
        .flatMap(x -> x.getValue())
        .collect();
    assertEquals(2, result2.firstEntry().getValue().size());

    // Line touches polygon
    TreeMap<OSHDBTimestamp, List<OSMEntitySnapshot>> result4 = (TreeMap<OSHDBTimestamp, List<OSMEntitySnapshot>>) OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 172510842)
        .aggregateByTimestamp()
        .touchingFeatures(
            mapReduce -> mapReduce.osmType(OSMType.WAY).collect())
        .flatMap(x -> x.getValue())
        .collect();
    assertEquals(1, result4.firstEntry().getValue().size());

    // Line touching line
    TreeMap<OSHDBTimestamp, List<OSMEntitySnapshot>> result5 = (TreeMap<OSHDBTimestamp, List<OSMEntitySnapshot>>) OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 36493984)
        .aggregateByTimestamp()
        .touchingFeatures(
            mapReduce -> mapReduce.osmType(OSMType.WAY).collect())
        .flatMap(x -> x.getValue())
        .collect();
    assertEquals(3, result5.firstEntry().getValue().size());

    // Line touches node
    TreeMap<OSHDBTimestamp, List<OSMEntitySnapshot>> result6 = (TreeMap<OSHDBTimestamp, List<OSMEntitySnapshot>>) OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 36493984)
        .aggregateByTimestamp()
        .touchingFeatures(
            mapReduce -> mapReduce.osmType(OSMType.NODE).collect())
        .flatMap(x -> x.getValue())
        .collect();
    assertEquals(1, result6.firstEntry().getValue().size());

    // todo: create test data for node touching polygon
    // todo: create tests for relations
  }

  // Neighbouring ------------------------------------------------------------------------------
  @Test
  public void test_neighbouring() throws Exception {
    TreeMap<OSHDBTimestamp, List<OSMEntitySnapshot>> result = (TreeMap<OSHDBTimestamp, List<OSMEntitySnapshot>>) OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 130530843)
        .aggregateByTimestamp()
        .neighbouringFeatures(12.,
            mapReduce -> mapReduce.osmType(OSMType.NODE).collect())
        .flatMap(x -> x.getValue())
        .collect();
    assertEquals(4, result.firstEntry().getValue().size());
  }


}