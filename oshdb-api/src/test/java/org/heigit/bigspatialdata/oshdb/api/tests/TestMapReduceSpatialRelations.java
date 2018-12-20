package org.heigit.bigspatialdata.oshdb.api.tests;

import static org.junit.Assert.*;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.celliterator.ContributionType;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps.Interval;
import org.junit.Test;

public class TestMapReduceSpatialRelations {

  private final OSHDBJdbc oshdb;

  // create bounding box from coordinates
  //private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(85.3406012, 27.6991942, 85.3585444, 27.7121143);
  private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8, 49, 9, 50);

  private final OSHDBTimestamps timestamps2017 = new OSHDBTimestamps("2017-01-01");
  private final OSHDBTimestamps timestamps2016 = new OSHDBTimestamps("2016-01-01");
  private final OSHDBTimestamps timestamps10 = new OSHDBTimestamps("2007-01-01", "2017-01-01",
      Interval.YEARLY);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2016-01-01", "2017-01-01",
      Interval.YEARLY);
  private final OSHDBTimestamps timestamps13 = new OSHDBTimestamps("2012-01-01", "2012-12-31");

  public TestMapReduceSpatialRelations() throws Exception {
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

    Integer result_contains = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 36493984)
        .containment(
            mapReduce -> mapReduce.osmType(OSMType.NODE).collect())
        .flatMap(x -> x.getRight())
        .count();
    assertEquals(3, result_contains.intValue());

    Integer result_inside = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.NODE)
        .inside(
            mapReduce -> mapReduce
                .filter(x -> ((OSMEntitySnapshot) x).getEntity().getId() == 36493984).collect())
        .count();
    assertEquals(3, result_inside.intValue());

    // Test nodes inside poylgon
    Integer result_contains2 = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .osmTag("building")
        .containment(
            mapReduce -> mapReduce.osmType(OSMType.NODE).collect())
        .flatMap(x -> x.getRight())
        .count();

    Integer result_inside2 = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.NODE)
        .inside(
            mapReduce -> mapReduce.osmType(OSMType.WAY).osmTag("building").collect())
        .count();
    assertEquals(result_inside2, result_contains2);

    // Line inside polygon
    // todo: create test data test_line_inside_polygon()
    Integer result_contains3 = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .osmTag("building")
        .containment(
            mapReduce -> mapReduce.osmType(OSMType.WAY).collect())
        .flatMap(x -> x.getRight())
        .count();
    Integer result_inside3 = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .inside(
            mapReduce -> mapReduce.osmType(OSMType.WAY).osmTag("building").collect())
        .count();
    //assertEquals(result_inside3, result_contains3);

  }

  // Equals  -----------------------------------------------------------------------------------

  @Test
  public void test_equals() throws Exception {

    // Polygon equals polygon
    // todo: create test data for test_polygon_equals_polygon()
    List<Pair<OSMEntitySnapshot, List<OSMEntitySnapshot>>> result = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .osmTag("building")
        .equalFeatures(
            mapReduce -> mapReduce.osmType(OSMType.WAY).osmTag("building").collect())
        .filter(x -> x.getRight().size() > 0)
        .collect();
    //assertEquals(2, result);

    // Line equals line
    // todo: create test data for test_line_equals_line()
    List<Pair<OSMEntitySnapshot, List<OSMEntitySnapshot>>> result2 = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .equalFeatures(
            mapReduce -> mapReduce.osmType(OSMType.WAY).collect())
        .filter(x -> x.getRight().size() > 0)
        .collect();
    //assertEquals(2, result2);

    // Node equals node
    Integer result3 = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2016)
        .areaOfInterest(bbox)
        .osmType(OSMType.NODE)
        .equals()
        .count();
    assertEquals(2, result3.intValue());
  }

  // Overlaps --------------------------------------------------------------------------------------

  @Test
  public void test_overlaps() throws Exception {

    // Line overlaps line
    Integer result = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 36493984)
        .overlappingFeatures(
            mapReduce -> mapReduce.osmType(OSMType.WAY).collect())
        .flatMap(x -> x.getRight())
        .count();
    assertEquals(2, result.intValue());

    // Line overlaps polygon
    Integer result2 = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 172510842)
        .overlappingFeatures(
            mapReduce -> mapReduce.osmType(OSMType.WAY).collect())
        .flatMap(x -> x.getRight())
        .count();
    assertEquals(2, result2.intValue());

    // todo: create test data polygon_overlaps_polygon
  }


  // Touches -----------------------------------------------------------------------------

  @Test
  public void test_touches() throws Exception {

    // Polygon touches polygon
    List<Pair<OSMEntitySnapshot, List<OSMEntitySnapshot>>> result = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .osmTag("building")
        .filter(x -> x.getEntity().getId() == 130518815)
        .touchingFeatures(
            mapReduce -> mapReduce.osmTag("building").collect())
        .collect();
    assertEquals(1, result.get(0).getRight().size());

    List<Pair<OSMEntitySnapshot, List<OSMEntitySnapshot>>> result2 = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .osmTag("building")
        .filter(x -> x.getEntity().getId() == 172510837)
        .touchingFeatures(
            mapReduce -> mapReduce.osmTag("building").collect())
        .collect();
    assertEquals(2, result2.get(0).getRight().size());

    // Polygon touching polygon
    Integer result3 = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .osmTag("building")
        .touches(
            mapReduce -> mapReduce.osmTag("building").collect())
        .count();
    assertEquals(41, result3.intValue());


    // Line touches polygon
    Integer result4 = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 172510842)
        .touchingFeatures(
            mapReduce -> mapReduce.osmType(OSMType.WAY).collect())
        .flatMap(x -> x.getRight())
        .count();
    assertEquals(1, result4.intValue());

    // Line touching line
    Integer result5 = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 36493984)
        .touchingFeatures(
            mapReduce -> mapReduce.osmType(OSMType.WAY).collect())
        .flatMap(x -> x.getRight())
        .count();
    assertEquals(3, result5.intValue());

    // Line touches node
    Integer result6 = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 36493984)
        .touchingFeatures(
            mapReduce -> mapReduce.osmType(OSMType.NODE).collect())
        .count();
    assertEquals(1, result6.intValue());

    // todo: create test data for node touching polygon
    // todo: create tests for relations
  }

  // Neighbouring ------------------------------------------------------------------------------
  @Test
  public void test_neighbouring() throws Exception {
    Integer result = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 130530843)
        .neighbourhood(12.,
            mapReduce -> mapReduce.osmType(OSMType.NODE).collect())
        .flatMap(x -> x.getRight())
        .count();
    assertEquals(4, result.intValue());
  }

  // ---------------------------------------------------------------------------------------------
  // CONTRIBUTIONS
  // ---------------------------------------------------------------------------------------------

  // CONTAINS --------------------------------------------------------------------------------------
  @Test
  public void test_contribution_contains() throws Exception {
    // Create MapReducer
    Integer result = OSMContributionView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps1)
        .areaOfInterest(bbox)
        .contains(MapReduce -> MapReduce.collect())
        .count();
    assertEquals(38, result.intValue());
  }

  // COVERED BY------------------------------------------------------------------------------------
  @Test
  public void test_contributions_covered_by_snapshots() throws Exception {
  }

  // COVERS ---------------------------------------------------------------------------------
  @Test
  public void test_contributions_covering_snapshots() throws Exception {
    Integer result = OSMContributionView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps13)
        .areaOfInterest(bbox)
        .covers(MapReduce -> MapReduce.collect())
        .count();
    assertEquals(1, result.intValue());
  }

  // EQUALS --------------------------------------------------------------------------------------
  @Test
  public void test_contributions_by_equals_snapshots() throws Exception {
    // Create MapReducer
    Integer result = OSMContributionView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps1)
        .areaOfInterest(bbox)
        .equalFeatures(mapReduce -> mapReduce.collect())
        .filter(x -> x.getRight().size() > 0)
        .count();
    assertEquals(1, result.intValue());
  }

  // INSIDE --------------------------------------------------------------------------------------
  @Test
  public void test_node_contributions_inside_polygon() throws Exception {
    Integer result = OSMContributionView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps10)
        .areaOfInterest(bbox)
        .osmType(OSMType.NODE)
        .inside(
            mapReduce -> mapReduce
                .filter(x -> ((OSMEntitySnapshot) x).getEntity().getId() == 130518827).collect())
        .count();
    assertEquals(1, result.intValue());
  }

  // OVERLAPS --------------------------------------------------------------------------------------
  @Test
  public void test_line_contribution_overlapping_polygon() throws Exception {
    Integer result = OSMContributionView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps10)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .overlaps(
            mapReduce -> mapReduce
                .filter(x -> ((OSMEntitySnapshot) x).getEntity().getId() == 172510842).collect())
        .count();
    assertEquals(3, result.intValue());
  }

  // NEIGHBOURING --------------------------------------------------------------------------------------
  @Test
  public void test_contributions_by_neighbouring_snapshots() throws Exception {
    // Create MapReducer
    Integer result = OSMContributionView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps10)
        .areaOfInterest(bbox)
        .osmTag("building")
        .neighbouring(40., mapReduce -> mapReduce.filter(x -> ((OSMEntitySnapshot) x).getEntity().getId() == 172510827).collect())
        .filter(x -> x.getContributionTypes().contains(ContributionType.CREATION))
        .count();
    assertEquals(4, result.intValue());
  }

  // TOUCHES --------------------------------------------------------------------------------------
  @Test
  public void test_contributions_by_touching_snapshots() throws Exception {
    // Create MapReducer
    Integer result = OSMContributionView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps10)
        .areaOfInterest(bbox)
        .osmTag("building")
        .touches(mapReduce -> mapReduce.filter(x -> ((OSMEntitySnapshot) x).getEntity().getId() == 172510843).collect())
        .filter(x -> x.getContributionTypes().contains(ContributionType.CREATION))
        .count();
    assertEquals(2, result.intValue());
  }

}
