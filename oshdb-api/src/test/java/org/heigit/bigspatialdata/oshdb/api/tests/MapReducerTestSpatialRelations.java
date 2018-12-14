package org.heigit.bigspatialdata.oshdb.api.tests;

import static org.junit.Assert.*;

import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps.Interval;
import org.junit.Test;

public class MapReducerTestSpatialRelations {

  private final OSHDBJdbc oshdb;

  // create bounding box from coordinates
  //private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(85.3406012, 27.6991942, 85.3585444, 27.7121143);
  private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8, 49, 9, 50);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2014-01-01", "2014-12-31", Interval.YEARLY);
  private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps("2014-01-01", "2015-12-31", Interval.MONTHLY);
  private final OSHDBTimestamps timestamps3 = new OSHDBTimestamps("2015-01-01", "2015-12-31", Interval.MONTHLY);
  private final OSHDBTimestamps timestamps2017 = new OSHDBTimestamps("2017-01-01");

  public MapReducerTestSpatialRelations() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data").multithreading(true);
  }

  @Test
  public void test_covers() {

  }

  @Test
  public void test_touchingElements() throws Exception {

    List<Pair<OSMEntitySnapshot, List<Object>>> result = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .osmTag("building")
        .filter(x -> x.getEntity().getId() == 130518815)
        .touchingElements(
            mapReduce -> mapReduce.osmTag("building").collect())
        .collect();
    assertEquals(1, result.get(0).getRight().size());

    List<Pair<OSMEntitySnapshot, List<Object>>> result2 = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .osmTag("building")
        .filter(x -> x.getEntity().getId() == 172510837)
        .touchingElements(
            mapReduce -> mapReduce.osmTag("building").collect())
        .collect();
    assertEquals(2, result2.get(0).getRight().size());
  }

  @Test
  public void test_touches() throws Exception {

    // Polygon touching polygon
    Integer result = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .osmTag("building")
        .touches(
            mapReduce -> mapReduce.osmTag("building").collect())
        .count();
    assertEquals(41, result.intValue());

    // Line touching point
    Integer result2 = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 36493984)
        .touchingElements(
            mapReduce -> mapReduce.osmType(OSMType.NODE).collect())
        .count();
    assertEquals(1, result2.intValue());

    // Line touching lines
    Integer result3 = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 36493984)
        .touchingElements(
            mapReduce -> mapReduce.osmType(OSMType.WAY).collect())
        .flatMap(x -> x.getRight())
        .count();
    assertEquals(3, result3.intValue());
  }

  @Test
  public void test_nodes_inside_line() throws Exception {
    Integer result = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 36493984)
        .containment(
            mapReduce -> mapReduce.osmType(OSMType.NODE).collect())
        .flatMap(x -> x.getRight())
        .count();
    assertEquals(3, result.intValue());
  }

  @Test
  public void test_line_overlaps_line() throws Exception {
    Integer result = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 36493984)
        .overlap(
            mapReduce -> mapReduce.osmType(OSMType.WAY).collect())
        .flatMap(x -> x.getRight())
        .count();
    assertEquals(2, result.intValue());
  }

  @Test
  public void test_line_overlaps_polygon() throws Exception {
    Integer result = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 172510842)
        .overlap(
            mapReduce -> mapReduce.osmType(OSMType.WAY).collect())
        .flatMap(x -> x.getRight())
        .count();
    assertEquals(2, result.intValue());
  }

  @Test
  public void test_line_touches_polygon() throws Exception {
    Integer result = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 172510842)
        .touchingElements(
            mapReduce -> mapReduce.osmType(OSMType.WAY).collect())
        .flatMap(x -> x.getRight())
        .count();
    assertEquals(1, result.intValue());
  }

  @Test
  public void test_contains_inside() throws Exception {

    Integer result_containment = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .osmTag("building")
        .containment(
            mapReduce -> mapReduce.osmType(OSMType.NODE).collect())
        .flatMap(x -> x.getRight())
        .count();

    Integer result_inside = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.NODE)
        .inside(
            mapReduce -> mapReduce.osmType(OSMType.WAY).osmTag("building").collect())
        .count();

    assertEquals(result_inside, result_containment);

    System.out.println("Count: " + result_containment);
  }

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
}