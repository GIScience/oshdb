package org.heigit.ohsome.oshdb.util.geometry;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.util.geometry.helpers.FakeTagInterpreterAreaAlways;
import org.heigit.ohsome.oshdb.util.geometry.helpers.FakeTagInterpreterAreaMultipolygonAllOuters;
import org.heigit.ohsome.oshdb.util.geometry.helpers.FakeTagInterpreterAreaNever;
import org.heigit.ohsome.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.ohsome.oshdb.util.xmlreader.OSMXmlReader;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * Tests the {@link OSHDBGeometryBuilder} class.
 */
public class OSHDBGeometryBuilderTest {

  private final OSMXmlReader testData = new OSMXmlReader();
  private static final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTest() {
    testData.add("./src/test/resources/geometryBuilder.osh");
    testData.add("./src/test/resources/relations/multipolygonShellsShareNode.osm");
  }

  @Test
  public void testPointGetGeometry() {
    OSMEntity entity = testData.nodes().get(1L).get(1);
    OSHDBTimestamp timestamp = TimestampParser.toOSHDBTimestamp("2001-01-01");
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, null);
    assertTrue(result instanceof Point);
    assertEquals(100, result.getCoordinates()[0].x, DELTA);
    assertEquals(80, result.getCoordinates()[0].y, DELTA);
  }

  @Test
  public void testPointGetGeometryClipped() {
    OSMEntity entity = testData.nodes().get(1L).get(1);
    OSHDBTimestamp timestamp = TimestampParser.toOSHDBTimestamp("2001-01-01");
    // by bbox
    OSHDBBoundingBox clipBbox = bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0);
    Geometry result = OSHDBGeometryBuilder.getGeometryClipped(entity, timestamp, null, clipBbox);
    assertFalse(result.isEmpty());
    clipBbox = bboxWgs84Coordinates(-180.0, -90.0, 0.0, 0.0);
    result = OSHDBGeometryBuilder.getGeometryClipped(entity, timestamp, null, clipBbox);
    assertTrue(result.isEmpty());
    // by poly
    Polygon clipPoly =
        OSHDBGeometryBuilder.getGeometry(bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0));
    result = OSHDBGeometryBuilder.getGeometryClipped(entity, timestamp, null, clipPoly);
    assertFalse(result.isEmpty());
    clipPoly = OSHDBGeometryBuilder.getGeometry(bboxWgs84Coordinates(-1.0, -1.0, 1.0, 1.0));
    result = OSHDBGeometryBuilder.getGeometryClipped(entity, timestamp, null, clipPoly);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testWayGetGeometry() {
    // linestring
    OSMEntity entity = testData.ways().get(1L).get(0);
    OSHDBTimestamp timestamp = TimestampParser.toOSHDBTimestamp("2001-01-01");
    TagInterpreter areaDecider = new FakeTagInterpreterAreaNever();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("LineString", result.getGeometryType());
    assertEquals(2, result.getNumPoints());
    assertEquals(100, result.getCoordinates()[0].x, DELTA);
    assertEquals(80, result.getCoordinates()[0].y, DELTA);
    assertEquals(110, result.getCoordinates()[1].x, DELTA);
    assertEquals(80.1, result.getCoordinates()[1].y, DELTA);

    // polygon
    entity = testData.ways().get(2L).get(0);
    areaDecider = new FakeTagInterpreterAreaAlways();
    result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("Polygon", result.getGeometryType());
    assertEquals(5, result.getNumPoints());
    assertEquals(result.getCoordinates()[0].x, result.getCoordinates()[4].x, DELTA);
    assertEquals(result.getCoordinates()[0].y, result.getCoordinates()[4].y, DELTA);

    // other timestamp -> changed member
    entity = testData.ways().get(1L).get(0);
    timestamp = TimestampParser.toOSHDBTimestamp("2003-01-01");
    areaDecider = new FakeTagInterpreterAreaNever();
    result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals(80.2, result.getCoordinates()[0].y, DELTA);
  }

  @Test
  public void testWayGetGeometryIncomplete() {
    // linestring with 3 node references, only 2 present
    OSMEntity entity = testData.ways().get(3L).get(0);
    OSHDBTimestamp timestamp = TimestampParser.toOSHDBTimestamp("2001-01-01");
    TagInterpreter areaDecider = new FakeTagInterpreterAreaNever();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("LineString", result.getGeometryType());
    assertEquals(2, result.getNumPoints());

    // single noded way
    entity = testData.ways().get(4L).get(0);
    result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("Point", result.getGeometryType());
    assertFalse(result.isEmpty());

    // zero noded way
    entity = testData.ways().get(5L).get(0);
    result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testRelationGetGeometry() {
    // simplest multipolygon
    OSMEntity entity = testData.relations().get(1L).get(0);
    OSHDBTimestamp timestamp = TimestampParser.toOSHDBTimestamp("2001-01-01");
    TagInterpreter areaDecider = new FakeTagInterpreterAreaMultipolygonAllOuters();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("Polygon", result.getGeometryType());
    assertEquals(5, result.getNumPoints());

    // other relation
    areaDecider = new FakeTagInterpreterAreaNever();
    result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("GeometryCollection", result.getGeometryType());
    assertEquals(1, result.getNumGeometries());
    assertEquals(5, result.getGeometryN(0).getNumPoints());
  }

  @Test
  public void testRelationGetGeometryIncomplete() {
    // multipolygon, missing ring
    OSMEntity entity = testData.relations().get(2L).get(0);
    OSHDBTimestamp timestamp = TimestampParser.toOSHDBTimestamp("2001-01-01");
    TagInterpreter areaDecider = new FakeTagInterpreterAreaMultipolygonAllOuters();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("Polygon", result.getGeometryType());
    assertEquals(1, result.getNumGeometries());

    // multipolygon, incomplete ring
    entity = testData.relations().get(3L).get(0);
    result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("GeometryCollection", result.getGeometryType());
    assertEquals(1, result.getNumGeometries());

    // other relation, missing member
    areaDecider = new FakeTagInterpreterAreaNever();
    result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("GeometryCollection", result.getGeometryType());
    assertEquals(1, result.getNumGeometries());
  }

  @Test
  public void testBoundingGetGeometry() throws ParseException {
    Polygon clipPoly =
        OSHDBGeometryBuilder.getGeometry(bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0));
    Geometry expectedPolygon = new WKTReader().read(
        "POLYGON((-180.0 -90.0, 180.0 -90.0, 180.0 90.0, -180.0 90.0, -180.0 -90.0))"
    );
    assertEquals(expectedPolygon, clipPoly);
  }

  @Test
  public void testBoundingBoxOf() {
    OSHDBBoundingBox bbox = OSHDBGeometryBuilder.boundingBoxOf(new Envelope(-180, 180, -90, 90));
    assertEquals("(-180.0000000,-90.0000000,180.0000000,90.0000000)", bbox.toString());
  }

  @Test
  public void testBoundingBoxGetGeometry() {
    // regular bbox
    OSHDBBoundingBox bbox = bboxWgs84Coordinates(0.0, 0.0, 1.0, 1.0);
    Polygon geometry = OSHDBGeometryBuilder.getGeometry(bbox);
    Coordinate[] test = {
      new Coordinate(0, 0),
      new Coordinate(1, 0),
      new Coordinate(1, 1),
      new Coordinate(0, 1),
      new Coordinate(0, 0)};
    Assert.assertArrayEquals(test, geometry.getCoordinates());

    // degenerate bbox: point
    bbox = bboxWgs84Coordinates(0.0, 0.0, 0.0, 0.0);
    geometry = OSHDBGeometryBuilder.getGeometry(bbox);
    test = new Coordinate[]{
      new Coordinate(0, 0),
      new Coordinate(0, 0),
      new Coordinate(0, 0),
      new Coordinate(0, 0),
      new Coordinate(0, 0)};
    Assert.assertArrayEquals(test, geometry.getCoordinates());

    // degenerate bbox: line
    bbox = bboxWgs84Coordinates(0.0, 0.0, 0.0, 1.0);
    geometry = OSHDBGeometryBuilder.getGeometry(bbox);
    test = new Coordinate[]{
      new Coordinate(0, 0),
      new Coordinate(0, 0),
      new Coordinate(0, 1),
      new Coordinate(0, 1),
      new Coordinate(0, 0)};
    Assert.assertArrayEquals(test, geometry.getCoordinates());
  }

  /** Test building of multipolygons with self-touching outer rings.
   *
   * <p>Example:
   * <pre>
   * lat
   *  ^
   *  |
   *  4 a--d
   *  3 |  |
   *  2 b--c--e
   *  1    |  |
   *  0    g--f
   *    0 1 2 3456 -> lon
   *
   * A: (a,b,c)\
   *            one shell (a,b,c,d,a)
   * B: (c,d,a)/
   *
   * C: (c,e,f)\
   *            one shell (c,e,f,g,c)
   * D: (f,g,c)/
   * </pre></p>
   *
   * <p>Expected:
   * <pre>
   * - MULTIPOLYGON (((0 4, 0 2, 3 2, 3 4, 0 4)), ((3 2, 6 2, 6 0, 3 0, 3 2)))
   * - MULTIPOLYGON (((3 2, 6 2, 6 0, 3 0, 3 2)), ((0 4, 0 2, 3 2, 3 4, 0 4)))
   * - or similar
   * </pre></p>
   */
  @Test
  public void testMultipolygonShellsShareNode() {
    // single figure-8
    var members = testData.relations().get(100L).get(0).getMembers();
    getMultipolygonSharedNodeCheck(2).accept(members);
    // all permutations of relation members should lead to the same result
    members = testData.relations().get(101L).get(0).getMembers();
    permutations(members.length, members, getMultipolygonSharedNodeCheck(2));
    // double loop
    members = testData.relations().get(102L).get(0).getMembers();
    getMultipolygonSharedNodeCheck(3).accept(members);
    // second loop forming whole
    members = testData.relations().get(103L).get(0).getMembers();
    getMultipolygonSharedNodeCheck(2).accept(members);
    // todo: more complex cases: * multiple holes, hole in hole
  }

  private Consumer<OSMMember[]> getMultipolygonSharedNodeCheck(int expectedParts) {
    var areaDecider = new FakeTagInterpreterAreaMultipolygonAllOuters();
    var timestamp = TimestampParser.toOSHDBTimestamp("2001-01-01");
    return relMembers -> {
      var relation = new OSMRelation(1, 1, timestamp.getEpochSecond(), 0, 0, null, relMembers);
      var geom = OSHDBGeometryBuilder.getGeometry(relation, timestamp, areaDecider);
      assertTrue(geom.isValid());
      assertTrue(geom instanceof MultiPolygon);
      assertEquals(expectedParts, geom.getNumGeometries());
    };
  }

  private static <T> void permutations(int n, T[] elements, Consumer<T[]> consumer) {
    if (n == 1) {
      consumer.accept(elements);
    } else {
      for (int i = 0; i < n - 1; i++) {
        permutations(n - 1, elements, consumer);
        if (n % 2 == 0) {
          swap(elements, i, n - 1);
        } else {
          swap(elements, 0, n - 1);
        }
      }
      permutations(n - 1, elements, consumer);
    }
  }

  private static <T> void swap(T[] elements, int a, int b) {
    T tmp = elements[a];
    elements[a] = elements[b];
    elements[b] = tmp;
  }
}
