package org.heigit.ohsome.oshdb.util.geometry;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Consumer;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osm.OSM;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.util.geometry.helpers.FakeTagInterpreterAreaAlways;
import org.heigit.ohsome.oshdb.util.geometry.helpers.FakeTagInterpreterAreaMultipolygonAllOuters;
import org.heigit.ohsome.oshdb.util.geometry.helpers.FakeTagInterpreterAreaNever;
import org.heigit.ohsome.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * Tests the {@link OSHDBGeometryBuilder} class.
 */
class OSHDBGeometryBuilderTest extends OSHDBGeometryTest {
  private static final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTest() {
    super(
        "./src/test/resources/geometryBuilder.osh",
        "./src/test/resources/relations/multipolygonShellsShareNode.osm"
    );
  }

  @Test
  void testPointGetGeometry() {
    OSMEntity entity = nodes(1L, 1);
    OSHDBTimestamp timestamp = TimestampParser.toOSHDBTimestamp("2001-01-01");
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, null);
    assertTrue(result instanceof Point);
    assertEquals(100, result.getCoordinates()[0].x, DELTA);
    assertEquals(80, result.getCoordinates()[0].y, DELTA);
  }

  @Test
  void testPointGetGeometryClipped() {
    OSMEntity entity = nodes(1L, 1);
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
  void testWayGetGeometry() {
    // linestring
    OSMEntity entity = ways(1L, 0);
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
    entity = ways(2L, 0);
    areaDecider = new FakeTagInterpreterAreaAlways();
    result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("Polygon", result.getGeometryType());
    assertEquals(5, result.getNumPoints());
    assertEquals(result.getCoordinates()[0].x, result.getCoordinates()[4].x, DELTA);
    assertEquals(result.getCoordinates()[0].y, result.getCoordinates()[4].y, DELTA);

    // other timestamp -> changed member
    entity = ways(1L, 0);
    timestamp = TimestampParser.toOSHDBTimestamp("2003-01-01");
    areaDecider = new FakeTagInterpreterAreaNever();
    result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals(80.2, result.getCoordinates()[0].y, DELTA);
  }

  @Test
  void testWayGetGeometryIncomplete() {
    // linestring with 3 node references, only 2 present
    OSMEntity entity = ways(3L, 0);
    OSHDBTimestamp timestamp = TimestampParser.toOSHDBTimestamp("2001-01-01");
    TagInterpreter areaDecider = new FakeTagInterpreterAreaNever();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("LineString", result.getGeometryType());
    assertEquals(2, result.getNumPoints());

    // single noded way
    entity = ways(4L, 0);
    result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("Point", result.getGeometryType());
    assertFalse(result.isEmpty());

    // zero noded way
    entity = ways(5L, 0);
    result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result.isEmpty());
  }

  @Test
  void testRelationGetGeometry() {
    // simplest multipolygon
    OSMEntity entity = relations(1L, 0);
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
  void testRelationGetGeometryIncomplete() {
    // multipolygon, missing ring
    OSMEntity entity = relations(2L, 0);
    OSHDBTimestamp timestamp = TimestampParser.toOSHDBTimestamp("2001-01-01");
    TagInterpreter areaDecider = new FakeTagInterpreterAreaMultipolygonAllOuters();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("Polygon", result.getGeometryType());
    assertEquals(1, result.getNumGeometries());

    // multipolygon, incomplete ring
    entity = relations(3L, 0);
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
  void testBoundingGetGeometry() throws ParseException {
    Polygon clipPoly =
        OSHDBGeometryBuilder.getGeometry(bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0));
    Geometry expectedPolygon = new WKTReader().read(
        "POLYGON((-180.0 -90.0, 180.0 -90.0, 180.0 90.0, -180.0 90.0, -180.0 -90.0))"
    );
    assertEquals(expectedPolygon, clipPoly);
  }

  @Test
  void testBoundingBoxOf() {
    OSHDBBoundingBox bbox = OSHDBGeometryBuilder.boundingBoxOf(new Envelope(-180, 180, -90, 90));
    assertEquals("(-180.0000000,-90.0000000,180.0000000,90.0000000)", bbox.toString());
  }

  @Test
  void testBoundingBoxGetGeometry() {
    // regular bbox
    OSHDBBoundingBox bbox = bboxWgs84Coordinates(0.0, 0.0, 1.0, 1.0);
    Polygon geometry = OSHDBGeometryBuilder.getGeometry(bbox);
    Coordinate[] test = {
      new Coordinate(0, 0),
      new Coordinate(1, 0),
      new Coordinate(1, 1),
      new Coordinate(0, 1),
      new Coordinate(0, 0)};
    assertArrayEquals(test, geometry.getCoordinates());

    // degenerate bbox: point
    bbox = bboxWgs84Coordinates(0.0, 0.0, 0.0, 0.0);
    geometry = OSHDBGeometryBuilder.getGeometry(bbox);
    test = new Coordinate[]{
      new Coordinate(0, 0),
      new Coordinate(0, 0),
      new Coordinate(0, 0),
      new Coordinate(0, 0),
      new Coordinate(0, 0)};
    assertArrayEquals(test, geometry.getCoordinates());

    // degenerate bbox: line
    bbox = bboxWgs84Coordinates(0.0, 0.0, 0.0, 1.0);
    geometry = OSHDBGeometryBuilder.getGeometry(bbox);
    test = new Coordinate[]{
      new Coordinate(0, 0),
      new Coordinate(0, 0),
      new Coordinate(0, 1),
      new Coordinate(0, 1),
      new Coordinate(0, 0)};
    assertArrayEquals(test, geometry.getCoordinates());
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
    getMultipolygonSharedNodeCheck(geom -> {
      assertTrue(geom instanceof MultiPolygon);
      assertEquals(2, geom.getNumGeometries());
    }).accept(members);
    // all permutations of relation members should lead to the same result
    members = testData.relations().get(101L).get(0).getMembers();
    checkAllMemberPermutations(members.length, members, getMultipolygonSharedNodeCheck(geom -> {
      assertTrue(geom instanceof MultiPolygon);
      assertEquals(2, geom.getNumGeometries());
    }));
    // double loop
    members = testData.relations().get(102L).get(0).getMembers();
    getMultipolygonSharedNodeCheck(geom -> {
      assertTrue(geom instanceof MultiPolygon);
      assertEquals(3, geom.getNumGeometries());
    }).accept(members);
    // second loop forming whole
    members = testData.relations().get(103L).get(0).getMembers();
    getMultipolygonSharedNodeCheck(geom -> {
      assertTrue(geom instanceof Polygon);
      assertEquals(1, ((Polygon) geom).getNumInteriorRing());
    }).accept(members);
    // multiple holes
    members = testData.relations().get(104L).get(0).getMembers();
    getMultipolygonSharedNodeCheck(geom -> {
      assertTrue(geom instanceof Polygon);
      assertEquals(2, ((Polygon) geom).getNumInteriorRing());
    }).accept(members);
    // hole in hole = additional outer
    members = testData.relations().get(105L).get(0).getMembers();
    checkAllMemberPermutations(members.length, members, getMultipolygonSharedNodeCheck(geom -> {
      assertTrue(geom instanceof MultiPolygon);
      assertEquals(2, geom.getNumGeometries());
      assertEquals(1,
          ((Polygon) geom.getGeometryN(0)).getNumInteriorRing()
              + ((Polygon) geom.getGeometryN(1)).getNumInteriorRing()
      );
    }));
  }

  public Consumer<OSMMember[]> getMultipolygonSharedNodeCheck(Consumer<Geometry> tester) {
    var areaDecider = new FakeTagInterpreterAreaMultipolygonAllOuters();
    var timestamp = TimestampParser.toOSHDBTimestamp("2001-01-01");
    return relMembers -> {
      var relation = OSM.relation(1, 1, timestamp.getEpochSecond(), 0, 0, null, relMembers);
      var geom = OSHDBGeometryBuilder.getGeometry(relation, timestamp, areaDecider);
      assertTrue(geom.isValid());
      tester.accept(geom);
    };
  }

  public static <T> void checkAllMemberPermutations(int n, T[] elements, Consumer<T[]> consumer) {
    if (n == 1) {
      consumer.accept(elements);
    } else {
      for (int i = 0; i < n - 1; i++) {
        checkAllMemberPermutations(n - 1, elements, consumer);
        if (n % 2 == 0) {
          swap(elements, i, n - 1);
        } else {
          swap(elements, 0, n - 1);
        }
      }
      checkAllMemberPermutations(n - 1, elements, consumer);
    }
  }

  private static <T> void swap(T[] elements, int a, int b) {
    T tmp = elements[a];
    elements[a] = elements[b];
    elements[b] = tmp;
  }


}
