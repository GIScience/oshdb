package org.heigit.ohsome.oshdb.util.geometry;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.geometry.helpers.FakeTagInterpreterAreaAlways;
import org.heigit.ohsome.oshdb.util.geometry.helpers.FakeTagInterpreterAreaMultipolygonAllOuters;
import org.heigit.ohsome.oshdb.util.geometry.helpers.FakeTagInterpreterAreaNever;
import org.heigit.ohsome.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.ohsome.oshdb.util.xmlreader.OSMXmlReader;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * Tests the {@link OSHDBGeometryBuilder} class.
 */
class OSHDBGeometryBuilderTest {

  private final OSMXmlReader testData = new OSMXmlReader();
  private static final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTest() {
    testData.add("./src/test/resources/geometryBuilder.osh");
  }

  @Test
  void testPointGetGeometry() {
    OSMEntity entity = testData.nodes().get(1L).get(1);
    OSHDBTimestamp timestamp = TimestampParser.toOSHDBTimestamp("2001-01-01");
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, null);
    assertTrue(result instanceof Point);
    assertEquals(100, result.getCoordinates()[0].x, DELTA);
    assertEquals(80, result.getCoordinates()[0].y, DELTA);
  }

  @Test
  void testPointGetGeometryClipped() {
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
  void testWayGetGeometry() {
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
  void testWayGetGeometryIncomplete() {
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
  void testRelationGetGeometry() {
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
  void testRelationGetGeometryIncomplete() {
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
}
