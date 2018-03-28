package org.heigit.bigspatialdata.oshdb.util.geometry.osmtestdata;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;

import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.bigspatialdata.oshdb.util.test.OSMXmlReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class OSHDBGeometryBuilderTestOsmTestData7xx {
  private final OSMXmlReader testData = new OSMXmlReader();
  private final OSMXmlReaderTagInterpreter tagInterpreter;
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");
  private final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestOsmTestData7xx() {
    testData.add("./src/test/resources/osm-testdata/all.osm");
    tagInterpreter = new OSMXmlReaderTagInterpreter(testData);
  }

  @Test
  public void test700() {
    // Polygon with one closed way.
    OSMEntity entity = testData.ways().get(700800L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    // check if coordinates of polygon are the correct ones
    assertEquals(7.01, ((Polygon) result).getExteriorRing().getCoordinateN(0).x, DELTA);
    assertEquals(1.01, ((Polygon) result).getExteriorRing().getCoordinateN(0).y, DELTA);
    assertEquals(7.01, ((Polygon) result).getExteriorRing().getCoordinateN(1).x, DELTA);
    assertEquals(1.04, ((Polygon) result).getExteriorRing().getCoordinateN(1).y, DELTA);
    assertEquals(7.04, ((Polygon) result).getExteriorRing().getCoordinateN(2).x, DELTA);
    assertEquals(1.04, ((Polygon) result).getExteriorRing().getCoordinateN(2).y, DELTA);
    assertEquals(7.04, ((Polygon) result).getExteriorRing().getCoordinateN(3).x, DELTA);
    assertEquals(1.01, ((Polygon) result).getExteriorRing().getCoordinateN(3).y, DELTA);
    assertEquals(7.01, ((Polygon) result).getExteriorRing().getCoordinateN(4).x, DELTA);
    assertEquals(1.01, ((Polygon) result).getExteriorRing().getCoordinateN(4).y, DELTA);

    // check if result has 5 points
    assertEquals(5, result.getCoordinates().length, DELTA);

  }

  @Test
  public void test701() throws ParseException {
    // Valid multipolygon relation with two ways (4 points) making up an outer ring.
    OSMEntity entity = testData.relations().get(701900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);

    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(5, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.14 1.01,7.11 1.01,7.11 1.04,7.14 1.04,7.14 1.01)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }
  @Test
  public void test702() throws ParseException {
    // Valid multipolygon relation with two ways (8 points) making up an outer ring."
    OSMEntity entity = testData.relations().get(702900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);

    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(9, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.24 1.01,7.21 1.01,7.21 1.02,7.23 1.03,7.23 1.04,7.21 1.04,7.21 1.05,7.24 1.05,7.24 1.01)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }
  @Test
  public void test703() throws ParseException {
    // Valid multipolygon relation with two ways (8 points) making up an outer ring."
    OSMEntity entity = testData.relations().get(703900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);

    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(9, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.34 1.01,7.31 1.01,7.31 1.02,7.33 1.03,7.33 1.04,7.32 1.04,"
            + "7.32 1.05,7.34 1.05,7.34 1.01)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }
  @Test
  public void test704() throws ParseException {
    // Valid multipolygon relation with three ways making up an outer ring in the form of a cross.
    OSMEntity entity = testData.relations().get(704900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);

    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(13, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.41 1.02,7.41 1.03,7.42 1.03,7.42 1.04,7.43 1.04,7.43 1.03,"
            + "7.44 1.03,7.44 1.02,7.43 1.02,7.43 1.01,7.42 1.01,7.42 1.02,7.41 1.02)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test705() throws ParseException {
    // Valid multipolygon relation with three ways making up an outer ring. Contains concave and convex parts.
    OSMEntity entity = testData.relations().get(705900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);

    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(14, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.58 1.02,7.56 1.03,7.56 1.04,7.55 1.04,7.54 1.01,7.52 1.01,"
            + "7.53 1.03,7.51 1.04,7.52 1.08,7.54 1.07,7.55 1.09,7.56 1.09,7.59 1.06,7.58 1.02)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test706() throws ParseException {
    // Valid multipolygon relation with three ways making up two outer rings that touch in one point.
    OSMEntity entity = testData.relations().get(706900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof MultiPolygon);
    assertEquals(2,result.getNumGeometries());
    assertEquals(11, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.61 1.04,7.62 1.06,7.65 1.05,7.64 1.03,7.63 1.02,7.61 1.04)),"
            + "((7.64 1.03,7.67 1.03,7.67 1.01,7.64 1.01,7.64 1.03)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test707() throws ParseException {
    // Valid multipolygon relation with three ways making up two separate outer rings.
    OSMEntity entity = testData.relations().get(707900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof MultiPolygon);
    assertEquals(2,result.getNumGeometries());
    assertEquals(10, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.71 1.04,7.72 1.06,7.75 1.05,7.73 1.02,7.71 1.04)),"
            + "((7.74 1.03,7.77 1.03,7.77 1.01,7.74 1.01,7.74 1.03)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test708() throws ParseException {
    // Valid multipolygon relation with three ways making up two separate outer rings.
    OSMEntity entity = testData.relations().get(708900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof MultiPolygon);
    assertEquals(2,result.getNumGeometries());
    assertEquals(18, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.81 1.03,7.82 1.06,7.85 1.06,7.86 1.03,7.85 1.03,7.84 1.05,"
            + "7.83 1.05,7.82 1.02,7.81 1.03)),((7.83 1.04,7.84 1.04,7.84 1.02,7.87 1.02,7.87 1.03,"
            + "7.88 1.03,7.88 1.01,7.83 1.01,7.83 1.04)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test709() throws ParseException {
    // Valid multipolygon relation with four ways making up three outer rings touching in three points.
    OSMEntity entity = testData.relations().get(709900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof MultiPolygon);
    assertEquals(3,result.getNumGeometries());
    assertEquals(15, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.91 1.04,7.92 1.06,7.95 1.05,7.94 1.03,7.93 1.02,7.91 1.04)),"
            + "((7.94 1.03,7.97 1.03,7.97 1.01,7.94 1.01,7.94 1.03)),"
            + "((7.95 1.05,7.97 1.03,7.98 1.08,7.95 1.05)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test720() throws ParseException {
    // "Multipolygon with one outer and one inner ring. They are both oriented clockwise and have the correct role.
    OSMEntity entity = testData.relations().get(720900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertEquals(1, ((Polygon) result).getNumInteriorRing());
    assertEquals(1,result.getNumGeometries());
    assertEquals(10, result.getCoordinates().length, DELTA);
    //assertEquals(10, entity., DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.05 1.21,7.01 1.21,7.01 1.25,7.05 1.25,7.05 1.21),"
            + "(7.04 1.22,7.02 1.22,7.02 1.24,7.04 1.24,7.04 1.22)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test721() throws ParseException {
    // "Multipolygon with one outer and one inner ring. They are both oriented anti-clockwise and have the correct role.
    // the same as test(720) apart from anti-clockwise
  }

  @Test
  public void test722() throws ParseException {
    // Multipolygon with one outer and one inner ring. The outer ring is oriented clockwise, the inner anti-clockwise. They have both the correct role.
    // the same as test(720) apart from anti-clockwise
  }

  @Test
  public void test723() throws ParseException {
    // Multipolygon with one outer and one inner ring. The outer ring is oriented anti-clockwise, the inner clockwise. They have both the correct role
    // the same as test(722) apart from anti-clockwise
  }

  @Test
  public void test724() throws ParseException {
    // Multipolygon with one outer and one inner ring and a bit more complex geometry and nodes not in order
    OSMEntity entity = testData.relations().get(724900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertEquals(1, ((Polygon) result).getNumInteriorRing());
    assertEquals(1,result.getNumGeometries());
    assertEquals(14, result.getCoordinates().length, DELTA);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.44 1.22,7.47 1.21,7.41 1.21,7.42 1.22,7.41 1.24,7.43 1.26,7.46 1.26,7.45 1.23,7.44 1.22),"
            + "(7.43 1.22,7.42 1.24,7.44 1.25,7.45 1.24,7.43 1.22)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test725() throws ParseException {
    // Valid multipolygon with one concave outer ring and no inner ring.
    OSMEntity entity = testData.relations().get(725900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(1,result.getNumGeometries());
    assertEquals(7, result.getCoordinates().length, DELTA);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.53 1.21,7.54 1.21,7.52 1.23,7.54 1.25,7.53 1.25,7.51 1.23,7.53 1.21)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }
  @Test
  public void test726() throws ParseException {
    // Valid multipolygon with one inner and one outer
    // the same as test(724) apart from anti-clockwise
  }
  @Test
  public void test727() throws ParseException {
    // Valid multipolygon with one inner and one outer
    // the same as test(724) apart from anti-clockwise
  }
  @Test
  public void test728() throws ParseException {
    // Multipolygon with one simple outer ring and a node member
    OSMEntity entity = testData.relations().get(728900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(1,result.getNumGeometries());
    assertEquals(9, result.getCoordinates().length, DELTA);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.85 1.23,7.86 1.22,7.87 1.22,7.87 1.24,7.86 1.25,7.83 1.25,7.82 1.26,7.84 1.23,7.85 1.23)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }
  @Test
  public void test729() throws ParseException {
    // Valid multipolygon with second outer ring in inner ring.
    OSMEntity entity = testData.relations().get(729900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof MultiPolygon);

    assertFalse((result.getGeometryN(1)).intersects((result.getGeometryN(0))));
    assertEquals(0, ((Polygon)result.getGeometryN(0)).getNumInteriorRing());
    assertEquals(1, ((Polygon)result.getGeometryN(1)).getNumInteriorRing());
    assertEquals(2,result.getNumGeometries());
    assertEquals(15, result.getCoordinates().length, DELTA);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.91 1.21,7.91 1.29,7.99 1.29,7.99 1.21,7.91 1.21),"
            + "(7.97 1.27,7.97 1.23,7.93 1.23,7.93 1.27,7.97 1.27)),"
            + "((7.96 1.26,7.94 1.26,7.94 1.24,7.96 1.24,7.96 1.26)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test730() throws ParseException {
    // Valid multipolygon with one outer and three inner rings with correct roles
    OSMEntity entity = testData.relations().get(730900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);

    assertEquals(3, ((Polygon)result).getNumInteriorRing());
    assertEquals(1,result.getNumGeometries());
    assertEquals(21, result.getCoordinates().length, DELTA);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.06 1.31,7.01 1.31,7.01 1.34,7.04 1.37,7.06 1.34,7.06 1.31),"
            + "(7.02 1.33,7.03 1.33,7.03 1.32,7.02 1.32,7.02 1.33),"
            + "(7.03 1.35,7.04 1.35,7.04 1.34,7.03 1.34,7.03 1.35),"
            + "(7.05 1.33,7.04 1.33,7.04 1.32,7.05 1.32,7.05 1.33)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }
  @Test
  public void test731() throws ParseException {
    // "Valid complex multipolygon with one outer and two inner rings made up of several ways. Roles are tagged correctly
    OSMEntity entity = testData.relations().get(731900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);

    assertEquals(2, ((Polygon)result).getNumInteriorRing());
    assertEquals(1,result.getNumGeometries());
    assertEquals(25, result.getCoordinates().length, DELTA);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.18 1.33,7.17 1.31,7.12 1.31,7.11 1.33,7.11 1.38,7.18 1.38,7.18 1.33),"
            + "(7.17 1.32,7.12 1.32,7.12 1.36,7.13 1.36,7.13 1.33,7.16 1.33,7.16 1.34,7.17 1.35,7.17 1.32),"
            + "(7.16 1.36,7.16 1.35,7.15 1.34,7.14 1.34,7.14 1.35,7.15 1.36,7.15 1.37,7.16 1.37,7.16 1.36)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }
  @Test
  public void test732() throws ParseException {
    // Valid multipolygon with two outer rings, one containing an inner. One ring contains a node twice in succession in data.osm
    OSMEntity entity = testData.relations().get(732900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof MultiPolygon);

    assertEquals(0, ((Polygon)result.getGeometryN(0)).getNumInteriorRing());
    assertEquals(1, ((Polygon)result.getGeometryN(1)).getNumInteriorRing());
    assertEquals(2,result.getNumGeometries());
    // 16 because of double node
    assertEquals(16, result.getCoordinates().length, DELTA);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.21 1.36,7.22 1.37,7.23 1.36,7.22 1.35,7.21 1.36)),"
            + "((7.21 1.33,7.21 1.31,7.26 1.31,7.26 1.34,7.24 1.36,7.21 1.33),"
            + "(7.24 1.34,7.22 1.32,7.25 1.32,7.24 1.34)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }
}

