package org.heigit.bigspatialdata.oshdb.util.geometry.osmtestdata;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.xmlreader.OSMXmlReader;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.operation.valid.IsValidOp;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OSHDBGeometryBuilderTestOsmTestData7xxTest {
  private final OSMXmlReader testData = new OSMXmlReader();
  private final TagInterpreter tagInterpreter;
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");
  private final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestOsmTestData7xxTest() {
    testData.add("./src/test/resources/osm-testdata/all.osm");
    tagInterpreter = new OSMXmlReaderTagInterpreter(testData);
  }

  @Test
  public void test700() {
    // Polygon with one closed way.
    OSMEntity entity = testData.ways().get(700800L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
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
    assertEquals(5, result.getCoordinates().length);

  }

  @Test
  public void test701() throws ParseException {
    // Valid multipolygon relation with two ways (4 points) making up an outer ring.
    OSMEntity entity = testData.relations().get(701900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(5, result.getCoordinates().length);

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
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(9, result.getCoordinates().length);

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
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(9, result.getCoordinates().length);

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
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(13, result.getCoordinates().length);

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
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(14, result.getCoordinates().length);

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
    assertTrue(result.isValid());
    assertEquals(2,result.getNumGeometries());
    assertEquals(11, result.getCoordinates().length);

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
    assertTrue(result.isValid());
    assertEquals(2,result.getNumGeometries());
    assertEquals(10, result.getCoordinates().length);

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
    assertTrue(result.isValid());
    assertEquals(2,result.getNumGeometries());
    assertEquals(18, result.getCoordinates().length);

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
    assertTrue(result.isValid());
    assertEquals(3,result.getNumGeometries());
    assertEquals(15, result.getCoordinates().length);

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
  public void test710() {
    // Invalid multipolygon relation: Three ways make up two outer rings, but the outer rings overlap.
    OSMEntity entity1 = testData.relations().get(710900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
      assertEquals(2, result.getNumGeometries());
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test711() {
    // Invalid multipolygon relation: Two ways, both containing one of the segments."
    OSMEntity entity1 = testData.relations().get(711900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
      assertEquals(2, result.getNumGeometries());
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test714() {
    // Invalid multipolygon relation: Open ring.
    OSMEntity entity1 = testData.relations().get(714900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
      assertEquals(1, result.getNumGeometries());
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test715() {
    // Invalid multipolygon relation: Two open rings
    OSMEntity entity1 = testData.relations().get(715900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
      assertEquals(2, result.getNumGeometries());
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }



  @Test
  public void test720() throws ParseException {
    // "Multipolygon with one outer and one inner ring. They are both oriented clockwise and have the correct role.
    OSMEntity entity = testData.relations().get(720900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(1, ((Polygon) result).getNumInteriorRing());
    assertEquals(1,result.getNumGeometries());
    assertEquals(10, result.getCoordinates().length);
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
    assertTrue(result.isValid());
    assertEquals(1, ((Polygon) result).getNumInteriorRing());
    assertEquals(1,result.getNumGeometries());
    assertEquals(14, result.getCoordinates().length);
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
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(1,result.getNumGeometries());
    assertEquals(7, result.getCoordinates().length);
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
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(1,result.getNumGeometries());
    assertEquals(9, result.getCoordinates().length);
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
    assertTrue(result.isValid());
    assertFalse((result.getGeometryN(1)).intersects((result.getGeometryN(0))));
    assertEquals(0, ((Polygon)result.getGeometryN(0)).getNumInteriorRing());
    assertEquals(1, ((Polygon)result.getGeometryN(1)).getNumInteriorRing());
    assertEquals(2,result.getNumGeometries());
    assertEquals(15, result.getCoordinates().length);
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
    assertTrue(result.isValid());
    assertEquals(3, ((Polygon)result).getNumInteriorRing());
    assertEquals(1,result.getNumGeometries());
    assertEquals(21, result.getCoordinates().length);
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
    assertTrue(result.isValid());
    assertEquals(2, ((Polygon)result).getNumInteriorRing());
    assertEquals(1,result.getNumGeometries());
    assertEquals(25, result.getCoordinates().length);
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
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon)result.getGeometryN(0)).getNumInteriorRing());
    assertEquals(1, ((Polygon)result.getGeometryN(1)).getNumInteriorRing());
    assertEquals(2,result.getNumGeometries());
    // 16 because of double node
    assertEquals(16, result.getCoordinates().length);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.21 1.36,7.22 1.37,7.23 1.36,7.22 1.35,7.21 1.36)),"
            + "((7.21 1.33,7.21 1.31,7.26 1.31,7.26 1.34,7.24 1.36,7.21 1.33),"
            + "(7.24 1.34,7.22 1.32,7.25 1.32,7.24 1.34)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test733() throws ParseException {
    // Valid multipolygon with two outer rings
    // The same as test 706
  }

  @Test
  public void test734() throws ParseException {
    // Valid multipolygon with three outer rings
    // the same as test 709
  }

  @Test
  public void test740() {
    // Invalid multipolygon because the outer ring crosses itself.
    OSMEntity entity1 = testData.relations().get(740900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test741() {
    // Invalid multipolygon with a line only as 'outer ring'
    OSMEntity entity1 = testData.relations().get(741900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test742() {
    // Invalid multipolygon because of a 'spike'
    OSMEntity entity1 = testData.relations().get(742900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test743() {
    // Invalid multipolygon because of a 'spike'
    // the same like test 742
  }

  @Test
  public void test744() {
    // Invalid multipolygon with single outer ring not closed.
    OSMEntity entity1 = testData.relations().get(744900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
      assertTrue(result.getNumGeometries() == 2);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test745() {
    // Impossible multipolygon out of one way.
    OSMEntity entity1 = testData.relations().get(745900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
      assertTrue(result.getNumGeometries() == 1);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test746() {
    // IImpossible multipolygon out of two way.
    OSMEntity entity1 = testData.relations().get(746900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
      assertTrue(result.getNumGeometries() == 2);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test747() {
    // Invalid multipolygon because there are two nodes with same location. Created from relation
    OSMEntity entity1 = testData.relations().get(747900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test748() {
    // Invalid multipolygon because there are two nodes with same location. Created from way
    OSMEntity entity1 = testData.ways().get(748800L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof LineString);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test749() {
    // Valid multipolygon with two outer rings
    OSMEntity entity1 = testData.ways().get(749800L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof LineString);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test750() throws ParseException {
    // "Valid OSM multipolygon with touching inner rings."
    OSMEntity entity = testData.relations().get(750900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(1, ((Polygon)result).getNumInteriorRing());
    // In the result are 12 points, but it does not matter that we get 19, because the intersection is correct
    //assertEquals(19, result.getCoordinates().length, DELTA);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.01 1.51,7.01 1.57,7.06 1.57,7.06 1.51,7.01 1.51),"
            + "(7.02 1.52,7.02 1.55,7.04 1.55,7.05 1.55,7.05 1.52,7.03 1.52,7.02 1.52)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test751() throws ParseException {
    // "Valid OSM multipolygon with touching inner rings."
    OSMEntity entity = testData.relations().get(751900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(1, ((Polygon)result).getNumInteriorRing());
    // In the result are 11 points, but it does not matter that we get 16, because the intersection is correct
    //assertEquals(16, result.getCoordinates().length, DELTA);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.12 1.51,7.15 1.51,7.16 1.57,7.13 1.57,7.11 1.54,7.12 1.51),"
            + "(7.12 1.54,7.14 1.52,7.15 1.55,7.13 1.56,7.12 1.54)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test752() {
    // Touching inner without common nodes
    OSMEntity entity1 = testData.relations().get(752900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test753() {
    // Touching inner with one common node missing.
    OSMEntity entity1 = testData.relations().get(753900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test754() {
    // Inner ring touching outer, but not in node
    OSMEntity entity1 = testData.relations().get(754900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test755() throws ParseException {
    // Inner ring touching outer in node.
    OSMEntity entity = testData.relations().get(755900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(1, ((Polygon) result).getNumInteriorRing());
    assertEquals(1,result.getNumGeometries());
    assertEquals(11, result.getCoordinates().length);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.57 1.51,7.51 1.51,7.51 1.57,7.57 1.57,7.57 1.54,7.57 1.51),"
            + "(7.55 1.56,7.57 1.54,7.55 1.52,7.53 1.54,7.55 1.56)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test756() {
    // Inner ring touches outer ring in line, no common nodes
    OSMEntity entity1 = testData.relations().get(756900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test757() {
    // Inner ring touches outer ring in line using common nodes.
    OSMEntity entity1 = testData.relations().get(757900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test758() throws ParseException {
    // Inner ring touching outer in node.
    // the same as test 755
  }

  @Test
  public void test759() throws ParseException {
    // https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/132
    // Outer going back on itself in single node.
    OSMEntity entity = testData.relations().get(759900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    IsValidOp isValidOp = new IsValidOp(result);
    isValidOp.setSelfTouchingRingFormingHoleValid(true);
    assertTrue(isValidOp.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(1,result.getNumGeometries());
    // In the result are 11 points, but it does not matter that we get 10, because the intersection is correct
    //assertEquals(10, result.getCoordinates().length, DELTA);
    //assertEquals(10, entity., DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.91 1.53,7.91 1.57,7.94 1.59,7.96 1.55,7.94 1.51,7.91 1.53),"
            + "(7.96 1.55,7.94 1.53,7.92 1.55,7.94 1.57,7.96 1.55)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test760() throws ParseException {
    // https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/132
    // Faking inner ring with outer going back on itself (with relation).
    OSMEntity entity = testData.relations().get(760900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(1, ((Polygon) result).getNumInteriorRing());
    assertEquals(1, result.getNumGeometries());

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.01 1.67,7.07 1.67,7.07 1.61,7.01 1.61,7.01 1.67),"
            + "(7.05 1.65,7.03 1.65,7.03 1.63,7.05 1.63,7.05 1.65)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test761() throws ParseException {
    // https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/132
    // Faking inner ring with outer going back on itself (with relation).
    OSMEntity entity = testData.ways().get(761800L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(1, ((Polygon) result).getNumInteriorRing());
    assertEquals(1, result.getNumGeometries());

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.11 1.67,7.17 1.67,7.17 1.61,7.11 1.61,7.11 1.67),"
            + "(7.15 1.65,7.13 1.65,7.13 1.63,7.15 1.63,7.15 1.65)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test762() throws ParseException {
    // Touching outer rings.
    OSMEntity entity = testData.relations().get(762900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(1, ((Polygon)result.getGeometryN(0)).getNumInteriorRing());
    assertEquals(1, result.getNumGeometries());

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.21 1.67,7.27 1.67,7.27 1.61,7.21 1.61,7.21 1.67),"
            + "(7.25 1.65,7.23 1.65,7.23 1.63,7.25 1.63,7.25 1.65)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test763() throws ParseException {
    // Valid multipolygon with four outer rings touching in a single point
    OSMEntity entity = testData.relations().get(763900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon)result.getGeometryN(0)).getNumInteriorRing());
    assertEquals(0, ((Polygon)result.getGeometryN(1)).getNumInteriorRing());
    assertEquals(0, ((Polygon)result.getGeometryN(2)).getNumInteriorRing());
    assertEquals(0, ((Polygon)result.getGeometryN(3)).getNumInteriorRing());
    assertEquals(4,result.getNumGeometries());
    //assertEquals(28, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.35 1.65,7.33 1.64,7.32 1.63,7.32 1.62,7.33 1.62,7.34 1.63,7.35 1.65)),"
            + "((7.35 1.65,7.36 1.63,7.37 1.62,7.38 1.62,7.38 1.63,7.37 1.64,7.35 1.65)),"
            + "((7.35 1.65,7.33 1.66,7.32 1.67,7.32 1.68,7.33 1.68,7.34 1.67,7.35 1.65)),"
            + "((7.35 1.65,7.36 1.67,7.37 1.68,7.38 1.68,7.38 1.67,7.37 1.66,7.35 1.65)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test764() throws ParseException {
    // Valid multipolygon with one outer ring and four inner rings touching in a single point.
    OSMEntity entity = testData.relations().get(764900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(4, ((Polygon) result).getNumInteriorRing());
    assertEquals(1,result.getNumGeometries());
    //assertEquals(33, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.41 1.61,7.41 1.69,7.49 1.69,7.49 1.61,7.41 1.61),"
            + "(7.45 1.65,7.43 1.64,7.42 1.63,7.42 1.62,7.43 1.62,7.44 1.63,7.45 1.65),"
            + "(7.45 1.65,7.46 1.63,7.47 1.62,7.48 1.62,7.48 1.63,7.47 1.64,7.45 1.65),"
            + "(7.45 1.65,7.43 1.66,7.42 1.67,7.42 1.68,7.43 1.68,7.44 1.67,7.45 1.65),"
            + "(7.45 1.65,7.46 1.67,7.47 1.68,7.48 1.68,7.48 1.67,7.47 1.66,7.45 1.65)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test765() throws ParseException {
    // https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/134
    // Multipolygon with one outer ring that should be split into two components.
    OSMEntity entity = testData.relations().get(765900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon)result.getGeometryN(0)).getNumInteriorRing());
    assertEquals(0, ((Polygon)result.getGeometryN(1)).getNumInteriorRing());
    assertEquals(2,result.getNumGeometries());
    //assertEquals(11, result.getCoordinates().length, DELTA);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.57 1.66,7.55 1.68,7.53 1.66,7.55 1.64,7.57 1.66)),"
            + "((7.55 1.63,7.58 1.63,7.58 1.62,7.52 1.62,7.52 1.63,7.55 1.63)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test766() throws ParseException {
    // https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/134
    // Multipolygon with one outer and inner ring that should be split into two components.
    OSMEntity entity = testData.relations().get(766900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(2, ((Polygon)result).getNumInteriorRing());
    assertEquals(1, result.getNumGeometries());
    //assertEquals(16, result.getCoordinates().length, DELTA);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.61 1.61,7.61 1.69,7.69 1.69,7.69 1.61,7.61 1.61),"
            + "(7.67 1.66,7.65 1.68,7.63 1.66,7.65 1.64,7.67 1.66),"
            + "(7.65 1.63,7.68 1.63,7.68 1.62,7.62 1.62,7.62 1.63,7.65 1.63)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test767() throws ParseException {
    // Single way going back on itself.
    OSMEntity entity = testData.ways().get(767800L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon)result.getGeometryN(0)).getNumInteriorRing());
    assertEquals(0, ((Polygon)result.getGeometryN(1)).getNumInteriorRing());
    assertEquals(2,result.getNumGeometries());
   // assertEquals(11, result.getCoordinates().length, DELTA);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.77 1.66,7.75 1.68,7.73 1.66,7.75 1.64,7.77 1.66)),"
            + "((7.75 1.63,7.78 1.63,7.78 1.62,7.72 1.62,7.72 1.63,7.75 1.63)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test768() {
    // Multipolygon with two overlapping ways
    OSMEntity entity1 = testData.relations().get(768900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test770() throws ParseException {
    // Multipolygon with two outer rings touching in single node.
    // the same as 706
  }

  @Test
  public void test771() {
    // Multipolygon with two outer rings touching in single point, but no common node there
    OSMEntity entity1 = testData.relations().get(771900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
      assertTrue(result.getNumGeometries() == 2);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test772() throws ParseException {
    // Multipolygon with two inner rings touching in single node
    OSMEntity entity = testData.relations().get(772900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(2, ((Polygon)result).getNumInteriorRing());
    assertEquals(1,result.getNumGeometries());
    //assertEquals(16, result.getCoordinates().length, DELTA);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.21 1.71,7.21 1.79,7.29 1.79,7.29 1.71,7.21 1.71),"
            + "(7.26 1.72,7.22 1.72,7.22 1.74,7.24 1.74,7.26 1.74,7.26 1.72),"
            + "(7.24 1.74,7.26 1.76,7.24 1.78,7.22 1.76,7.24 1.74)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test773() {
    // Multipolygon with two inner rings touching in single point, but no common node there.
    OSMEntity entity1 = testData.relations().get(773900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test774() throws ParseException {
    // https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/136
    // Multipolygon with two outer rings touching in two nodes.
    OSMEntity entity = testData.relations().get(774900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(2,result.getNumGeometries());
    //assertEquals(14, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.42 1.73,7.42 1.75,7.44 1.75,7.44 1.73,7.42 1.73)),"
            + "((7.44 1.75,7.44 1.76,7.47 1.76,7.47 1.72,7.44 1.72,7.44 1.73,7.45 1.73,7.45 1.75,7.44 1.75)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test775() throws ParseException {
    // https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/136
    // Multipolygon with two outer rings touching in two nodes.
    OSMEntity entity = testData.relations().get(775900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(2,result.getNumGeometries());
    //assertEquals(14, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.52 1.73,7.52 1.75,7.54 1.75,7.54 1.73,7.52 1.73)),"
            + "((7.54 1.75,7.54 1.76,7.57 1.76,7.57 1.72,7.54 1.72,7.54 1.73,7.55 1.73,7.55 1.75,7.54 1.75)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test776() throws ParseException {
    // https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/136
    // Multipolygon with two outer rings touching in two nodes.
    OSMEntity entity = testData.relations().get(776900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(2,result.getNumGeometries());
    //assertEquals(14, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.62 1.73,7.62 1.75,7.64 1.75,7.64 1.73,7.62 1.73)),"
            + "((7.64 1.75,7.64 1.76,7.67 1.76,7.67 1.72,7.64 1.72,7.64 1.73,7.65 1.73,7.65 1.75,7.64 1.75)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test777() throws ParseException {
    // https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/137
    // Multipolygon with two outer rings and two inner rings touching in two nodes
    OSMEntity entity = testData.relations().get(777900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(2,result.getNumGeometries());
    assertEquals(2, ((Polygon)result.getGeometryN(0)).getNumInteriorRing());
    assertEquals(0, ((Polygon)result.getGeometryN(1)).getNumInteriorRing());
    //assertEquals(19, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.71 1.71,7.78 1.71,7.78 1.77,7.71 1.77,7.71 1.71),"
            + "(7.72 1.73,7.72 1.75,7.74 1.75,7.74 1.76,7.77 1.76,7.77 1.72,7.74 1.72,7.74 1.73,7.72 1.73)),"
            + "((7.74 1.73,7.75 1.73,7.75 1.75,7.74 1.75,7.74 1.73)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test778() throws ParseException {
    // https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/137
    // Multipolygon with two outer rings and two inner rings touching in two nodes
    OSMEntity entity = testData.relations().get(778900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(2,result.getNumGeometries());
    assertEquals(2, ((Polygon)result.getGeometryN(0)).getNumInteriorRing());
    assertEquals(0, ((Polygon)result.getGeometryN(1)).getNumInteriorRing());
    //assertEquals(19, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.81 1.71,7.88 1.71,7.88 1.77,7.81 1.77,7.81 1.71),"
            + "(7.82 1.73,7.82 1.75,7.84 1.75,7.84 1.76,7.87 1.76,7.87 1.72,7.84 1.72,7.84 1.73,7.82 1.73)),"
            + "((7.84 1.73,7.85 1.73,7.85 1.75,7.84 1.75,7.84 1.73)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test779() throws ParseException {
    // https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/137
    // Multipolygon with two outer rings and two inner rings touching in two nodes
    OSMEntity entity = testData.relations().get(779900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof MultiPolygon);

    assertTrue(result.isValid());
    assertEquals(2,result.getNumGeometries());
    assertEquals(2, ((Polygon)result.getGeometryN(0)).getNumInteriorRing());
    assertEquals(0, ((Polygon)result.getGeometryN(1)).getNumInteriorRing());
    //assertEquals(19, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.91 1.71,7.98 1.71,7.98 1.77,7.91 1.77,7.91 1.71),"
            + "(7.92 1.73,7.92 1.75,7.94 1.75,7.94 1.76,7.97 1.76,7.97 1.72,7.94 1.72,7.94 1.73,7.92 1.73)),"
            + "((7.94 1.73,7.95 1.73,7.95 1.75,7.94 1.75,7.94 1.73)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test780() {
    // Way with different nodes as start and endpoint, but same location of those nodes
    OSMEntity entity1 = testData.ways().get(780800L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof LineString);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test781() {
    // Multipolygon with one outer ring from single way that has different end-nodes, but they have same location
    OSMEntity entity1 = testData.relations().get(781900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test782() {
    // Multipolygon with correct outer ring, but inner ring made up out of two ways where locations match but not node ids
    OSMEntity entity1 = testData.relations().get(782900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test783() throws ParseException {
    // https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/131
    // Valid OSM multipolygon with multiple touching inner rings
    OSMEntity entity = testData.relations().get(783900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(1, ((Polygon)result).getNumInteriorRing());
    assertEquals(1,result.getNumGeometries());
    //assertEquals(11, result.getCoordinates().length, DELTA);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.32 1.81,7.35 1.81,7.36 1.87,7.33 1.87,7.31 1.84,7.32 1.81),"
            + "(7.32 1.84,7.34 1.82,7.35 1.85,7.33 1.86,7.32 1.84)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test784() throws ParseException {
    // https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/131
    // Valid OSM multipolygon with multiple touching inner rings
    OSMEntity entity = testData.relations().get(784900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(1, ((Polygon)result).getNumInteriorRing());
    assertEquals(1,result.getNumGeometries());
    //assertEquals(11, result.getCoordinates().length, DELTA);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.41 1.81,7.46 1.81,7.46 1.86,7.41 1.86,7.41 1.81),"
            + "(7.42 1.82,7.45 1.82,7.45 1.85,7.42 1.85,7.42 1.82)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test785() throws ParseException {
    // https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/131
    // Valid OSM multipolygon with two touching inner rings leaving an empty area.
    OSMEntity entity = testData.relations().get(785900L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(2, ((Polygon)result.getGeometryN(0)).getNumInteriorRing());
    assertEquals(0, ((Polygon)result.getGeometryN(1)).getNumInteriorRing());
    assertEquals(2,result.getNumGeometries());
    //assertEquals(11, result.getCoordinates().length, DELTA);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.51 1.81,7.56 1.81,7.56 1.86,7.51 1.86,7.51 1.81),"
            + "(7.52 1.82,7.55 1.82,7.55 1.85,7.52 1.85,7.52 1.82)),"
            + "((7.53 1.83,7.54 1.83,7.54 1.84,7.53 1.84,7.53 1.83)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test790() {
    // Multipolygon relation containing the same way twice.
    OSMEntity entity1 = testData.relations().get(790900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test791() {
    // Multipolygon relation containing the two ways using the same nodes in the same order
    OSMEntity entity1 = testData.relations().get(791900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test792() {
    // Multipolygon relation containing two ways using the same nodes in different order
    OSMEntity entity1 = testData.relations().get(792900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test793() {
    // Multipolygon relation containing the two ways using nearly the same nodes
    OSMEntity entity1 = testData.relations().get(793900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void test794() {
    // Multipolygon relation containing three ways using the same nodes in the same order.
    // the same like test 791
  }

  @Test
  public void test795() {
    // Multipolygon with one outer and one duplicated inner ring
    OSMEntity entity1 = testData.relations().get(795900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }
}

