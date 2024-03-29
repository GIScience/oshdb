package org.heigit.ohsome.oshdb.util.geometry.osmtestdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryTest;
import org.heigit.ohsome.oshdb.util.geometry.helpers.TimestampParser;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * Tests the {@link OSHDBGeometryBuilder} class: multipolygon geometry tests.
 *
 * @see <a href="https://github.com/osmcode/osm-testdata/tree/master/grid">osm-testdata</a>
 */
class OSHDBGeometryBuilderTestOsmTestData7xxTest extends OSHDBGeometryTest {
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");
  private static final double DELTA = 1E-8;

  OSHDBGeometryBuilderTestOsmTestData7xxTest() {
    super("./src/test/resources/osm-testdata/all.osm");
  }

  @Test
  void test700() {
    // Polygon with one closed way.
    Geometry result = buildGeometry(ways(700800L, 0), timestamp);
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
  void test701() throws ParseException {
    // Valid multipolygon relation with two ways (4 points) making up an outer ring.
    Geometry result = buildGeometry(relations(701900L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(5, result.getCoordinates().length);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.14 1.01,7.11 1.01,7.11 1.04,7.14 1.04,7.14 1.01)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test702() throws ParseException {
    // Valid multipolygon relation with two ways (8 points) making up an outer ring."
    Geometry result = buildGeometry(relations(702900L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(9, result.getCoordinates().length);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.24 1.01,7.21 1.01,7.21 1.02,7.23 1.03,7.23 1.04,7.21 1.04,7.21 1.05,"
            + "7.24 1.05,7.24 1.01)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test703() throws ParseException {
    // Valid multipolygon relation with two ways (8 points) making up an outer ring."
    Geometry result = buildGeometry(relations(703900L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(9, result.getCoordinates().length);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.34 1.01,7.31 1.01,7.31 1.02,7.33 1.03,7.33 1.04,7.32 1.04,7.32 1.05,"
            + "7.34 1.05,7.34 1.01)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test704() throws ParseException {
    // Valid multipolygon relation with three ways making up an outer ring in the form of a cross.
    Geometry result = buildGeometry(relations(704900L, 0), timestamp);
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
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test705() throws ParseException {
    // Valid multipolygon relation with three ways making up an outer ring. Contains concave and
    // convex parts.
    Geometry result = buildGeometry(relations(705900L, 0), timestamp);
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
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test706() throws ParseException {
    // Valid multipolygon relation with three ways making up two outer rings that touch in one
    // point.
    Geometry result = buildGeometry(relations(706900L, 0), timestamp);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(2, result.getNumGeometries());
    assertEquals(11, result.getCoordinates().length);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.61 1.04,7.62 1.06,7.65 1.05,7.64 1.03,7.63 1.02,7.61 1.04)),"
            + "((7.64 1.03,7.67 1.03,7.67 1.01,7.64 1.01,7.64 1.03)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test707() throws ParseException {
    // Valid multipolygon relation with three ways making up two separate outer rings.
    Geometry result = buildGeometry(relations(707900L, 0), timestamp);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(2, result.getNumGeometries());
    assertEquals(10, result.getCoordinates().length);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.71 1.04,7.72 1.06,7.75 1.05,7.73 1.02,7.71 1.04)),"
            + "((7.74 1.03,7.77 1.03,7.77 1.01,7.74 1.01,7.74 1.03)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test708() throws ParseException {
    // Valid multipolygon relation with three ways making up two separate outer rings.
    Geometry result = buildGeometry(relations(708900L, 0), timestamp);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(2, result.getNumGeometries());
    assertEquals(18, result.getCoordinates().length);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.81 1.03,7.82 1.06,7.85 1.06,7.86 1.03,7.85 1.03,7.84 1.05,"
            + "7.83 1.05,7.82 1.02,7.81 1.03)),((7.83 1.04,7.84 1.04,7.84 1.02,7.87 1.02,7.87 1.03,"
            + "7.88 1.03,7.88 1.01,7.83 1.01,7.83 1.04)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test709() throws ParseException {
    // Valid multipolygon relation with four ways making up three outer rings touching in three
    // points.
    Geometry result = buildGeometry(relations(709900L, 0), timestamp);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(3, result.getNumGeometries());
    assertEquals(15, result.getCoordinates().length);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.91 1.04,7.92 1.06,7.95 1.05,7.94 1.03,7.93 1.02,7.91 1.04)),"
            + "((7.94 1.03,7.97 1.03,7.97 1.01,7.94 1.01,7.94 1.03)),"
            + "((7.95 1.05,7.97 1.03,7.98 1.08,7.95 1.05)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test710() {
    // Invalid multipolygon relation: Three ways make up two outer rings, but the outer rings
    // overlap.
    Geometry result = buildGeometry(relations(710900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    assertEquals(2, result.getNumGeometries());
  }

  @Test
  void test711() {
    // Invalid multipolygon relation: Two ways, both containing one of the segments."
    Geometry result = buildGeometry(relations(711900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    assertEquals(2, result.getNumGeometries());
  }

  @Test
  void test714() {
    // Invalid multipolygon relation: Open ring.
    Geometry result = buildGeometry(relations(714900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    assertEquals(1, result.getNumGeometries());
  }

  @Test
  void test715() {
    // Invalid multipolygon relation: Two open rings
    Geometry result = buildGeometry(relations(715900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    assertEquals(2, result.getNumGeometries());
  }



  @Test
  void test720() throws ParseException {
    // "Multipolygon with one outer and one inner ring. They are both oriented clockwise and have
    // the correct role.
    Geometry result = buildGeometry(relations(720900L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(1, ((Polygon) result).getNumInteriorRing());
    assertEquals(1, result.getNumGeometries());
    assertEquals(10, result.getCoordinates().length);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.05 1.21,7.01 1.21,7.01 1.25,7.05 1.25,7.05 1.21),"
            + "(7.04 1.22,7.02 1.22,7.02 1.24,7.04 1.24,7.04 1.22)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  /* @Test
  public void test721() throws ParseException {
    // "Multipolygon with one outer and one inner ring. They are both oriented anti-clockwise and
    // have the correct role.
    // the same as test(720) apart from anti-clockwise
  } */

  /* @Test
  public void test722() throws ParseException {
    // Multipolygon with one outer and one inner ring. The outer ring is oriented clockwise, the
    // inner anti-clockwise. They have both the correct role.
    // the same as test(720) apart from anti-clockwise
  } */

  /* @Test
  public void test723() throws ParseException {
    // Multipolygon with one outer and one inner ring. The outer ring is oriented anti-clockwise,
    // the inner clockwise. They have both the correct role.
    // the same as test(722) apart from anti-clockwise
  } */

  @Test
  void test724() throws ParseException {
    // Multipolygon with one outer and one inner ring and a bit more complex geometry and nodes not
    // in order
    Geometry result = buildGeometry(relations(724900L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(1, ((Polygon) result).getNumInteriorRing());
    assertEquals(1, result.getNumGeometries());
    assertEquals(14, result.getCoordinates().length);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.44 1.22,7.47 1.21,7.41 1.21,7.42 1.22,7.41 1.24,7.43 1.26,7.46 1.26,"
            + "7.45 1.23,7.44 1.22),(7.43 1.22,7.42 1.24,7.44 1.25,7.45 1.24,7.43 1.22)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test725() throws ParseException {
    // Valid multipolygon with one concave outer ring and no inner ring.
    Geometry result = buildGeometry(relations(725900L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(1, result.getNumGeometries());
    assertEquals(7, result.getCoordinates().length);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.53 1.21,7.54 1.21,7.52 1.23,7.54 1.25,7.53 1.25,7.51 1.23,7.53 1.21)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  /* @Test
  public void test726() throws ParseException {
    // Valid multipolygon with one inner and one outer
    // the same as test(724) apart from anti-clockwise
  } */

  /* @Test
  public void test727() throws ParseException {
    // Valid multipolygon with one inner and one outer
    // the same as test(724) apart from anti-clockwise
  } */

  @Test
  void test728() throws ParseException {
    // Multipolygon with one simple outer ring and a node member
    Geometry result = buildGeometry(relations(728900L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(1, result.getNumGeometries());
    assertEquals(9, result.getCoordinates().length);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.85 1.23,7.86 1.22,7.87 1.22,7.87 1.24,7.86 1.25,7.83 1.25,7.82 1.26,"
            + "7.84 1.23,7.85 1.23)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test729() throws ParseException {
    // Valid multipolygon with second outer ring in inner ring.
    Geometry result = buildGeometry(relations(729900L, 0), timestamp);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertFalse((result.getGeometryN(1)).intersects((result.getGeometryN(0))));
    assertEquals(0, ((Polygon) result.getGeometryN(0)).getNumInteriorRing());
    assertEquals(1, ((Polygon) result.getGeometryN(1)).getNumInteriorRing());
    assertEquals(2, result.getNumGeometries());
    assertEquals(15, result.getCoordinates().length);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.91 1.21,7.91 1.29,7.99 1.29,7.99 1.21,7.91 1.21),"
            + "(7.97 1.27,7.97 1.23,7.93 1.23,7.93 1.27,7.97 1.27)),"
            + "((7.96 1.26,7.94 1.26,7.94 1.24,7.96 1.24,7.96 1.26)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test730() throws ParseException {
    // Valid multipolygon with one outer and three inner rings with correct roles
    Geometry result = buildGeometry(relations(730900L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(3, ((Polygon) result).getNumInteriorRing());
    assertEquals(1, result.getNumGeometries());
    assertEquals(21, result.getCoordinates().length);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.06 1.31,7.01 1.31,7.01 1.34,7.04 1.37,7.06 1.34,7.06 1.31),"
            + "(7.02 1.33,7.03 1.33,7.03 1.32,7.02 1.32,7.02 1.33),"
            + "(7.03 1.35,7.04 1.35,7.04 1.34,7.03 1.34,7.03 1.35),"
            + "(7.05 1.33,7.04 1.33,7.04 1.32,7.05 1.32,7.05 1.33)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test731() throws ParseException {
    // "Valid complex multipolygon with one outer and two inner rings made up of several ways. Roles
    // are tagged correctly
    Geometry result = buildGeometry(relations(731900L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(2, ((Polygon) result).getNumInteriorRing());
    assertEquals(1, result.getNumGeometries());
    assertEquals(25, result.getCoordinates().length);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.18 1.33,7.17 1.31,7.12 1.31,7.11 1.33,7.11 1.38,7.18 1.38,7.18 1.33),"
            + "(7.17 1.32,7.12 1.32,7.12 1.36,7.13 1.36,7.13 1.33,7.16 1.33,7.16 1.34,7.17 1.35,"
            + "7.17 1.32),(7.16 1.36,7.16 1.35,7.15 1.34,7.14 1.34,7.14 1.35,7.15 1.36,7.15 1.37,"
            + "7.16 1.37,7.16 1.36)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test732() throws ParseException {
    // Valid multipolygon with two outer rings, one containing an inner. One ring contains a node
    // twice in succession in data.osm
    Geometry result = buildGeometry(relations(732900L, 0), timestamp);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(2, result.getNumGeometries());
    assertEquals(1,
        ((Polygon) result.getGeometryN(0)).getNumInteriorRing()
        + ((Polygon) result.getGeometryN(1)).getNumInteriorRing()
    );
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.21 1.36,7.22 1.37,7.23 1.36,7.22 1.35,7.21 1.36)),"
            + "((7.21 1.33,7.21 1.31,7.26 1.31,7.26 1.34,7.24 1.36,7.21 1.33),"
            + "(7.24 1.34,7.22 1.32,7.25 1.32,7.24 1.34)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  /* @Test
  public void test733() throws ParseException {
    // Valid multipolygon with two outer rings
    // The same as test 706
  } */

  /* @Test
  public void test734() throws ParseException {
    // Valid multipolygon with three outer rings
    // the same as test 709
  } */

  @Test
  void test740() {
    // Invalid multipolygon because the outer ring crosses itself.
    Geometry result = buildGeometry(relations(740900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
  }

  @Test
  void test741() {
    // Invalid multipolygon with a line only as 'outer ring'
    Geometry result = buildGeometry(relations(741900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
  }

  @Test
  void test742() {
    // Invalid multipolygon because of a 'spike'
    Geometry result = buildGeometry(relations(742900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
  }

  /* @Test
  public void test743() {
    // Invalid multipolygon because of a 'spike'
    // the same like test 742
  } */

  @Test
  void test744() {
    // Invalid multipolygon with single outer ring not closed.
    Geometry result = buildGeometry(relations(744900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    assertEquals(2, result.getNumGeometries());
  }

  @Test
  void test745() {
    // Impossible multipolygon out of one way.
    Geometry result = buildGeometry(relations(745900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    assertEquals(1, result.getNumGeometries());
  }

  @Test
  void test746() {
    // Impossible multipolygon out of two ways.
    Geometry result = buildGeometry(relations(746900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
  }

  @Test
  void test747() {
    // Invalid multipolygon because there are two nodes with same location. Created from relation
    Geometry result = buildGeometry(relations(747900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
  }

  @Test
  void test748() {
    // Invalid multipolygon because there are two nodes with same location. Created from way
    Geometry result = buildGeometry(ways(748800L, 0), timestamp);
    assertTrue(result instanceof LineString);
  }

  @Test
  void test749() {
    // Valid multipolygon with two outer rings
    Geometry result = buildGeometry(ways(749800L, 0), timestamp);
    assertTrue(result instanceof LineString);
  }

  @Test
  void test750() throws ParseException {
    // "Valid OSM multipolygon with touching inner rings."
    Geometry result = buildGeometry(relations(750900L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(1, ((Polygon) result).getNumInteriorRing());
    // In the result are 12 points, but it does not matter that we get 19, because the intersection
    // is correct.
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.01 1.51,7.01 1.57,7.06 1.57,7.06 1.51,7.01 1.51),"
            + "(7.02 1.52,7.02 1.55,7.04 1.55,7.05 1.55,7.05 1.52,7.03 1.52,7.02 1.52)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test751() throws ParseException {
    // "Valid OSM multipolygon with touching inner rings."
    Geometry result = buildGeometry(relations(751900L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(1, ((Polygon) result).getNumInteriorRing());
    // In the result are 11 points, but it does not matter that we get 16, because the intersection
    // is correct.
    //assertEquals(16, result.getCoordinates().length);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.12 1.51,7.15 1.51,7.16 1.57,7.13 1.57,7.11 1.54,7.12 1.51),"
            + "(7.12 1.54,7.14 1.52,7.15 1.55,7.13 1.56,7.12 1.54)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test752() {
    // Touching inner without common nodes
    Geometry result = buildGeometry(relations(752900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
  }

  @Test
  void test753() {
    // Touching inner with one common node missing.
    Geometry result = buildGeometry(relations(753900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
  }

  @Test
  void test754() {
    // Inner ring touching outer, but not in node
    Geometry result = buildGeometry(relations(754900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
  }

  @Test
  void test755() throws ParseException {
    // Inner ring touching outer in node.
    Geometry result = buildGeometry(relations(755900L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(1, ((Polygon) result).getNumInteriorRing());
    assertEquals(1, result.getNumGeometries());
    assertEquals(11, result.getCoordinates().length);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.57 1.51,7.51 1.51,7.51 1.57,7.57 1.57,7.57 1.54,7.57 1.51),"
            + "(7.55 1.56,7.57 1.54,7.55 1.52,7.53 1.54,7.55 1.56)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test756() {
    // Inner ring touches outer ring in line, no common nodes
    Geometry result = buildGeometry(relations(756900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
  }

  @Test
  void test757() {
    // Inner ring touches outer ring in line using common nodes.
    Geometry result = buildGeometry(relations(757900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
  }

  /* @Test
  public void test758() throws ParseException {
    // Inner ring touching outer in node.
    // the same as test 755
  } */

  /* @Test
  public void test759() throws ParseException {
    // https://github.com/GIScience/oshdb/issues/128
    // Outer going back on itself in single node.
    // -- no assertion, because the test data itself is not valid --
  } */

  /* @Test
  public void test760() throws ParseException {
    // https://github.com/GIScience/oshdb/issues/128
    // Faking inner ring with outer going back on itself (with relation).
    // -- no assertion, because the test data itself is not valid --
  } */

  /* @Test
  public void test761() throws ParseException {
    // https://github.com/GIScience/oshdb/issues/128
    // Faking inner ring with outer going back on itself (with relation).
    // -- no assertion, because the test data itself is not valid --
  } */

  /* @Test
  public void test762() throws ParseException {
    // https://github.com/GIScience/oshdb/issues/127
    // Touching outer rings.
    // -- no assertion, because the test data itself is not valid --
  } */

  @Test
  void test763() throws ParseException {
    // Valid multipolygon with four outer rings touching in a single point
    Geometry result = buildGeometry(relations(763900L, 0), timestamp);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result.getGeometryN(0)).getNumInteriorRing());
    assertEquals(0, ((Polygon) result.getGeometryN(1)).getNumInteriorRing());
    assertEquals(0, ((Polygon) result.getGeometryN(2)).getNumInteriorRing());
    assertEquals(0, ((Polygon) result.getGeometryN(3)).getNumInteriorRing());
    assertEquals(4, result.getNumGeometries());
    //assertEquals(28, result.getCoordinates().length);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.35 1.65,7.33 1.64,7.32 1.63,7.32 1.62,7.33 1.62,7.34 1.63,7.35 1.65)),"
            + "((7.35 1.65,7.36 1.63,7.37 1.62,7.38 1.62,7.38 1.63,7.37 1.64,7.35 1.65)),"
            + "((7.35 1.65,7.33 1.66,7.32 1.67,7.32 1.68,7.33 1.68,7.34 1.67,7.35 1.65)),"
            + "((7.35 1.65,7.36 1.67,7.37 1.68,7.38 1.68,7.38 1.67,7.37 1.66,7.35 1.65)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test764() throws ParseException {
    // Valid multipolygon with one outer ring and four inner rings touching in a single point.
    Geometry result = buildGeometry(relations(764900L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(4, ((Polygon) result).getNumInteriorRing());
    assertEquals(1, result.getNumGeometries());
    //assertEquals(33, result.getCoordinates().length);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.41 1.61,7.41 1.69,7.49 1.69,7.49 1.61,7.41 1.61),"
            + "(7.45 1.65,7.43 1.64,7.42 1.63,7.42 1.62,7.43 1.62,7.44 1.63,7.45 1.65),"
            + "(7.45 1.65,7.46 1.63,7.47 1.62,7.48 1.62,7.48 1.63,7.47 1.64,7.45 1.65),"
            + "(7.45 1.65,7.43 1.66,7.42 1.67,7.42 1.68,7.43 1.68,7.44 1.67,7.45 1.65),"
            + "(7.45 1.65,7.46 1.67,7.47 1.68,7.48 1.68,7.48 1.67,7.47 1.66,7.45 1.65)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  /* @Test
  public void test765() throws ParseException {
    // https://github.com/GIScience/oshdb/issues/126
    // Multipolygon with one outer ring that should be split into two components.
    // -- no assertion, because the test data itself is not valid --
  } */

  /* @Test
  public void test766() throws ParseException {
    // https://github.com/GIScience/oshdb/issues/126
    // Multipolygon with one outer and inner ring that should be split into two components.
    // -- no assertion, because the test data itself is not valid --
  } */

  /* @Test
  public void test767() throws ParseException {
    // https://github.com/GIScience/oshdb/issues/125
    // Single way going back on itself.
    // -- no assertion, because the test data itself is not valid --
  } */

  @Test
  void test768() {
    // Multipolygon with two overlapping ways
    Geometry result = buildGeometry(relations(768900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
  }

  /* @Test
  public void test770() throws ParseException {
    // Multipolygon with two outer rings touching in single node.
    // the same as 706
  } */

  @Test
  void test771() {
    // Multipolygon with two outer rings touching in single point, but no common node there
    Geometry result = buildGeometry(relations(771900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    assertEquals(2, result.getNumGeometries());
  }

  @Test
  void test772() throws ParseException {
    // Multipolygon with two inner rings touching in single node
    Geometry result = buildGeometry(relations(772900L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(2, ((Polygon) result).getNumInteriorRing());
    assertEquals(1, result.getNumGeometries());
    //assertEquals(16, result.getCoordinates().length);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.21 1.71,7.21 1.79,7.29 1.79,7.29 1.71,7.21 1.71),"
            + "(7.26 1.72,7.22 1.72,7.22 1.74,7.24 1.74,7.26 1.74,7.26 1.72),"
            + "(7.24 1.74,7.26 1.76,7.24 1.78,7.22 1.76,7.24 1.74)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test773() {
    // Multipolygon with two inner rings touching in single point, but no common node there.
    Geometry result = buildGeometry(relations(773900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
  }

  @Test
  void test774() throws ParseException {
    // https://github.com/GIScience/oshdb/issues/124
    // Multipolygon with two outer rings touching in two nodes.
    Geometry result = buildGeometry(relations(774900L, 0), timestamp);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(2, result.getNumGeometries());
    //assertEquals(14, result.getCoordinates().length);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.42 1.73,7.42 1.75,7.44 1.75,7.44 1.73,7.42 1.73)),"
          + "((7.44 1.75,7.44 1.76,7.47 1.76,7.47 1.72,7.44 1.72,7.44 1.73,7.45 1.73,7.45 1.75,"
            + "7.44 1.75)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  /* @Test
  public void test775() throws ParseException {
    // https://github.com/GIScience/oshdb/issues/124
    // Multipolygon with two outer rings touching in two nodes.
    // -- no assertion, because the test data itself is not valid --
  } */

  /* @Test
  public void test776() throws ParseException {
    // https://github.com/GIScience/oshdb/issues/124
    // Multipolygon with two outer rings touching in two nodes.
    // -- no assertion, because the test data itself is not valid --
  } */

  @Test
  void test777() throws ParseException {
    /*
    777 is not a valid test case.

    According to https://wiki.openstreetmap.org/wiki/Relation:multipolygon: “Generally, the
    multipolygon relation can be used to build multipolygons in compliance with the OGC Simple
    Feature standard. Anything that is not a valid multipolygon according to this standard
    should also be considered an invalid multipolygon relation.”

    Here, the inners form a ring around a portion of the interior.
    */
    // https://github.com/GIScience/oshdb/issues/123
    // Multipolygon with two outer rings and two inner rings touching in two nodes
    Geometry result = buildGeometry(relations(777900L, 0), timestamp);
    assertTrue(result instanceof Polygonal);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.71 1.71,7.78 1.71,7.78 1.77,7.71 1.77,7.71 1.71),"
            + "(7.72 1.73,7.72 1.75,7.74 1.75,7.74 1.76,7.77 1.76,7.77 1.72,7.74 1.72,7.74 1.73,"
            + "7.72 1.73)),((7.74 1.73,7.75 1.73,7.75 1.75,7.74 1.75,7.74 1.73)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test778() {
    /*
    778 is not a valid test case.

    According to https://wiki.openstreetmap.org/wiki/Relation:multipolygon: “[inner ways make] up
    the optional inner ring(s) delimiting the excluded holes that must be fully inside the area
    delimited by outer ring(s).”

    For overlapping inner rings (such as it is the case here), it is not defined if the
    geometry building algorithm should use the even-odd rule, the non-zero-winding rule, a
    subtractive algorithm or something else.
    */
    // https://github.com/GIScience/oshdb/issues/123
    // Multipolygon with two outer rings and two inner rings touching in two nodes
    Geometry result = buildGeometry(relations(778900L, 0), timestamp);
    assertTrue(result instanceof Polygonal);
  }

  @Test
  void test779() throws ParseException {
    // https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/137
    // Multipolygon with two outer rings and two inner rings touching in two nodes
    Geometry result = buildGeometry(relations(779900L, 0), timestamp);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(2, result.getNumGeometries());
    assertEquals(1, ((Polygon) result.getGeometryN(0)).getNumInteriorRing()
        + ((Polygon) result.getGeometryN(1)).getNumInteriorRing());

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.91 1.71,7.98 1.71,7.98 1.77,7.91 1.77,7.91 1.71),"
            + "(7.92 1.73,7.92 1.75,7.94 1.75,7.94 1.76,7.97 1.76,7.97 1.72,7.94 1.72,7.94 1.73,"
            + "7.92 1.73)),((7.94 1.73,7.95 1.73,7.95 1.75,7.94 1.75,7.94 1.73)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test780() {
    // Way with different nodes as start and endpoint, but same location of those nodes
    Geometry result = buildGeometry(ways(780800L, 0), timestamp);
    assertTrue(result instanceof LineString);
  }

  @Test
  void test781() {
    // Multipolygon with one outer ring from single way that has different end-nodes, but they have
    // same location
    Geometry result = buildGeometry(relations(781900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
  }

  @Test
  void test782() {
    // Multipolygon with correct outer ring, but inner ring made up out of two ways where locations
    // match but not node ids
    Geometry result = buildGeometry(relations(782900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
  }

  @Test
  void test783() throws ParseException {
    // https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/131
    // Valid OSM multipolygon with multiple touching inner rings
    Geometry result = buildGeometry(relations(783900L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(1, ((Polygon) result).getNumInteriorRing());
    assertEquals(1, result.getNumGeometries());
    //assertEquals(11, result.getCoordinates().length);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.32 1.81,7.35 1.81,7.36 1.87,7.33 1.87,7.31 1.84,7.32 1.81),"
            + "(7.32 1.84,7.34 1.82,7.35 1.85,7.33 1.86,7.32 1.84)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test784() throws ParseException {
    // https://github.com/GIScience/oshdb/issues/129
    // Valid OSM multipolygon with multiple touching inner rings
    Geometry result = buildGeometry(relations(784900L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(1, ((Polygon) result).getNumInteriorRing());
    assertEquals(1, result.getNumGeometries());
    //assertEquals(11, result.getCoordinates().length);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.41 1.81,7.46 1.81,7.46 1.86,7.41 1.86,7.41 1.81),"
            + "(7.42 1.82,7.45 1.82,7.45 1.85,7.42 1.85,7.42 1.82)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test785() throws ParseException {
    /*
    785 is not a valid test case.

    According to https://wiki.openstreetmap.org/wiki/Relation:multipolygon: “An implementation of
    multipolygons should attempt to render [touching inner rings] as if the touching rings were
    indeed one ring.”

    Here, the inners form more than one ring: one inner ring and one additional outer ring.
    */
    // https://github.com/GIScience/oshdb/issues/129
    // Valid OSM multipolygon with two touching inner rings leaving an empty area.
    Geometry result = buildGeometry(relations(785900L, 0), timestamp);
    assertTrue(result instanceof Polygonal);
    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.51 1.81,7.56 1.81,7.56 1.86,7.51 1.86,7.51 1.81),"
            + "(7.52 1.82,7.55 1.82,7.55 1.85,7.52 1.85,7.52 1.82)),"
            + "((7.53 1.83,7.54 1.83,7.54 1.84,7.53 1.84,7.53 1.83)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(1.0, expectedPolygon.getArea() / intersection.getArea(), DELTA);
  }

  @Test
  void test790() {
    // Multipolygon relation containing the same way twice.
    Geometry result = buildGeometry(relations(790900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
  }

  @Test
  void test791() {
    // Multipolygon relation containing the two ways using the same nodes in the same order
    Geometry result = buildGeometry(relations(791900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
  }

  @Test
  void test792() {
    // Multipolygon relation containing two ways using the same nodes in different order
    Geometry result = buildGeometry(relations(792900L, 0), timestamp);
    assertNotNull(result);
  }

  @Test
  void test793() {
    // Multipolygon relation containing the two ways using nearly the same nodes
    Geometry result = buildGeometry(relations(793900L, 0), timestamp);
    assertNotNull(result);
  }

  /* @Test
  public void test794() {
    // Multipolygon relation containing three ways using the same nodes in the same order.
    // the same like test 791
  } */

  @Test
  void test795() {
    // Multipolygon with one outer and one duplicated inner ring
    Geometry result = buildGeometry(relations(795900L, 0), timestamp);
    assertNotNull(result);
  }
}
