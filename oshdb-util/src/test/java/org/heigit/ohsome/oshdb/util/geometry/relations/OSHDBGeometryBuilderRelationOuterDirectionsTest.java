package org.heigit.ohsome.oshdb.util.geometry.relations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryTest;
import org.heigit.ohsome.oshdb.util.geometry.helpers.TimestampParser;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * Tests the {@link OSHDBGeometryBuilder} class for the special case of multipolygons with
 * split rings.
 */
class OSHDBGeometryBuilderRelationOuterDirectionsTest extends OSHDBGeometryTest {
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");
  private static final double DELTA = 1E-6;

  OSHDBGeometryBuilderRelationOuterDirectionsTest() {
    super("./src/test/resources/relations/outer-directions.osm");
  }


  @Test
  void testFromPointTwoWaysGoingToDiffDirections() throws ParseException {
    // start of partial ring matches start of current line
    // from one point in outer ring two ways are going to different directions
    Geometry result = buildGeometry(relations(1L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.16 1.36,7.16 1.35,7.15 1.34,7.14 1.34,7.14 1.35,7.14 "
        + "1.36,7.15 1.36,7.15 1.37,7.16 1.37,7.16 1.36)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  void testToPointTwoWaysPointingFromDiffDirections() throws ParseException {
    // end of partial ring matches end of current line
    // to one point in outer ring two ways are pointing from different directions
    Geometry result = buildGeometry(relations(2L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.16 1.36,7.16 1.35,7.15 1.34,7.14 1.34,7.14 1.35,7.14 "
        + "1.36,7.15 1.36,7.15 1.37,7.16 1.37,7.16 1.36)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  void testStartMatchesEnd() throws ParseException {
    // start of partial ring matches end of current line
    //
    Geometry result = buildGeometry(relations(3L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.16 1.36,7.16 1.35,7.15 1.34,7.14 1.34,7.14 1.35,7.14 "
        + "1.36,7.15 1.36,7.15 1.37,7.16 1.37,7.16 1.36)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  void testEndMatchesStart() throws ParseException {
    // end of partial ring matches to start of current line
    //
    Geometry result = buildGeometry(relations(4L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.16 1.36,7.16 1.35,7.15 1.34,7.14 1.34,7.14 1.35,7.14 "
        + "1.36,7.15 1.36,7.15 1.37,7.16 1.37,7.16 1.36)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }
}