package org.heigit.ohsome.oshdb.util.geometry.incomplete;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
 * Tests the {@link OSHDBGeometryBuilder} class on incomplete polygons.
 */
class OSHDBGeometryBuilderTestPolygonIncompleteDataTest extends OSHDBGeometryTest {
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");
  private static final double DELTA = 1E-6;

  OSHDBGeometryBuilderTestPolygonIncompleteDataTest() {
    super("./src/test/resources/incomplete-osm/polygon.osm");
  }

  @Test
  void testSomeNodesOfWayNotExistent() throws ParseException {
    // Valid multipolygon relation with two ways making up an outer ring, in second ring 2 node
    // references to not existing nodes
    //TODO https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/138
    Geometry result = buildGeometry(relations(500L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(7, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.34 1.01,7.31 1.01,7.33 1.04,7.32 1.04,7.32 1.05,7.34 1.05,7.34 1.01)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  void testWayNotExistent() throws ParseException {
    // Valid multipolygon relation with two way references, one way does not exist
    //TODO https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/138
    Geometry result = buildGeometry(relations(501L, 0), timestamp);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(6, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.31 1.04, 7.33 1.05, 7.33 1.04, 7.32 1.04, 7.31 1.01,7.31 1.01,"
            + "7.31 1.04)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  void testAllNodesOfWayNotExistent() {
    // relation with one way with two nodes, both missing
    Geometry result = buildGeometry(relations(502L, 0), timestamp);
    assertNotNull(result);
  }
}
