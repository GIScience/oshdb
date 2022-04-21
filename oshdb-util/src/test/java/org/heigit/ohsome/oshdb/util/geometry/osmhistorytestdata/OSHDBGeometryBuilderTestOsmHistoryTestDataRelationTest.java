package org.heigit.ohsome.oshdb.util.geometry.osmhistorytestdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryTest;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * Tests the {@link OSHDBGeometryBuilder} class on OSM relations.
 */
class OSHDBGeometryBuilderTestOsmHistoryTestDataRelationTest extends OSHDBGeometryTest {
  private static final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestOsmHistoryTestDataRelationTest() {
    super("./src/test/resources/different-timestamps/polygon.osm");
  }

  @Test
  void testGeometryChange() throws ParseException {
    // relation getting more ways, one disappears, last version not valid
    Geometry result = buildGeometry(relations(500L, 0));
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(9, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.31 1.01,7.34 1.01,7.34 1.05, 7.31 1.01)),"
            + "((7.34 1.05, 7.32 1.05, 7.32 1.04, 7.33 1.04, 7.34 1.05)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);

    // second version
    result = buildGeometry(relations(500L, 1));
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(14, result.getCoordinates().length);
    expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.31 1.01,7.34 1.01,7.34 1.05, 7.31 1.01)),"
            + "((7.34 1.05, 7.32 1.05, 7.32 1.04, 7.33 1.04, 7.34 1.05)),"
            + "(( 7.32 1.05,7.32 1.07,7.31 1.07,7.31 1.05,7.32 1.05)))"
    );
    intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);

    // third version
    result = buildGeometry(relations(500L, 2));
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    assertEquals(3, result.getNumGeometries());
  }

  @Test
  void testVisibleChange() throws ParseException {
    // relation  visible tag changed
    Geometry result = buildGeometry(relations(501L, 0));
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(10, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.35 1.01, 7.34 1.01,7.34 1.02,7.35 1.02, 7.35 1.01)),"
            + "((7.33 1.04,7.33 1.03, 7.31 1.02, 7.31 1.04, 7.33 1.04)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);

    // second version
    result = buildGeometry(relations(501L, 1));
    assertTrue(result.isEmpty());

    // third version
    result = buildGeometry(relations(501L, 2));
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(10, result.getCoordinates().length);
    expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.35 1.01, 7.34 1.01,7.34 1.02,7.35 1.02, 7.35 1.01)),"
               + "((7.33 1.04,7.33 1.03, 7.31 1.02, 7.31 1.04, 7.33 1.04)))"
    );
    intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  void testWaysNotExistent() {
    // relation with two ways, both missing
    Geometry result = buildGeometry(relations(502L, 0));
    assertNotNull(result);
  }

  @Test
  void testTagChange() throws ParseException {
    // relation tags changing
    Geometry result = buildGeometry(relations(503L, 0));
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(5, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON((( 7.33 1.05,7.33 1.06,7.32 1.06,7.32 1.05,7.33 1.05)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);

    // second version
    result = buildGeometry(relations(503L, 1));
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(5, result.getCoordinates().length);
    expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON((( 7.33 1.05,7.33 1.06,7.32 1.06,7.32 1.05,7.33 1.05)))"
    );
    intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);

    // third version
    result = buildGeometry(relations(503L, 2));
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(5, result.getCoordinates().length);
    expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON((( 7.33 1.05,7.33 1.06,7.32 1.06,7.32 1.05,7.33 1.05)))"
    );
    intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  void testGeometryChangeOfNodeRefsInWays() throws ParseException {
    // relation, way 109 -inner- and 110 -outer- ways changed node refs
    Geometry result = buildGeometry(relations(504L, 0));
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(10, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON((( 7.24 1.04, 7.24 1.07, 7.30 1.07, 7.30 1.04, 7.24 1.04),"
            + "(7.26 1.055, 7.265 1.06, 7.28 1.06,7.265 1.065, 7.26 1.055)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);

    // second version
    result = buildGeometry(relations(504L, 1));
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(10, result.getCoordinates().length);
    expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON((( 7.24 1.04, 7.24 1.07, 7.30 1.07, 7.30 1.04, 7.24 1.04),"
            + "( 7.26 1.05,7.265 1.06, 7.28 1.06, 7.265 1.05,7.26 1.05)))"
    );
    intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);

    // version in between
    result = buildGeometry(relations(504L, 0), "2012-02-01T00:00:00Z");
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(10, result.getCoordinates().length);
    expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.24 1.04, 7.24 1.07, 7.31 1.07, 7.31 1.04 , 7.24 1.04),"
            + "(7.26 1.055, 7.265 1.06, 7.28 1.06,7.265 1.065, 7.26 1.055)))"
    );
    intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  void testGeometryChangeOfNodeCoordinatesInWay() throws ParseException {
    // relation, way 112  changed node coordinates
    Geometry result = buildGeometry(relations(505L, 0));
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(5, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.24 1.048, 7.245 1.072, 7.305 1.078, 7.303 1.042 , 7.24 1.048)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);

    // version after
    result = buildGeometry(relations(505L, 0), "2012-02-01T00:00:00Z");
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(5, result.getCoordinates().length);
    expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.24 1.042, 7.242 1.07, 7.305 1.07, 7.295 1.039 , 7.24 1.042)))"
    );
    intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  void testGeometryChangeOfNodeCoordinatesInRelationAndWay() throws ParseException {
    // relation, with node members, nodes changed coordinates
    Geometry result = buildGeometry(relations(506L, 0));
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(4, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.24 1.048, 7.245 1.072,  7.303 1.042 , 7.24 1.048)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);

    // version after
    result = buildGeometry(relations(506L, 0), "2012-02-01T00:00:00Z");
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(4, result.getCoordinates().length);
    expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.24 1.042, 7.242 1.07, 7.295 1.039 , 7.24 1.042)))"
    );
    intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  void testGeometryCollection() {
    // relation, not valid, should be a not empty geometryCollection
    // https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/143
    Geometry result = buildGeometry(relations(507L, 0));
    assertTrue(result instanceof GeometryCollection);
    assertEquals(6, result.getNumGeometries());
    assertFalse(result instanceof MultiPolygon);
  }

  @Test
  void testNodesOfWaysNotExistent() {
    // relation with two ways, all nodes not existing
    Geometry result = buildGeometry(relations(508L, 0));
    assertNotNull(result);
  }

  @Test
  void testVisibleChangeOfNodeInWay() throws ParseException {
    // relation, way member: node 52 changes visible tag
    Geometry result = buildGeometry(relations(509L, 0));
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(4, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.303 1.042, 7.32 1.07, 7.32 1.04,7.303 1.042)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);

    // version after
    result = buildGeometry(relations(509L, 0), "2014-02-01T00:00:00Z");
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(5, result.getCoordinates().length);
    expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.303 1.042, 7.31 1.06, 7.32 1.07, 7.32 1.04, 7.303 1.042)))"
    );
    intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  void testTagChangeOfNodeInWay() throws ParseException {
    // relation, way member: node 53 changes tags, 51 changes coordinates
    Geometry result = buildGeometry(relations(510L, 0));
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(4, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.303 1.042,1.43 1.24,7.32 1.04,7.303 1.042)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);

    // version after
    result = buildGeometry(relations(510L, 0), "2014-02-01T00:00:00Z");
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(4, result.getCoordinates().length);
    expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.295 1.039, 1.43 1.24, 7.32 1.04, 7.295 1.039)))"
    );
    intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  void testVisibleChangeOfWay() throws ParseException {
    // relation, way member: way 119 changes visible tag
    Geometry result = buildGeometry(relations(511L, 0));
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(4, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.29 1.01, 7.29 1.05, 7.30 1.01, 7.29 1.01)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);

    // version after, visible false
    result = buildGeometry(relations(511L, 0), "2017-02-01T00:00:00Z");
    assertTrue(result.isEmpty());
  }

  @Test
  void testVisibleChangeOfOneWayOfOuterRing() throws ParseException {
    // relation, 2 way members making outer ring: way 120 changes visible tag later, 121 not
    Geometry result = buildGeometry(relations(512L, 0));
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(5, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON((( 7.5 1.04, 7.5 1.6, 7.4 1.6, 7.4 1.04,7.5 1.04)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);

    // version after: way 120 does not exit any more
    result = buildGeometry(relations(512L, 0), "2018-02-01T00:00:00Z");
    assertTrue(result instanceof GeometryCollection);
    assertEquals(2, result.getNumGeometries());
  }

  @Test
  void testTagChangeOfWay() throws ParseException {
    // relation, way member: way 122 changes tags
    Geometry result = buildGeometry(relations(513L, 0));
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(5, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.34 1.01, 7.34 1.05, 7.32 1.05, 7.32 1.04,7.34 1.01)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);

    // way first version
    result = buildGeometry(relations(513L, 0), "2009-02-01T00:00:00Z");
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(5, result.getCoordinates().length);
    expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.34 1.01, 7.34 1.05, 7.32 1.05, 7.32 1.04,7.34 1.01)))"
    );
    intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);

    // way second version
    result = buildGeometry(relations(513L, 0), "2012-02-01T00:00:00Z");
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(5, result.getCoordinates().length);
    expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.34 1.01, 7.34 1.05, 7.32 1.05, 7.32 1.04,7.34 1.01)))"
    );
    intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  void testOneOfTwoPolygonDisappears() throws ParseException {
    // relation getting more ways, one disappears, last version not valid
    Geometry result = buildGeometry(relations(514L, 0));
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(9, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.31 1.01,7.34 1.01,7.34 1.05, 7.31 1.01)),"
            + "((7.34 1.05, 7.32 1.05, 7.32 1.04, 7.33 1.04, 7.34 1.05)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);

    // second version
    result = buildGeometry(relations(514L, 1));
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(5, result.getCoordinates().length);
    expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.34 1.05, 7.32 1.05, 7.32 1.04, 7.33 1.04, 7.34 1.05)))"
    );
    intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  void testWaySplitUpInTwo() throws ParseException {
    // relation, at the beginning one way, split up later into 2 ways
    Geometry result = buildGeometry(relations(515L, 0));
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(5, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON((( 7.0 1.04, 7.0 1.6, 7.2 1.6, 7.2 1.04,7.0 1.04)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);

    // second version
    result = buildGeometry(relations(515L, 1));
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(5, result.getCoordinates().length);
    expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON((( 7.0 1.04, 7.0 1.6, 7.2 1.6, 7.2 1.04,7.0 1.04)))"
    );
    intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  void testNullRefEntities() {
    // broken rel references (=invalid OSM data) can occur after "partial" data redactions
    OSMRelation rel = relations(524L, 0);
    Geometry result = buildGeometry(rel);
    // no exception should have been thrown at this point
    assertTrue(result.getNumGeometries() < rel.getMembers().length);
  }
}
