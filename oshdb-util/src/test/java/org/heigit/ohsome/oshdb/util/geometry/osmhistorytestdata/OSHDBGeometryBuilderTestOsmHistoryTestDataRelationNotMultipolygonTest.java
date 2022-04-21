package org.heigit.ohsome.oshdb.util.geometry.osmhistorytestdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryTest;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygonal;

/**
 * Tests the {@link OSHDBGeometryBuilder} class on OSM relations except multipolygon relations.
 */
class OSHDBGeometryBuilderTestOsmHistoryTestDataRelationNotMultipolygonTest
    extends OSHDBGeometryTest {

  public OSHDBGeometryBuilderTestOsmHistoryTestDataRelationNotMultipolygonTest() {
    super("./src/test/resources/different-timestamps/type-not-multipolygon.osm");
  }

  @Test
  void testGeometryChange() {
    // relation getting more ways, one disappears, last version not valid
    Geometry result = buildGeometry(relations(500L, 0));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());

    // second version
    result = buildGeometry(relations(500L, 1));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());

    // third version
    result = buildGeometry(relations(500L, 2));
    assertTrue(result instanceof GeometryCollection || result instanceof Polygonal);
    assertEquals(3, result.getNumGeometries());
  }

  @Test
  void testVisibleChange() {
    // relation  visible tag changed
    Geometry result = buildGeometry(relations(501L, 0));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(2, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
    assertTrue(result.getGeometryN(1) instanceof LineString);

    // second version
    result = buildGeometry(relations(501L, 1));
    assertTrue(result.isEmpty());

    // third version
    result = buildGeometry(relations(501L, 2));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(2, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
    assertTrue(result.getGeometryN(1) instanceof LineString);
  }

  @Test
  void testWaysNotExistent() {
    // relation with three ways, all not existing
    Geometry result = buildGeometry(relations(502L, 0));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertTrue(result.isEmpty());
  }

  @Test
  void testTagChange() {
    // relation tags changing
    Geometry result = buildGeometry(relations(503L, 0));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(1, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);

    // second version
    result = buildGeometry(relations(503L, 1));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(1, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);

    // third version
    result = buildGeometry(relations(503L, 2));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(1, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
  }

  @Test
  void testGeometryChangeOfNodeRefsInWays() {
    // relation, way 109 -inner- and 110 -outer- ways changed node refs
    Geometry result = buildGeometry(relations(504L, 0));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(2, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
    assertTrue(result.getGeometryN(1) instanceof LineString);

    // second version
    result = buildGeometry(relations(504L, 1));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(2, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
    assertTrue(result.getGeometryN(1) instanceof LineString);

    // version in between
    result = buildGeometry(relations(504L, 0), "2012-02-01T00:00:00Z");
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(2, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
    assertTrue(result.getGeometryN(1) instanceof LineString);
  }

  @Test
  void testGeometryChangeOfNodeCoordinatesInWay() {
    // relation, way 112  changed node coordinates
    Geometry result = buildGeometry(relations(505L, 0));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(1, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);

    // version after
    result = buildGeometry(relations(505L, 0), "2012-02-01T00:00:00Z");
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(1, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
  }

  @Test
  void testGeometryChangeOfNodeCoordinatesInRelationAndWay() {
    // relation, with node members, nodes changed coordinates
    Geometry result = buildGeometry(relations(506L, 0));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(3, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof Point);
    assertTrue(result.getGeometryN(1) instanceof Point);
    assertTrue(result.getGeometryN(2) instanceof LineString);

    // version after
    result = buildGeometry(relations(506L, 0), "2012-02-01T00:00:00Z");
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(3, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof Point);
    assertTrue(result.getGeometryN(1) instanceof Point);
    assertTrue(result.getGeometryN(2) instanceof LineString);
  }

  @Test
  void testGeometryCollection() {
    // relation, not valid, should be a not empty geometryCollection
    Geometry result = buildGeometry(relations(507L, 0));
    assertTrue(result instanceof GeometryCollection);
    assertEquals(6, result.getNumGeometries());
    assertFalse(result instanceof MultiPolygon);
  }

  @Test
  void testNodesOfWaysNotExistent() {
    // relation with two ways, all nodes not existing
    Geometry result = buildGeometry(relations(508L, 0));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
  }

  @Test
  void testVisibleChangeOfNodeInWay() {
    // relation, way member: node 52 changes visible tag
    // timestamp where node 52 visible is false
    Geometry result = buildGeometry(relations(509L, 0));
    // version after
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(1, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);

    // timestamp where node 52 visible is true
    result = buildGeometry(relations(509L, 0), "2014-02-01T00:00:00Z");
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(1, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
  }

  @Test
  void testTagChangeOfNodeInWay() {
    // relation, way member: node 53 changes tags, 51 changes coordinates
    Geometry result = buildGeometry(relations(510L, 0));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(1, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);

    // version after
    result = buildGeometry(relations(510L, 0), "2014-02-01T00:00:00Z");
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(1, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
  }

  @Test
  void testVisibleChangeOfWay() {
    // relation, way member: way 119 changes visible tag
    Geometry result = buildGeometry(relations(511L, 0));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(1, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);

    // version after, visible false
    result = buildGeometry(relations(511L, 0), "2017-02-01T00:00:00Z");
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isEmpty());
  }

  @Test
  void testVisibleChangeOfOneWayOfOuterRing() {
    // relation, 2 way members making outer ring: way 120 changes visible tag later, 121 not
    Geometry result = buildGeometry(relations(512L, 0));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(2, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
    assertTrue(result.getGeometryN(1) instanceof LineString);

    // version after: way 120 does not exit any more
    result = buildGeometry(relations(512L, 0), "2018-02-01T00:00:00Z");
    assertTrue(result instanceof GeometryCollection);
    assertEquals(2, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString
        || result.getGeometryN(1) instanceof LineString);
  }

  @Test
  void testTagChangeOfWay() {
    // relation, way member: way 122 changes tags
    Geometry result = buildGeometry(relations(513L, 0));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(1, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);

    // way first version
    result = buildGeometry(relations(513L, 0), "2009-02-01T00:00:00Z");
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(1, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);

    // way second version
    result = buildGeometry(relations(513L, 0), "2012-02-01T00:00:00Z");
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(1, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
  }

  @Test
  void testOneOfTwoPolygonDisappears() {
    // relation, at the beginning two polygons, one disappears later
    Geometry result = buildGeometry(relations(514L, 0));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(2, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
    assertTrue(result.getGeometryN(1) instanceof LineString);

    // second version
    result = buildGeometry(relations(514L, 1));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(1, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
  }

  @Test
  void testWaySplitUpInTwo() {
    // relation, at the beginning one way, split up later into 2 ways
    Geometry result = buildGeometry(relations(515L, 0));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(1, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);

    // second version
    result = buildGeometry(relations(515L, 1));
    assertTrue(result instanceof GeometryCollection);
    assertTrue(result.isValid());
    assertEquals(2, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
    assertTrue(result.getGeometryN(1) instanceof LineString);
  }

  @Test
  void testRestrictionRoles() {
    // relation, restriction, role changes
    Geometry result = buildGeometry(relations(518L, 0));
    assertTrue(result instanceof GeometryCollection);
    assertEquals(3, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
    assertTrue(result.getGeometryN(1) instanceof Point);
    assertTrue(result.getGeometryN(2) instanceof LineString);
  }

  @Test
  void testRolesArePartAndOutline() {
    // relation as building with role=part and outline
    Geometry result = buildGeometry(relations(519L, 0));
    assertTrue(result instanceof GeometryCollection);
    assertEquals(2, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
    assertTrue(result.getGeometryN(1) instanceof LineString);

    // second version
    result = buildGeometry(relations(519L, 1));
    assertTrue(result instanceof GeometryCollection);
    assertEquals(3, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
    assertTrue(result.getGeometryN(1) instanceof LineString);
    assertTrue(result.getGeometryN(2) instanceof LineString);
  }
}