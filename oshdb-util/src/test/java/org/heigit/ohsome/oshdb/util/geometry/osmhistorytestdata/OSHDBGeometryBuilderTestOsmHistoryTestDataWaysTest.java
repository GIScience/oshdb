package org.heigit.ohsome.oshdb.util.geometry.osmhistorytestdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryTest;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;

/**
 * Tests the {@link OSHDBGeometryBuilder} class on OSM ways.
 */
class OSHDBGeometryBuilderTestOsmHistoryTestDataWaysTest extends OSHDBGeometryTest {

  private static final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestOsmHistoryTestDataWaysTest() {
    super("./src/test/resources/different-timestamps/way.osm");
  }

  private static void checkLineString(double[][] expectedCoordinates, Geometry result,
      int numCoordinates) {
    assertTrue(result instanceof LineString);
    assertEquals(numCoordinates, result.getNumPoints());
    for (int i = 0; i < numCoordinates; i++) {
      assertEquals(expectedCoordinates[i][0], (((LineString) result).getCoordinateN(i)).x, DELTA);
      assertEquals(expectedCoordinates[i][1], (((LineString) result).getCoordinateN(i)).y, DELTA);
    }
  }

  @Test
  void testGeometryChange() {
    // Way getting more nodes, one disappears
    // first appearance
    Geometry result = buildGeometry(ways(100L, 0));
    double[][] expectedCoordinates = {{1.42, 1.22}, {1.42, 1.23}, {1.42, 1.24}, {1.42, 1.25}};
    checkLineString(expectedCoordinates, result, 4);

    // second appearance
    result = buildGeometry(ways(100L, 1));
    expectedCoordinates = new double[][]{{1.42, 1.22}, {1.42, 1.23}, {1.42, 1.24}, {1.42, 1.25},
        {1.42, 1.26}, {1.42, 1.27}, {1.42, 1.28}, {1.43, 1.29}};
    checkLineString(expectedCoordinates, result, 8);

    // last appearance
    result = buildGeometry(ways(100L, 2));
    expectedCoordinates = new double[][]{{1.42, 1.22}, {1.42, 1.23}, {1.42, 1.24}, {1.42, 1.25},
        {1.42, 1.26}, {1.42, 1.28}, {1.43, 1.29}, {1.43, 1.30}, {1.43, 1.31}};
    checkLineString(expectedCoordinates, result, 9);

    // timestamp after last one
    result = buildGeometry(ways(100L, 2), "2012-01-01T00:00:00Z");
    expectedCoordinates = new double[][]{{1.42, 1.22}, {1.42, 1.23}, {1.42, 1.24}, {1.42, 1.25},
        {1.42, 1.26}, {1.42, 1.28}, {1.43, 1.29}, {1.43, 1.30}, {1.43, 1.31}};
    checkLineString(expectedCoordinates, result, 9);
  }

  @Test
  void testGeometryChangeOfNodeInWay() {
    // Way with two then three nodes, changing lat lon
    // first appearance
    Geometry result = buildGeometry(ways(101L, 0));
    double[][] expectedCoordinates = {{1.42, 1.22}, {1.44, 1.22}};
    checkLineString(expectedCoordinates, result, 2);

    // last appearance
    result = buildGeometry(ways(101L, 1));
    expectedCoordinates = new double[][]{{1.425, 1.23}, {1.44, 1.23}, {1.43, 1.30}};
    checkLineString(expectedCoordinates, result, 3);

    // timestamp in between
    result = buildGeometry(ways(101L, 0), "2009-02-01T00:00:00Z");
    expectedCoordinates = new double[][]{{1.42, 1.225}, {1.445, 1.225}};
    checkLineString(expectedCoordinates, result, 2);
  }

  @Test
  void testVisibleChange() {
    // Way visible changed
    // first appearance
    Geometry result = buildGeometry(ways(102L, 0));
    assertTrue(result instanceof LineString);
    assertEquals(3, result.getNumPoints());

    // last appearance
    result = buildGeometry(ways(102L, 1));
    assertTrue(result.isEmpty());
  }

  @Test
  void testTagChange() {
    // Way tags changed
    // first appearance
    Geometry result = buildGeometry(ways(103L, 0));
    assertTrue(result instanceof LineString);
    assertEquals(3, result.getNumPoints());

    // second appearance
    result = buildGeometry(ways(103L, 1));
    assertTrue(result instanceof LineString);
    assertEquals(5, result.getNumPoints());

    // last appearance
    result = buildGeometry(ways(103L, 2));
    assertTrue(result instanceof LineString);
    assertEquals(5, result.getNumPoints());
  }

  @Test
  void testMultipleChangesOnNodesOfWay() {
    // Way various things changed
    // first appearance
    Geometry result = buildGeometry(ways(104L, 0));
    assertTrue(result instanceof LineString);
    assertEquals(2, result.getNumPoints());

    // last appearance
    result = buildGeometry(ways(104L, 1));
    assertTrue(result instanceof LineString);
    assertEquals(3, result.getNumPoints());
  }

  @Test
  void testMultipleChangesOnNodesAndWays() {
    // way and nodes have different changes
    // first appearance
    Geometry result = buildGeometry(ways(105L, 0));
    assertTrue(result instanceof LineString);
    assertEquals(2, result.getNumPoints());

    // second appearance
    result = buildGeometry(ways(105L, 1));
    assertTrue(result instanceof LineString);
    assertEquals(2, result.getNumPoints());

    // third appearance
    result = buildGeometry(ways(105L, 2));
    assertTrue(result.isEmpty());

    // last appearance
    result = buildGeometry(ways(105L, 3));
    assertTrue(result instanceof LineString);
    assertEquals(4, result.getNumPoints());
  }

  // MULTIPOLYGON(((1.45 1.45, 1.46 1.45, 1.46 1.44, 1.45 1.44)))
  @Test
  void testPolygonAreaYesTagDisappears() {
    // way seems to be polygon with area=yes, later linestring because area=yes deleted
    // first appearance
    Geometry result = buildGeometry(ways(106L, 0));
    assertTrue(result instanceof Polygon);
    assertEquals(5, result.getNumPoints());

    // last appearance
    result = buildGeometry(ways(106L, 1));
    assertTrue(result instanceof LineString);
    assertEquals(5, result.getNumPoints());
  }

  @Test
  void testPolygonAreaYesNodeDisappears() {
    // way seems to be polygon with area=yes, later linestring because area=yes deleted
    // first appearance
    Geometry result = buildGeometry(ways(107L, 0));
    assertTrue(result instanceof Polygon);
    assertEquals(5, result.getNumPoints());

    // last appearance
    result = buildGeometry(ways(107L, 1));
    assertTrue(result instanceof LineString);
    assertEquals(4, result.getNumPoints());
  }

  @Test
  void testNullRefEntities() {
    // broken way references (=invalid OSM data) can occur after "partial" data redactions
    OSMWay way = ways(177974941L, 0);
    Geometry result = buildGeometry(way);
    // no exception should have been thrown at this point
    assertTrue(result.getCoordinates().length < way.getMembers().length);
  }
}
