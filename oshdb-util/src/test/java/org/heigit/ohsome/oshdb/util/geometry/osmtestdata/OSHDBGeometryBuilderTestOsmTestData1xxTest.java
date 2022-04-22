package org.heigit.ohsome.oshdb.util.geometry.osmtestdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryTest;
import org.heigit.ohsome.oshdb.util.geometry.helpers.FakeTagInterpreterAreaMultipolygonAllOuters;
import org.heigit.ohsome.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;

/**
 * Tests the {@link OSHDBGeometryBuilder} class: basic geometry tests.
 *
 * @see <a href="https://github.com/osmcode/osm-testdata/tree/master/grid">osm-testdata</a>
 */
class OSHDBGeometryBuilderTestOsmTestData1xxTest extends OSHDBGeometryTest {
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");
  private static final double DELTA = 1E-6;

  OSHDBGeometryBuilderTestOsmTestData1xxTest() {
    super("./src/test/resources/osm-testdata/all.osm");
  }

  @Test
  void test100() {
    // A single node
    Geometry result = buildGeometry(nodes(100000L, 0), timestamp);
    assertTrue(result instanceof Point);
    assertEquals(1.02, ((Point) result).getX(), DELTA);
    assertEquals(1.02, ((Point) result).getY(), DELTA);
  }

  /* @Test
  public void test101() {
    // 101: 4 single nodes
    // the same like test100() just with 4 nodes
  } */

  @Test
  void test102() {
    // Two nodes at same location
    Geometry result1 = buildGeometry(nodes(102000L, 0), timestamp);
    Geometry result2 = buildGeometry(nodes(102001L, 0), timestamp);
    assertTrue(result1 instanceof Point);
    assertTrue(result2 instanceof Point);
    assertEquals(((Point) result2).getX(), ((Point) result1).getX(), DELTA);
    assertEquals(((Point) result1).getY(), ((Point) result2).getY(), DELTA);
  }

  @Test
  void test110() {
    // Way with two nodes
    Geometry result = buildGeometry(ways(110800L, 0), timestamp);
    assertTrue(result instanceof LineString);
    assertEquals(2, result.getCoordinates().length);
  }

  /* @Test
  public void test111() {
    // 111 : Way with 4 nodes
    // the same like test110() just with 4 nodes
  } */

  @Test
  void test112() {
    // Closed way with four nodes
    Geometry result = buildGeometry(ways(112800L, 0), timestamp);
    assertTrue(result instanceof LineString);
    assertEquals(5, result.getCoordinates().length);
    assertEquals(
        ((LineString) result).getCoordinateN(result.getNumPoints() - 1),
        ((LineString) result).getCoordinateN(0)
    );
  }

  @Test
  void test113() {
    // Two separate ways
    Geometry result1 = buildGeometry(ways(113800L, 0), timestamp);
    Geometry result2 = buildGeometry(ways(113801L, 0), timestamp);
    assertTrue(result1 instanceof LineString);
    assertTrue(result2 instanceof LineString);
    assertFalse(result1.crosses(result2));
  }

  @Test
  void test114() {
    // Two ways connected end-to-beginning
    TagInterpreter areaDecider = new FakeTagInterpreterAreaMultipolygonAllOuters();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(ways(114800L, 0), timestamp, areaDecider);
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(ways(114801L, 0), timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    assertTrue(result2 instanceof LineString);
    assertEquals(
        ((LineString) result1).getCoordinateN(result1.getNumPoints() - 1),
        ((LineString) result2).getCoordinateN(0)
    );
  }

  @Test
  void test115() {
    // Two ways connected end-to-end
    Geometry result1 = buildGeometry(ways(115800L, 0), timestamp);
    Geometry result2 = buildGeometry(ways(115801L, 0), timestamp);
    assertTrue(result1 instanceof LineString);
    assertTrue(result2 instanceof LineString);
    assertEquals(
        ((LineString) result1).getCoordinateN(result1.getNumPoints() - 1),
        ((LineString) result2).getCoordinateN(result2.getNumPoints() - 1)
    );
  }

  @Test
  void test116() {
    // Three ways connected in a closed loop
    Geometry result1 = buildGeometry(ways(116800L, 0), timestamp);
    Geometry result2 = buildGeometry(ways(116801L, 0), timestamp);
    Geometry result3 = buildGeometry(ways(116802L, 0), timestamp);
    assertTrue(result1 instanceof LineString);
    assertTrue(result2 instanceof LineString);
    assertTrue(result3 instanceof LineString);
    int idx1 = result1.getNumPoints();
    int idx2 = result2.getNumPoints();
    int idx3 = result3.getNumPoints();
    assertEquals(
        ((LineString) result3).getCoordinateN(idx3 - 1),
        ((LineString) result1).getCoordinateN(0)
    );
    assertEquals(
        ((LineString) result1).getCoordinateN(idx1 - 1),
        ((LineString) result2).getCoordinateN(0)
    );
    assertEquals(
        ((LineString) result2).getCoordinateN(idx2 - 1),
        ((LineString) result3).getCoordinateN(0)
    );
  }

  @Test
  void test120() {
    // Way without any nodes
    Geometry result = buildGeometry(ways(120800L, 0), timestamp);
    assertNotNull(result);
  }

  @Test
  void test121() {
    // Way with a single node
    Geometry result = buildGeometry(ways(121800L, 0), timestamp);
    assertNotNull(result);
  }

  @Test
  void test122() {
    // Same node twice in way
    Geometry result = buildGeometry(ways(122800L, 0), timestamp);
    assertNotNull(result);
  }

  @Test
  void test123() {
    // Way with two nodes at same position
    Geometry result = buildGeometry(ways(123800L, 0), timestamp);
    assertNotNull(result);
  }

  @Test
  void test124() {
    // Way with three nodes, first two nodes have the same position
    Geometry result = buildGeometry(ways(124800L, 0), timestamp);
    assertNotNull(result);
  }

  @Test
  void test130() {
    // Crossing ways without common node
    Geometry result1 = buildGeometry(ways(130800L, 0), timestamp);
    Geometry result2 = buildGeometry(ways(130801L, 0), timestamp);
    assertTrue(result1 instanceof LineString);
    assertTrue(result2 instanceof LineString);
    assertTrue(result1.crosses(result2));
    for (int j = 0; j < result1.getLength(); j++) {
      for (int i = 0; i < result2.getLength(); i++) {
        assertNotEquals(((LineString) result1).getCoordinateN(j),
            ((LineString) result2).getCoordinateN(i));
      }
    }
  }

  @Test
  void test131() {
    // Crossing ways with common node
    Geometry result1 = buildGeometry(ways(131800L, 0), timestamp);
    Geometry result2 = buildGeometry(ways(131801L, 0), timestamp);
    assertTrue(result1 instanceof LineString);
    assertTrue(result2 instanceof LineString);
    assertTrue(result1.intersects(result2));
    Set<Coordinate> res1Coords = Arrays
        .stream(result1.getCoordinates())
        .collect(Collectors.toSet());
    assertTrue(Arrays
        .stream(result2.getCoordinates())
        .anyMatch(res1Coords::contains));
  }

  @Test
  void test132() {
    // Crossing ways without common node, but crossing node at same position
    Geometry result1 = buildGeometry(ways(132800L, 0), timestamp);
    Geometry result2 = buildGeometry(ways(132801L, 0), timestamp);
    assertTrue(result1 instanceof LineString);
    assertTrue(result2 instanceof LineString);
    assertTrue(result1.crosses(result2));
    assertTrue(result1.intersects(result2));
    Set<Coordinate> res1Coords = Arrays
        .stream(result1.getCoordinates())
        .collect(Collectors.toSet());
    assertTrue(Arrays
        .stream(result2.getCoordinates())
        .anyMatch(res1Coords::contains));
  }

  @Test
  void test133() {
    // Self-crossing way without common node
    Geometry result = buildGeometry(ways(133800L, 0), timestamp);
    assertTrue(result instanceof LineString);
    // If a LineString intersects like that, isSimple() will return false as self-intersection is
    // not allowed for Simple Geometries.
    assertFalse(result.isSimple());
    // punkt mit punkt, linie bilden, crosses
    GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] xy1 = new Coordinate[]{(((LineString) result).getCoordinateN(0)),
        (((LineString) result).getCoordinateN(1))};
    LineString lineString1 = geometryFactory.createLineString(xy1);
    Coordinate[] xy2 = new Coordinate[]{(((LineString) result).getCoordinateN(2)),
        (((LineString) result).getCoordinateN(3))};
    LineString lineString2 = geometryFactory.createLineString(xy2);
    assertTrue(lineString1.crosses(lineString2));
    assertEquals(4, result.getCoordinates().length);
  }

  @Test
  void test134() {
    // Self-crossing way with common node
    Geometry result = buildGeometry(ways(134800L, 0), timestamp);
    assertTrue(result instanceof LineString);
    assertFalse(result.isSimple());
    // punkt mit punkt, linie bilden, crosses
    GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] xy1 = new Coordinate[]{(((LineString) result).getCoordinateN(0)),
        (((LineString) result).getCoordinateN(2))};
    LineString lineString1 = geometryFactory.createLineString(xy1);
    Coordinate[] xy2 = new Coordinate[]{(((LineString) result).getCoordinateN(3)),
        (((LineString) result).getCoordinateN(5))};
    LineString lineString2 = geometryFactory.createLineString(xy2);
    assertTrue(lineString1.intersects(lineString2));
    assertEquals(6, result.getCoordinates().length);
  }
}
