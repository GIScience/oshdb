package org.heigit.ohsome.oshdb.util.geometry.osmtestdata;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.geometry.helpers.FakeTagInterpreterAreaMultipolygonAllOuters;
import org.heigit.ohsome.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.ohsome.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.ohsome.oshdb.util.xmlreader.OSMXmlReader;
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
public class OSHDBGeometryBuilderTestOsmTestData1xxTest {
  private final OSMXmlReader testData = new OSMXmlReader();
  TagInterpreter areaDecider;
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");
  private static final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestOsmTestData1xxTest() {
    testData.add("./src/test/resources/osm-testdata/all.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(testData);
  }


  @Test
  public void test100() {
    // A single node
    OSMEntity entity = testData.nodes().get(100000L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
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
  public void test102() {
    // Two nodes at same location
    OSMEntity entity1 = testData.nodes().get(102000L).get(0);
    OSMEntity entity2 = testData.nodes().get(102001L).get(0);
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp, areaDecider);
    assertTrue(result1 instanceof Point);
    assertTrue(result2 instanceof Point);
    assertEquals(((Point) result2).getX(), ((Point) result1).getX(), DELTA);
    assertEquals(((Point) result1).getY(), ((Point) result2).getY(), DELTA);
  }

  @Test
  public void test110() {
    // Way with two nodes
    OSMEntity entity1 = testData.ways().get(110800L).get(0);
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    assertEquals(2, result1.getCoordinates().length);
  }

  /* @Test
  public void test111() {
    // 111 : Way with 4 nodes
    // the same like test110() just with 4 nodes
  } */

  @Test
  public void test112() {
    // Closed way with four nodes
    OSMEntity entity1 = testData.ways().get(112800L).get(0);
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    assertEquals(5, result1.getCoordinates().length);
    assertEquals(
        ((LineString) result1).getCoordinateN(result1.getNumPoints() - 1),
        ((LineString) result1).getCoordinateN(0)
    );
  }

  @Test
  public void test113() {
    // Two separate ways
    OSMEntity entity1 = testData.ways().get(113800L).get(0);
    OSMEntity entity2 = testData.ways().get(113801L).get(0);
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    assertTrue(result2 instanceof LineString);
    assertFalse(result1.crosses(result2));
  }

  @Test
  public void test114() {
    // Two ways connected end-to-beginning
    OSMEntity entity1 = testData.ways().get(114800L).get(0);
    OSMEntity entity2 = testData.ways().get(114801L).get(0);
    TagInterpreter areaDecider = new FakeTagInterpreterAreaMultipolygonAllOuters();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    assertTrue(result2 instanceof LineString);
    assertEquals(
        ((LineString) result1).getCoordinateN(result1.getNumPoints() - 1),
        ((LineString) result2).getCoordinateN(0)
    );
  }

  @Test
  public void test115() {
    // Two ways connected end-to-end
    OSMEntity entity1 = testData.ways().get(115800L).get(0);
    OSMEntity entity2 = testData.ways().get(115801L).get(0);
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    assertTrue(result2 instanceof LineString);
    assertEquals(
        ((LineString) result1).getCoordinateN(result1.getNumPoints() - 1),
        ((LineString) result2).getCoordinateN(result2.getNumPoints() - 1)
    );
  }

  @Test
  public void test116() {
    // Three ways connected in a closed loop
    OSMEntity entity1 = testData.ways().get(116800L).get(0);
    OSMEntity entity2 = testData.ways().get(116801L).get(0);
    OSMEntity entity3 = testData.ways().get(116802L).get(0);
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp, areaDecider);
    Geometry result3 = OSHDBGeometryBuilder.getGeometry(entity3, timestamp, areaDecider);
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
  public void test120() {
    // Way without any nodes
    OSMEntity entity1 = testData.ways().get(120800L).get(0);
    assertDoesNotThrow(() -> {
      OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    });
  }

  @Test
  public void test121() {
    // Way with a single node
    OSMEntity entity1 = testData.ways().get(121800L).get(0);
    assertDoesNotThrow(() -> {
      OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    });
  }

  @Test
  public void test122() {
    // Same node twice in way
    OSMEntity entity1 = testData.ways().get(122800L).get(0);
    assertDoesNotThrow(() -> {
      OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    });
  }

  @Test
  public void test123() {
    // Way with two nodes at same position
    OSMEntity entity1 = testData.ways().get(123800L).get(0);
    assertDoesNotThrow(() -> {
      OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    });
  }

  @Test
  public void test124() {
    // Way with three nodes, first two nodes have the same position
    OSMEntity entity1 = testData.ways().get(124800L).get(0);
    assertDoesNotThrow(() -> {
      OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    });
  }

  @Test
  public void test130() {
    // Crossing ways without common node
    OSMEntity entity1 = testData.ways().get(130800L).get(0);
    OSMEntity entity2 = testData.ways().get(130801L).get(0);
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp, areaDecider);
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
  public void test131() {
    // Crossing ways with common node
    OSMEntity entity1 = testData.ways().get(131800L).get(0);
    OSMEntity entity2 = testData.ways().get(131801L).get(0);
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp, areaDecider);
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
  public void test132() {
    // Crossing ways without common node, but crossing node at same position
    OSMEntity entity1 = testData.ways().get(132800L).get(0);
    OSMEntity entity2 = testData.ways().get(132801L).get(0);
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp, areaDecider);
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
  public void test133() {
    // Self-crossing way without common node
    OSMEntity entity1 = testData.ways().get(133800L).get(0);
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    // If a LineString intersects like that, isSimple() will return false as self-intersection is
    // not allowed for Simple Geometries.
    assertFalse(result1.isSimple());
    // punkt mit punkt, linie bilden, crosses
    GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] xy1 = new Coordinate[]{(((LineString) result1).getCoordinateN(0)),
        (((LineString) result1).getCoordinateN(1))};
    LineString lineString1 = geometryFactory.createLineString(xy1);
    Coordinate[] xy2 = new Coordinate[]{(((LineString) result1).getCoordinateN(2)),
        (((LineString) result1).getCoordinateN(3))};
    LineString lineString2 = geometryFactory.createLineString(xy2);
    assertTrue(lineString1.crosses(lineString2));
    assertEquals(4, result1.getCoordinates().length);
  }

  @Test
  public void test134() {
    // Self-crossing way with common node
    OSMEntity entity1 = testData.ways().get(134800L).get(0);
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    assertFalse(result1.isSimple());
    // punkt mit punkt, linie bilden, crosses
    GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] xy1 = new Coordinate[]{(((LineString) result1).getCoordinateN(0)),
        (((LineString) result1).getCoordinateN(2))};
    LineString lineString1 = geometryFactory.createLineString(xy1);
    Coordinate[] xy2 = new Coordinate[]{(((LineString) result1).getCoordinateN(3)),
        (((LineString) result1).getCoordinateN(5))};
    LineString lineString2 = geometryFactory.createLineString(xy2);
    assertTrue(lineString1.intersects(lineString2));
    assertEquals(6, result1.getCoordinates().length);
  }


}
