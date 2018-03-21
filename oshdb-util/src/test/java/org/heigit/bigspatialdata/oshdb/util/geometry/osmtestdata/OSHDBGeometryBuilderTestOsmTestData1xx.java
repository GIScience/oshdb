package org.heigit.bigspatialdata.oshdb.util.geometry.osmtestdata;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;

import java.util.Arrays;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.FakeTagInterpreterAreaMultipolygonAllOuters;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.FakeTagInterpreterAreaNever;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.test.OSMXmlReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class OSHDBGeometryBuilderTestOsmTestData1xx {
  private final OSMXmlReader testData = new OSMXmlReader();
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");
  private final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestOsmTestData1xx() {
    testData.add("./src/test/resources/osm-testdata/all.osm");
  }

  @Test
  public void test100() {
    // A single node
    OSMEntity entity = testData.nodes().get(100000L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, null);
    assertTrue(result instanceof Point);
    assertEquals(1.02, ((Point) result).getX(), DELTA);
    assertEquals(1.02, ((Point) result).getY(), DELTA);
  }

  // 101: 4 single nodes

  @Test
  public void test102() {
    // Two nodes at same location
    OSMEntity entity1 = testData.nodes().get(102000L).get(0);
    OSMEntity entity2 = testData.nodes().get(102001L).get(0);
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, null);
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp, null);
    assertTrue(result1 instanceof Point);
    assertTrue(result2 instanceof Point);
    assertEquals(((Point) result2).getX(), ((Point) result1).getX(), DELTA);
    assertEquals(((Point) result1).getY(), ((Point) result2).getY(), DELTA);
  }

  @Test
  public void test110() {
    // Way with two nodes
    OSMEntity entity1 = testData.ways().get(110800L).get(0);
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, null);
    assertTrue(result1 instanceof LineString);
    assertEquals(2, result1.getCoordinates().length, DELTA);
  }

  // 111 : Way with 4 nodes

  @Test
  public void test112() {
    // Closed way with four nodes
    OSMEntity entity1 = testData.ways().get(112800L).get(0);
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, null);
    assertTrue(result1 instanceof LineString);
    assertEquals(4, result1.getCoordinates().length, DELTA);
    assertEquals(
        ((LineString) result1).getCoordinateN(result1.getNumPoints()-1),
        ((LineString) result1).getCoordinateN(0)
    );
  }

  @Test
  public void test113() {
    // Two separate ways
    OSMEntity entity1 = testData.ways().get(113800L).get(0);
    OSMEntity entity2 = testData.ways().get(113801L).get(0);
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, null);
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp, null);
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
        ((LineString) result1).getCoordinateN(result1.getNumPoints()-1),
        ((LineString) result2).getCoordinateN(0)
    );
  }

  @Test
  public void test115() {
    // Two ways connected end-to-end
    OSMEntity entity1 = testData.ways().get(115800L).get(0);
    OSMEntity entity2 = testData.ways().get(115801L).get(0);
    TagInterpreter areaDecider = new FakeTagInterpreterAreaMultipolygonAllOuters();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    assertTrue(result2 instanceof LineString);
    assertEquals(
        ((LineString) result1).getCoordinateN(result1.getNumPoints()-1),
        ((LineString) result2).getCoordinateN(result2.getNumPoints()-1)
    );
  }
  @Test
  public void test116() {
    // Three ways connected in a closed loop
    OSMEntity entity1 = testData.ways().get(116800L).get(0);
    OSMEntity entity2 = testData.ways().get(116801L).get(0);
    OSMEntity entity3 = testData.ways().get(116802L).get(0);
    TagInterpreter areaDecider = new FakeTagInterpreterAreaMultipolygonAllOuters();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp, areaDecider);
    Geometry result3 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    assertTrue(result2 instanceof LineString);
    assertTrue(result3 instanceof LineString);
    assertEquals(
        ((LineString) result3).getCoordinateN(result3.getNumPoints()-1),
        ((LineString) result1).getCoordinateN(0)
    );
    assertEquals(
        ((LineString) result1).getCoordinateN(result1.getNumPoints()-1),
        ((LineString) result2).getCoordinateN(0)
    );
    assertEquals(
        ((LineString) result2).getCoordinateN(result2.getNumPoints()-1),
        ((LineString) result3).getCoordinateN(0)
    );
  }

  @Test
  public void test130() {
    // Crossing ways without common node
    OSMEntity entity1 = testData.ways().get(130800L).get(0);
    OSMEntity entity2 = testData.ways().get(130801L).get(0);
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, null);
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp, null);
    assertTrue(result1 instanceof LineString);
    assertTrue(result2 instanceof LineString);
    assertTrue(result1.crosses(result2));
    assertFalse(result1.intersects(result2));
  }

  @Test
  public void test131() {
    // Crossing ways with common node
    OSMEntity entity1 = testData.ways().get(131800L).get(0);
    OSMEntity entity2 = testData.ways().get(131801L).get(0);
    OSMEntity entityP = testData.nodes().get(131004L).get(0);
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, null);
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp, null);
    assertTrue(result1 instanceof LineString);
    assertTrue(result2 instanceof LineString);
    assertTrue(result1.intersects(result2));
    assertTrue(Arrays.asList(result1).contains(entityP));
    assertTrue(Arrays.asList(result2).contains(entityP));
  }

  @Test
  public void test132() {
    // Crossing ways without common node, but crossing node at same position
    OSMEntity entity1 = testData.ways().get(132800L).get(0);
    OSMEntity entity2 = testData.ways().get(132801L).get(0);
    OSMEntity entityP = testData.nodes().get(132005L).get(0);
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, null);
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp, null);
    assertTrue(result1 instanceof LineString);
    assertTrue(result2 instanceof LineString);
    assertTrue(result1.crosses(result2));
    assertFalse(result1.intersects(result2));
    assertTrue(Arrays.asList(result1).contains(entityP));
    assertTrue(Arrays.asList(result2).contains(entityP));
  }

  @Test
  public void test133() {
    // Self-crossing way without common node
    OSMEntity entity1 = testData.ways().get(133800L).get(0);
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, null);
    assertTrue(result1 instanceof LineString);
    assertTrue(result1.isSimple());
  //  punkt mit punkt, linie bilden, crosses
    GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] xy1 = new Coordinate[]{(((LineString) result1).getCoordinateN(0)),
        (((LineString) result1).getCoordinateN(1))};
    LineString lineString1 = geometryFactory.createLineString(xy1);
    Coordinate[] xy2 = new Coordinate[]{(((LineString) result1).getCoordinateN(2)),
        (((LineString) result1).getCoordinateN(3))};
    LineString lineString2 = geometryFactory.createLineString(xy2);
    assertTrue(lineString1.crosses(lineString2));
    assertEquals(4, result1.getCoordinates().length, DELTA);
  }

  @Test
  public void test134() {
    // Self-crossing way with common node
    OSMEntity entity1 = testData.ways().get(133800L).get(0);
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, null);
    assertTrue(result1 instanceof LineString);
    assertTrue(result1.isSimple());
    //  punkt mit punkt, linie bilden, crosses
    GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] xy1 = new Coordinate[]{(((LineString) result1).getCoordinateN(0)),
        (((LineString) result1).getCoordinateN(1))};
    LineString lineString1 = geometryFactory.createLineString(xy1);
    Coordinate[] xy2 = new Coordinate[]{(((LineString) result1).getCoordinateN(2)),
        (((LineString) result1).getCoordinateN(3))};
    LineString lineString2 = geometryFactory.createLineString(xy2);
    assertTrue(lineString1.intersects(lineString2));
    assertEquals(5, result1.getCoordinates().length, DELTA);
  }
}
