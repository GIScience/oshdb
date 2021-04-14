package org.heigit.ohsome.oshdb.util.geometry.osmhistorytestdata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.ohsome.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.ohsome.oshdb.util.xmlreader.OSMXmlReader;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;

public class OSHDBGeometryBuilderTestOsmHistoryTestDataWaysTest {
  private final OSMXmlReader testData = new OSMXmlReader();
  TagInterpreter areaDecider;
  private static final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestOsmHistoryTestDataWaysTest() {
    testData.add("./src/test/resources/different-timestamps/way.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(testData);
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
  public void testGeometryChange() {
    // Way getting more nodes, one disappears
    // first appearance
    OSMEntity entity1 = testData.ways().get(100L).get(0);
    OSHDBTimestamp timestamp = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    double[][] expectedCoordinates1 = {{1.42, 1.22}, {1.42, 1.23}, {1.42, 1.24}, {1.42, 1.25}};
    checkLineString(expectedCoordinates1, result1, 4);
    // second appearance
    OSMEntity entity2 = testData.ways().get(100L).get(1);
    OSHDBTimestamp timestamp2 = entity2.getTimestamp();
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
    double[][] expectedCoordinates2 = {{1.42, 1.22}, {1.42, 1.23}, {1.42, 1.24}, {1.42, 1.25},
        {1.42, 1.26}, {1.42, 1.27}, {1.42, 1.28}, {1.43, 1.29}};
    checkLineString(expectedCoordinates2, result2, 8);
    // last appearance
    OSMEntity entity3 = testData.ways().get(100L).get(2);
    OSHDBTimestamp timestamp3 = entity3.getTimestamp();
    Geometry result3 = OSHDBGeometryBuilder.getGeometry(entity3, timestamp3, areaDecider);
    double[][] expectedCoordinates3 = {{1.42, 1.22}, {1.42, 1.23}, {1.42, 1.24}, {1.42, 1.25},
        {1.42, 1.26}, {1.42, 1.28}, {1.43, 1.29}, {1.43, 1.30}, {1.43, 1.31}};
    checkLineString(expectedCoordinates3, result3, 9);
    // timestamp after last one
    OSMEntity entityAfter = testData.ways().get(100L).get(2);
    OSHDBTimestamp timestampAfter =  TimestampParser.toOSHDBTimestamp("2012-01-01T00:00:00Z");
    Geometry resultAfter = OSHDBGeometryBuilder.getGeometry(entityAfter, timestampAfter,
        areaDecider);
    double[][] expectedCoordinatesAfter = {{1.42, 1.22}, {1.42, 1.23}, {1.42, 1.24}, {1.42, 1.25},
        {1.42, 1.26}, {1.42, 1.28}, {1.43, 1.29}, {1.43, 1.30}, {1.43, 1.31}};
    checkLineString(expectedCoordinatesAfter, resultAfter, 9);
  }

  @Test
  public void testGeometryChangeOfNodeInWay() {
    // Way with two then three nodes, changing lat lon
    // first appearance
    OSMEntity entity1 = testData.ways().get(101L).get(0);
    OSHDBTimestamp timestamp = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    double[][] expectedCoordinates1 = {{1.42, 1.22}, {1.44, 1.22}};
    checkLineString(expectedCoordinates1, result1, 2);
    // last appearance
    OSMEntity entity2 = testData.ways().get(101L).get(1);
    OSHDBTimestamp timestamp2 = entity2.getTimestamp();
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
    double[][] expectedCoordinates2 = {{1.425, 1.23}, {1.44, 1.23}, {1.43, 1.30}};
    checkLineString(expectedCoordinates2, result2, 3);
    // timestamp in between
    OSHDBTimestamp timestampBetween =  TimestampParser.toOSHDBTimestamp("2009-02-01T00:00:00Z");
    OSMEntity entityBetween = testData.ways().get(101L).get(0);
    Geometry resultBetween = OSHDBGeometryBuilder.getGeometry(entityBetween, timestampBetween,
        areaDecider);
    double[][] expectedCoordinatesBetween = {{1.42, 1.225}, {1.445, 1.225}};
    checkLineString(expectedCoordinatesBetween, resultBetween, 2);
  }

  @Test
  public void testVisibleChange() {
    // Way visible changed
    // first appearance
    OSMEntity entity1 = testData.ways().get(102L).get(0);
    OSHDBTimestamp timestamp = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    assertEquals(3, result1.getNumPoints());

    // last appearance
    OSMEntity entity2 = testData.ways().get(102L).get(1);
    OSHDBTimestamp timestamp2 = entity2.getTimestamp();
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
    assertTrue(result2.isEmpty());
  }

  @Test
  public void testTagChange() {
    // Way tags changed
    // first appearance
    OSMEntity entity1 = testData.ways().get(103L).get(0);
    OSHDBTimestamp timestamp = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    assertEquals(3, result1.getNumPoints());
    // second appearance
    OSMEntity entity2 = testData.ways().get(103L).get(1);
    OSHDBTimestamp timestamp2 = entity2.getTimestamp();
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
    assertTrue(result2 instanceof LineString);
    assertEquals(5, result2.getNumPoints());
    // last appearance
    OSMEntity entity3 = testData.ways().get(103L).get(1);
    OSHDBTimestamp timestamp3 = entity3.getTimestamp();
    Geometry result3 = OSHDBGeometryBuilder.getGeometry(entity3, timestamp3, areaDecider);
    assertTrue(result3 instanceof LineString);
    assertEquals(5, result3.getNumPoints());

  }

  @Test
  public void testMultipleChangesOnNodesOfWay() {
    // Way various things changed
    // first appearance
    OSMEntity entity1 = testData.ways().get(104L).get(0);
    OSHDBTimestamp timestamp = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    assertEquals(2, result1.getNumPoints());

    // last appearance
    OSMEntity entity2 = testData.ways().get(104L).get(1);
    OSHDBTimestamp timestamp2 = entity2.getTimestamp();
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
    assertTrue(result2 instanceof LineString);
    assertEquals(3, result2.getNumPoints());
  }

  @Test
  public void testMultipleChangesOnNodesAndWays() {
    // way and nodes have different changes
    // first appearance
    OSMEntity entity1 = testData.ways().get(105L).get(0);
    OSHDBTimestamp timestamp = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    assertEquals(2, result1.getNumPoints());
    // second appearance
    OSMEntity entity2 = testData.ways().get(105L).get(1);
    OSHDBTimestamp timestamp2 = entity2.getTimestamp();
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
    assertTrue(result2 instanceof LineString);
    assertEquals(2, result2.getNumPoints());
    // third appearance
    OSMEntity entity3 = testData.ways().get(105L).get(2);
    OSHDBTimestamp timestamp3 = entity3.getTimestamp();
    Geometry result3 = OSHDBGeometryBuilder.getGeometry(entity3, timestamp3, areaDecider);
    assertTrue(result3.isEmpty());
    // last appearance
    OSMEntity entity4 = testData.ways().get(105L).get(3);
    OSHDBTimestamp timestamp4 = entity4.getTimestamp();
    Geometry result4 = OSHDBGeometryBuilder.getGeometry(entity4, timestamp4, areaDecider);
    assertTrue(result4 instanceof LineString);
    assertEquals(4, result4.getNumPoints());

  }

  // MULTIPOLYGON(((1.45 1.45, 1.46 1.45, 1.46 1.44, 1.45 1.44)))
  @Test
  public void testPolygonAreaYesTagDisappears() {
    // way seems to be polygon with area=yes, later linestring because area=yes deleted
    // first appearance
    OSMEntity entity1 = testData.ways().get(106L).get(0);
    OSHDBTimestamp timestamp = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    assertTrue(result1 instanceof Polygon);
    assertEquals(5, result1.getNumPoints());

    // last appearance
    OSMEntity entity4 = testData.ways().get(106L).get(1);
    OSHDBTimestamp timestamp4 = entity4.getTimestamp();
    Geometry result4 = OSHDBGeometryBuilder.getGeometry(entity4, timestamp4, areaDecider);
    assertTrue(result4 instanceof LineString);
    assertEquals(5, result4.getNumPoints());
  }


  @Test
  public void testPolygonAreaYesNodeDisappears() {
    // way seems to be polygon with area=yes, later linestring because area=yes deleted
    // first appearance
    OSMEntity entity1 = testData.ways().get(107L).get(0);
    OSHDBTimestamp timestamp = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    assertTrue(result1 instanceof Polygon);
    assertEquals(5, result1.getNumPoints());

    // last appearance
    OSMEntity entity4 = testData.ways().get(107L).get(1);
    OSHDBTimestamp timestamp4 = entity4.getTimestamp();
    Geometry result4 = OSHDBGeometryBuilder.getGeometry(entity4, timestamp4, areaDecider);
    assertTrue(result4 instanceof LineString);
    assertEquals(4, result4.getNumPoints());
  }

  @Test
  public void testNullRefEntities() {
    // broken way references (=invalid OSM data) can occur after "partial" data redactions
    OSMWay way = testData.ways().get(177974941L).get(0);
    OSHDBTimestamp timestamp = way.getTimestamp();
    Geometry result = OSHDBGeometryBuilder.getGeometry(way, timestamp, areaDecider);
    // no exception should have been thrown at this point
    assertTrue(result.getCoordinates().length < way.getMembers().length);
  }
}
