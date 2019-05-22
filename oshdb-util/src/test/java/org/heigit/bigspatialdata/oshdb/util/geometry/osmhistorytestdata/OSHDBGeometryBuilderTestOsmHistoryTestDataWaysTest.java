package org.heigit.bigspatialdata.oshdb.util.geometry.osmhistorytestdata;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.bigspatialdata.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.xmlreader.OSMXmlReader;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OSHDBGeometryBuilderTestOsmHistoryTestDataWaysTest {
  private final OSMXmlReader testData = new OSMXmlReader();
  TagInterpreter areaDecider;
  private final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestOsmHistoryTestDataWaysTest() {
    testData.add("./src/test/resources/different-timestamps/way.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(testData);
  }

  @Test
  public void testGeometryChange() throws ParseException {
    // Way getting more nodes, one disappears
    // first appearance
    OSMEntity entity1 = testData.ways().get(100L).get(0);
    OSHDBTimestamp timestamp = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    assertEquals(4,result1.getNumPoints());
    assertEquals(1.42, (((LineString) result1).getCoordinateN(0)).x, DELTA);
    assertEquals(1.22, (((LineString) result1).getCoordinateN(0)).y, DELTA);
    assertEquals(1.42, (((LineString) result1).getCoordinateN(1)).x, DELTA);
    assertEquals(1.23, (((LineString) result1).getCoordinateN(1)).y, DELTA);
    assertEquals(1.42, (((LineString) result1).getCoordinateN(2)).x, DELTA);
    assertEquals(1.24, (((LineString) result1).getCoordinateN(2)).y, DELTA);
    assertEquals(1.42, (((LineString) result1).getCoordinateN(3)).x, DELTA);
    assertEquals(1.25, (((LineString) result1).getCoordinateN(3)).y, DELTA);
    // second appearance
    OSMEntity entity2 = testData.ways().get(100L).get(1);
    OSHDBTimestamp timestamp2 = entity2.getTimestamp();
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
    assertTrue(result2 instanceof LineString);
    assertEquals(8,result2.getNumPoints());
    assertEquals(1.42, (((LineString) result2).getCoordinateN(0)).x, DELTA);
    assertEquals(1.22, (((LineString) result2).getCoordinateN(0)).y, DELTA);
    assertEquals(1.42, (((LineString) result2).getCoordinateN(1)).x, DELTA);
    assertEquals(1.23, (((LineString) result2).getCoordinateN(1)).y, DELTA);
    assertEquals(1.42, (((LineString) result2).getCoordinateN(2)).x, DELTA);
    assertEquals(1.24, (((LineString) result2).getCoordinateN(2)).y, DELTA);
    assertEquals(1.42, (((LineString) result2).getCoordinateN(3)).x, DELTA);
    assertEquals(1.25, (((LineString) result2).getCoordinateN(3)).y, DELTA);
    assertEquals(1.42, (((LineString) result2).getCoordinateN(4)).x, DELTA);
    assertEquals(1.26, (((LineString) result2).getCoordinateN(4)).y, DELTA);
    assertEquals(1.42, (((LineString) result2).getCoordinateN(5)).x, DELTA);
    assertEquals(1.27, (((LineString) result2).getCoordinateN(5)).y, DELTA);
    assertEquals(1.42, (((LineString) result2).getCoordinateN(6)).x, DELTA);
    assertEquals(1.28, (((LineString) result2).getCoordinateN(6)).y, DELTA);
    assertEquals(1.43, (((LineString) result2).getCoordinateN(7)).x, DELTA);
    assertEquals(1.29, (((LineString) result2).getCoordinateN(7)).y, DELTA);
    // last appearance
    OSMEntity entity3 = testData.ways().get(100L).get(2);
    OSHDBTimestamp timestamp3 = entity3.getTimestamp();
    Geometry result3 = OSHDBGeometryBuilder.getGeometry(entity3, timestamp3, areaDecider);
    assertTrue(result3 instanceof LineString);
    assertEquals(9,result3.getNumPoints());
    assertEquals(1.42, (((LineString) result3).getCoordinateN(0)).x, DELTA);
    assertEquals(1.22, (((LineString) result3).getCoordinateN(0)).y, DELTA);
    assertEquals(1.42, (((LineString) result3).getCoordinateN(1)).x, DELTA);
    assertEquals(1.23, (((LineString) result3).getCoordinateN(1)).y, DELTA);
    assertEquals(1.42, (((LineString) result3).getCoordinateN(2)).x, DELTA);
    assertEquals(1.24, (((LineString) result3).getCoordinateN(2)).y, DELTA);
    assertEquals(1.42, (((LineString) result3).getCoordinateN(3)).x, DELTA);
    assertEquals(1.25, (((LineString) result3).getCoordinateN(3)).y, DELTA);
    assertEquals(1.42, (((LineString) result3).getCoordinateN(4)).x, DELTA);
    assertEquals(1.26, (((LineString) result3).getCoordinateN(4)).y, DELTA);
    assertEquals(1.42, (((LineString) result3).getCoordinateN(5)).x, DELTA);
    assertEquals(1.28, (((LineString) result3).getCoordinateN(5)).y, DELTA);
    assertEquals(1.43, (((LineString) result3).getCoordinateN(6)).x, DELTA);
    assertEquals(1.29, (((LineString) result3).getCoordinateN(6)).y, DELTA);
    assertEquals(1.43, (((LineString) result3).getCoordinateN(7)).x, DELTA);
    assertEquals(1.30, (((LineString) result3).getCoordinateN(7)).y, DELTA);
    assertEquals(1.43, (((LineString) result3).getCoordinateN(8)).x, DELTA);
    assertEquals(1.31, (((LineString) result3).getCoordinateN(8)).y, DELTA);
    // timestamp after last one
    OSMEntity entity_after = testData.ways().get(100L).get(2);
    OSHDBTimestamp timestamp_after =  TimestampParser.toOSHDBTimestamp("2012-01-01T00:00:00Z");
    Geometry result_after = OSHDBGeometryBuilder.getGeometry(entity_after, timestamp_after, areaDecider);
    assertTrue(result_after instanceof LineString);
    assertEquals(1.42, (((LineString) result3).getCoordinateN(0)).x, DELTA);
    assertEquals(1.22, (((LineString) result3).getCoordinateN(0)).y, DELTA);
    assertEquals(1.42, (((LineString) result3).getCoordinateN(1)).x, DELTA);
    assertEquals(1.23, (((LineString) result3).getCoordinateN(1)).y, DELTA);
    assertEquals(1.42, (((LineString) result3).getCoordinateN(2)).x, DELTA);
    assertEquals(1.24, (((LineString) result3).getCoordinateN(2)).y, DELTA);
    assertEquals(1.42, (((LineString) result3).getCoordinateN(3)).x, DELTA);
    assertEquals(1.25, (((LineString) result3).getCoordinateN(3)).y, DELTA);
    assertEquals(1.42, (((LineString) result3).getCoordinateN(4)).x, DELTA);
    assertEquals(1.26, (((LineString) result3).getCoordinateN(4)).y, DELTA);
    assertEquals(1.42, (((LineString) result3).getCoordinateN(5)).x, DELTA);
    assertEquals(1.28, (((LineString) result3).getCoordinateN(5)).y, DELTA);
    assertEquals(1.43, (((LineString) result3).getCoordinateN(6)).x, DELTA);
    assertEquals(1.29, (((LineString) result3).getCoordinateN(6)).y, DELTA);
    assertEquals(1.43, (((LineString) result3).getCoordinateN(7)).x, DELTA);
    assertEquals(1.30, (((LineString) result3).getCoordinateN(7)).y, DELTA);
    assertEquals(1.43, (((LineString) result3).getCoordinateN(8)).x, DELTA);
    assertEquals(1.31, (((LineString) result3).getCoordinateN(8)).y, DELTA);
  }

  @Test
  public void testGeometryChangeOfNodeInWay() throws ParseException {
    // Way with two then three nodes, changing lat lon
    // first appearance
    OSMEntity entity1 = testData.ways().get(101L).get(0);
    OSHDBTimestamp timestamp = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    assertEquals(2,result1.getNumPoints());
    assertEquals(1.42, (((LineString) result1).getCoordinateN(0)).x, DELTA);
    assertEquals(1.22, (((LineString) result1).getCoordinateN(0)).y, DELTA);
    assertEquals(1.44, (((LineString) result1).getCoordinateN(1)).x, DELTA);
    assertEquals(1.22, (((LineString) result1).getCoordinateN(1)).y, DELTA);
    // last appearance
    OSMEntity entity2 = testData.ways().get(101L).get(1);
    OSHDBTimestamp timestamp2 = entity2.getTimestamp();
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
    assertTrue(result2 instanceof LineString);
    assertEquals(3,result2.getNumPoints());
    assertEquals(1.425, (((LineString) result2).getCoordinateN(0)).x, DELTA);
    assertEquals(1.23, (((LineString) result2).getCoordinateN(0)).y, DELTA);
    assertEquals(1.44, (((LineString) result2).getCoordinateN(1)).x, DELTA);
    assertEquals(1.23, (((LineString) result2).getCoordinateN(1)).y, DELTA);
    assertEquals(1.43, (((LineString) result2).getCoordinateN(2)).x, DELTA);
    assertEquals(1.30, (((LineString) result2).getCoordinateN(2)).y, DELTA);
    // timestamp in between
    OSHDBTimestamp timestamp_between =  TimestampParser.toOSHDBTimestamp("2009-02-01T00:00:00Z");
    OSMEntity entity_between = testData.ways().get(101L).get(0);
    Geometry result_between = OSHDBGeometryBuilder.getGeometry(entity_between, timestamp_between, areaDecider);
    assertTrue(result_between instanceof LineString);
    assertEquals(2,result_between.getNumPoints());
    assertEquals(1.42, (((LineString) result_between).getCoordinateN(0)).x, DELTA);
    assertEquals(1.225, (((LineString) result_between).getCoordinateN(0)).y, DELTA);
    assertEquals(1.445, (((LineString) result_between).getCoordinateN(1)).x, DELTA);
    assertEquals(1.225, (((LineString) result_between).getCoordinateN(1)).y, DELTA);
  }

  @Test
  public void testVisibleChange() throws ParseException {
    // Way visible schanged
    // first appearance
    OSMEntity entity1 = testData.ways().get(102L).get(0);
    OSHDBTimestamp timestamp = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    assertEquals(3,result1.getNumPoints());

    // last appearance
    OSMEntity entity2 = testData.ways().get(102L).get(1);
    OSHDBTimestamp timestamp2 = entity2.getTimestamp();
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
    assertTrue(result2.isEmpty());
  }

  @Test
  public void testTagChange() throws ParseException {
    // Way tags schanged
    // first appearance
    OSMEntity entity1 = testData.ways().get(103L).get(0);
    OSHDBTimestamp timestamp = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    assertEquals(3,result1.getNumPoints());
    // second appearance
    OSMEntity entity2 = testData.ways().get(103L).get(1);
    OSHDBTimestamp timestamp2 = entity2.getTimestamp();
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
    assertTrue(result2 instanceof LineString);
    assertEquals(5,result2.getNumPoints());
    // last appearance
    OSMEntity entity3 = testData.ways().get(103L).get(1);
    OSHDBTimestamp timestamp3 = entity3.getTimestamp();
    Geometry result3 = OSHDBGeometryBuilder.getGeometry(entity3, timestamp3, areaDecider);
    assertTrue(result3 instanceof LineString);
    assertEquals(5,result3.getNumPoints());

  }

  @Test
  public void testMultipleChangesOnNodesOfWay() throws ParseException {
    // Way various things schanged
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
  public void testMultipleChangesOnNodesAndWays() throws ParseException {
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
    assertEquals(2,result2.getNumPoints());
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
  // MULTIPOLYGON(((1.45 1.45,1.46 1.45,1.46 1.44,1.45 1.44)))
  @Test
  public void testPolygonAreaYesTagDisappears() throws ParseException {
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
  public void testPolygonAreaYesNodeDisappears() throws ParseException {
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
}
