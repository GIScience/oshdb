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
import org.locationtech.jts.geom.Point;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OSHDBGeometryBuilderTestOsmHistoryTestDataNodesTest {
  private final OSMXmlReader testData = new OSMXmlReader();
  TagInterpreter areaDecider;
  private final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestOsmHistoryTestDataNodesTest() {
    testData.add("./src/test/resources/different-timestamps/node.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(testData);
  }


  @Test
  public void testGeometryChange() {
    // A single node, lat lon changed over time
    OSMEntity entity = testData.nodes().get(1L).get(0);
    // first appearance
    OSHDBTimestamp timestamp = entity.getTimestamp();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof Point);
    assertEquals(1.42, ((Point) result).getX(), DELTA);
    assertEquals(1.22, ((Point) result).getY(), DELTA);
    // second
    OSMEntity entity2 = testData.nodes().get(1L).get(1);
    OSHDBTimestamp timestamp2 = entity2.getTimestamp();
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
    assertTrue(result2 instanceof Point);
    assertEquals(1.42, ((Point) result2).getX(), DELTA);
    assertEquals(1.225, ((Point) result2).getY(), DELTA);
    // last
    OSMEntity entity3 = testData.nodes().get(1L).get(2);
    OSHDBTimestamp timestamp3 = entity3.getTimestamp();
    Geometry result3 = OSHDBGeometryBuilder.getGeometry(entity3, timestamp3, areaDecider);
    assertTrue(result3 instanceof Point);
    assertEquals(1.425, ((Point) result3).getX(), DELTA);
    assertEquals(1.23, ((Point) result3).getY(), DELTA);
    // timestamp after newest timestamp
    OSHDBTimestamp timestamp_after =  TimestampParser.toOSHDBTimestamp("2012-01-01T00:00:00Z");
    Geometry result_after = OSHDBGeometryBuilder.getGeometry(entity3, timestamp_after, areaDecider);
    assertTrue(result_after instanceof Point);
    assertEquals(1.425, ((Point) result_after).getX(), DELTA);
    assertEquals(1.23, ((Point) result_after).getY(), DELTA);
  }

  @Test(expected = AssertionError.class)
  public void testInvalidAccess() {
    // A single node, lat lon changed over time
    OSMEntity entity = testData.nodes().get(1L).get(0);
    // timestamp before oldest timestamp
    OSHDBTimestamp timestamp_before =  TimestampParser.toOSHDBTimestamp("2007-01-01T00:00:00Z");
    Geometry result_before = OSHDBGeometryBuilder.getGeometry(entity, timestamp_before, areaDecider);
  }

  @Test
  public void testTagChange() {
    // A single node, tags changed over time
    OSMEntity entity = testData.nodes().get(2L).get(0);
    OSHDBTimestamp timestamp = entity.getTimestamp();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof Point);
    assertEquals(1.43, ((Point) result).getX(), DELTA);
    assertEquals(1.24, ((Point) result).getY(), DELTA);
    OSMEntity entity2 = testData.nodes().get(2L).get(1);
    OSHDBTimestamp timestamp2 = entity2.getTimestamp();
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
    assertTrue(result2 instanceof Point);
    assertEquals(1.43, ((Point) result2).getX(), DELTA);
    assertEquals(1.24, ((Point) result2).getY(), DELTA);
    OSMEntity entity3 = testData.nodes().get(2L).get(2);
    OSHDBTimestamp timestamp3 = entity3.getTimestamp();
    Geometry result3 = OSHDBGeometryBuilder.getGeometry(entity3, timestamp3, areaDecider);
    assertTrue(result3 instanceof Point);
    assertEquals(1.43, ((Point) result3).getX(), DELTA);
    assertEquals(1.24, ((Point) result3).getY(), DELTA);
  }

  @Test
  public void testVisibleChange() {
    // A single node, visible changes
    OSMEntity entity = testData.nodes().get(3L).get(0);
    OSHDBTimestamp timestamp = entity.getTimestamp();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof Point);
    assertEquals(1.44, ((Point) result).getX(), DELTA);
    assertEquals(1.24, ((Point) result).getY(), DELTA);
    OSMEntity entity2 = testData.nodes().get(3L).get(1);
    OSHDBTimestamp timestamp2 = entity2.getTimestamp();
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
    assertTrue(result2 instanceof Point);
    assertTrue(result2.isEmpty());
    OSMEntity entity3 = testData.nodes().get(3L).get(2);
    OSHDBTimestamp timestamp3 = entity3.getTimestamp();
    Geometry result3 = OSHDBGeometryBuilder.getGeometry(entity3, timestamp3, areaDecider);
    assertTrue(result3 instanceof Point);
    assertEquals(1.44, ((Point) result3).getX(), DELTA);
    assertEquals(1.24, ((Point) result3).getY(), DELTA);
    OSMEntity entity4 = testData.nodes().get(3L).get(3);
    OSHDBTimestamp timestamp4 = entity4.getTimestamp();
    Geometry result4 = OSHDBGeometryBuilder.getGeometry(entity4, timestamp4, areaDecider);
    assertTrue(result4 instanceof Point);
    assertTrue(result4.isEmpty());
    OSMEntity entity5 = testData.nodes().get(3L).get(4);
    OSHDBTimestamp timestamp5 = entity5.getTimestamp();
    Geometry result5 = OSHDBGeometryBuilder.getGeometry(entity5, timestamp5, areaDecider);
    assertTrue(result5 instanceof Point);
    assertEquals(1.44, ((Point) result5).getX(), DELTA);
    assertEquals(1.24, ((Point) result5).getY(), DELTA);
  }

  @Test
  public void testMultipleChanges() {
    // A single node, various changes over time
    OSMEntity entity = testData.nodes().get(4L).get(0);
    OSHDBTimestamp timestamp = entity.getTimestamp();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof Point);
    assertEquals(1.42, ((Point) result).getX(), DELTA);
    assertEquals(1.21, ((Point) result).getY(), DELTA);
    OSMEntity entity2 = testData.nodes().get(4L).get(1);
    OSHDBTimestamp timestamp2 = entity2.getTimestamp();
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
    assertTrue(result2 instanceof Point);
    assertEquals(1.425, ((Point) result2).getX(), DELTA);
    assertEquals(1.20, ((Point) result2).getY(), DELTA);
    OSMEntity entity3 = testData.nodes().get(4L).get(2);
    OSHDBTimestamp timestamp3 = entity3.getTimestamp();
    Geometry result3 = OSHDBGeometryBuilder.getGeometry(entity3, timestamp3, areaDecider);
    assertTrue(result3 instanceof Point);
    assertTrue(result3.isEmpty());
    OSMEntity entity4 = testData.nodes().get(4L).get(3);
    OSHDBTimestamp timestamp4 = entity4.getTimestamp();
    Geometry result4 = OSHDBGeometryBuilder.getGeometry(entity4, timestamp4, areaDecider);
    assertTrue(result4 instanceof Point);
    assertEquals(1.42, ((Point) result4).getX(), DELTA);
    assertEquals(1.21, ((Point) result4).getY(), DELTA);
    OSMEntity entity5 = testData.nodes().get(4L).get(4);
    OSHDBTimestamp timestamp5 = entity5.getTimestamp();
    Geometry result5 = OSHDBGeometryBuilder.getGeometry(entity5, timestamp5, areaDecider);
    assertTrue(result5 instanceof Point);
    assertEquals(1.42, ((Point) result5).getX(), DELTA);
    assertEquals(1.215, ((Point) result5).getY(), DELTA);
    OSMEntity entity6 = testData.nodes().get(4L).get(5);
    OSHDBTimestamp timestamp6 = entity6.getTimestamp();
    Geometry result6 = OSHDBGeometryBuilder.getGeometry(entity6, timestamp6, areaDecider);
    assertTrue(result6 instanceof Point);
    assertEquals(1.42, ((Point) result6).getX(), DELTA);
    assertEquals(1.215, ((Point) result6).getY(), DELTA);
  }
}
