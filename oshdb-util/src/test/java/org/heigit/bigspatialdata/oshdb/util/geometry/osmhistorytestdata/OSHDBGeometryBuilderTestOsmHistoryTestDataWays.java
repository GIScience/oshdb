package org.heigit.bigspatialdata.oshdb.util.geometry.osmhistorytestdata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.test.OSMXmlReader;
import org.junit.Test;

public class OSHDBGeometryBuilderTestOsmHistoryTestDataWays {
  private final OSMXmlReader testData = new OSMXmlReader();
  TagInterpreter areaDecider;
  private final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestOsmHistoryTestDataWays() {
    testData.add("./src/test/resources/different-timestamps/way.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(testData);
  }

  @Test
  public void test1() throws ParseException {
    // Way getting more nodes, one disappears
    // first appearance
    OSMEntity entity1 = testData.ways().get(100L).get(0);
    OSHDBTimestamp timestamp = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    assertEquals(4,result1.getNumPoints());
    // compare if coordinates of created points equals the coordinates of way
    Geometry expectedWay = (new WKTReader()).read(
        "LINESTRING(1.42 1.22, 1.42 1.23, 1.42 1.24, 1.42 1.25)"
    );
    Geometry intersection = result1.intersection(expectedWay);
    assertEquals(expectedWay.getArea(), intersection.getArea(), DELTA);
    // second appearance
    OSMEntity entity2 = testData.ways().get(100L).get(1);
    OSHDBTimestamp timestamp2 = entity2.getTimestamp();
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
    assertTrue(result2 instanceof LineString);
    assertEquals(8,result2.getNumPoints());
    // compare if coordinates of created points equals the coordinates of way
    Geometry expectedWay2 = (new WKTReader()).read(
        "LINESTRING(1.42 1.22, 1.42 1.23, 1.42 1.24, 1.42 1.25, 1.42 1.26, 1.42 1.27, 1.42 1.28, 1.43 1.29)"
    );
    Geometry intersection2 = result2.intersection(expectedWay2);
    assertEquals(expectedWay2.getArea(), intersection2.getArea(), DELTA);
    // last appearance
    OSMEntity entity3 = testData.ways().get(100L).get(2);
    OSHDBTimestamp timestamp3 = entity3.getTimestamp();
    Geometry result3 = OSHDBGeometryBuilder.getGeometry(entity3, timestamp3, areaDecider);
    assertTrue(result3 instanceof LineString);
    assertEquals(9,result3.getNumPoints());
    // timestamp after last one
    OSMEntity entity_after = testData.ways().get(100L).get(2);
    OSHDBTimestamp timestamp_after =  TimestampParser.toOSHDBTimestamp("2012-01-01T00:00:00Z");
    Geometry result_after = OSHDBGeometryBuilder.getGeometry(entity_after, timestamp_after, areaDecider);
    assertTrue(result_after instanceof LineString);
    assertEquals(9,result_after.getNumPoints());
    // compare if coordinates of created points equals the coordinates of way
    Geometry expectedWay_after = (new WKTReader()).read(
        "LINESTRING(1.42 1.22, 1.42 1.23, 1.42 1.24, 1.42 1.25, 1.42 1.26, 1.42 1.28, 1.43 1.29, 1.43 1.30, 1.43 1.31)"
    );
    Geometry intersection_after = result_after.intersection(expectedWay_after);
    assertEquals(expectedWay_after.getArea(), intersection_after.getArea(), DELTA);
  }

  @Test
  public void test2() throws ParseException {
    // Way with two then three nodes, changing lat lon
    // first appearance
    OSMEntity entity1 = testData.ways().get(101L).get(0);
    OSHDBTimestamp timestamp = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    assertEquals(2,result1.getNumPoints());
    // compare if coordinates of created points equals the coordinates of way
    Geometry expectedWay = (new WKTReader()).read(
        "LINESTRING(1.44 1.22, 1.42 1.22)"
    );
    Geometry intersection = result1.intersection(expectedWay);
    assertEquals(expectedWay.getArea(), intersection.getArea(), DELTA);
    // last appearance
    OSMEntity entity2 = testData.ways().get(101L).get(1);
    OSHDBTimestamp timestamp2 = entity2.getTimestamp();
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
    assertTrue(result2 instanceof LineString);
    assertEquals(3,result2.getNumPoints());
    // compare if coordinates of created points equals the coordinates of way
    Geometry expectedWay2 = (new WKTReader()).read(
        "LINESTRING(1.425 1.23, 1.44 1.23, 1.43 1.30)"
    );
    Geometry intersection2 = result2.intersection(expectedWay2);
    assertEquals(expectedWay2.getArea(), intersection2.getArea(), DELTA);
    // timestamp in between
    OSHDBTimestamp timestamp_after =  TimestampParser.toOSHDBTimestamp("2009-02-01T00:00:00Z");
    OSMEntity entity_between = testData.ways().get(101L).get(1);
    OSHDBTimestamp timestamp_between = entity_between.getTimestamp();
    Geometry result_between = OSHDBGeometryBuilder.getGeometry(entity_between, timestamp_between, areaDecider);
    assertTrue(result_between instanceof LineString);
    assertEquals(3,result_between.getNumPoints());
    // compare if coordinates of created points equals the coordinates of way
    Geometry expectedWay_between = (new WKTReader()).read(
        "LINESTRING(1.42 1.225, 1.445 1.225)"
    );
    Geometry intersection_between = result_between.intersection(expectedWay2);
    assertEquals(expectedWay_between.getArea(), intersection_between.getArea(), DELTA);

  }

}
