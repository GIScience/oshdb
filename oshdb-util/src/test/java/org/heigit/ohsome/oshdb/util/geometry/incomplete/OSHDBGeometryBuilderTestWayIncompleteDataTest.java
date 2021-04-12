package org.heigit.ohsome.oshdb.util.geometry.incomplete;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.ohsome.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.ohsome.oshdb.util.xmlreader.OSMXmlReader;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;

public class OSHDBGeometryBuilderTestWayIncompleteDataTest {
  private final OSMXmlReader testData = new OSMXmlReader();
  TagInterpreter areaDecider;
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");

  public OSHDBGeometryBuilderTestWayIncompleteDataTest() {
    testData.add("./src/test/resources/incomplete-osm/way.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(testData);
  }


  @Test
  public void testOneOfNodesNotExistent() {
    // Way with four node references, one node missing
    OSMEntity entity1 = testData.ways().get(100L).get(0);
    Geometry result1 = null;
    try {
      result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    assertTrue(result1 instanceof LineString);
    assertTrue(result1.isValid());
    assertTrue(result1.getCoordinates().length >= 3);
  }

  @Test
  public void testWayAreaYes() {
    // Way with four nodes, area = yes
    OSMEntity entity1 = testData.ways().get(101L).get(0);
    Geometry result1 = null;
    try {
      result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    assertTrue(result1 instanceof LineString);
    assertTrue(result1.isValid());
    assertTrue(result1.getCoordinates().length >= 3);
  }

  @Test
  public void testAllNodesNotExistent() {
    // Way with two nodes, both missing
    OSMEntity entity1 = testData.ways().get(102L).get(0);
    Geometry result1 = null;
    try {
      result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
      assertEquals(0, result1.getCoordinates().length);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    assertTrue(result1.isValid());
  }


}
