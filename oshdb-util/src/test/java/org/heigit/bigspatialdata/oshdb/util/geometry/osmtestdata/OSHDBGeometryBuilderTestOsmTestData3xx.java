package org.heigit.bigspatialdata.oshdb.util.geometry.osmtestdata;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.test.OSMXmlReader;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertTrue;

public class OSHDBGeometryBuilderTestOsmTestData3xx {
  private final OSMXmlReader testData = new OSMXmlReader();
  TagInterpreter areaDecider;
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");
  private final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestOsmTestData3xx() {
    testData.add("./src/test/resources/osm-testdata/all.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(testData);
  }

  @Test
  public void test300() {
    // Normal node with uid (and user name)
    OSMEntity entity = testData.nodes().get(200000L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof Point);
    Integer entity_uid = testData.nodes().get(200000L).get(0).getUserId();
    assertTrue(entity_uid instanceof Integer);
  }
  @Test
  public void test301() {
    // Empty username on node should not happen
    OSMEntity entity1 = testData.nodes().get(201000L).get(0);
    try {
      OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }
  @Test
  public void test302() {
    // No uid and no user name means user is anonymous
    // user name is not priority
  }
  @Test
  public void test303() {
    // No uid and no user name means user is anonymous
    // user name is not priority
  }
  @Test
  public void test304() {
    // negative user ids are not allowed (but -1 could have been meant as anonymous user)
    OSMEntity entity1 = testData.nodes().get(204000L).get(0);
    try {
      OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }
  @Test
  public void test305() {
    // uid < 0 and username is inconsistent and definitely wrong
    OSMEntity entity1 = testData.nodes().get(205000L).get(0);
    try {
      OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }
  @Test
  public void test306() {
    // 250 characters in username is okay
    // user name is not priority
  }
  @Test
  public void test307() {
    // 260 characters in username is too long
    OSMEntity entity1 = testData.nodes().get(207000L).get(0);
    try {
      OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }
}
