package org.heigit.bigspatialdata.oshdb.util.geometry.osmtestdata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

public class OSHDBGeometryBuilderTestOsmTestData3xxTest {
  private final OSMXmlReader testData = new OSMXmlReader();
  TagInterpreter areaDecider;
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");

  public OSHDBGeometryBuilderTestOsmTestData3xxTest() {
    testData.add("./src/test/resources/osm-testdata/all.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(testData);
  }

  private Geometry buildEntityGeometry(long id) {
    OSMEntity entity = testData.nodes().get(id).get(0);
    return OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
  }

  @Test
  public void test300() {
    // Normal node with uid (and user name)
    Geometry result = buildEntityGeometry(200000L);
    assertTrue(result instanceof Point);
    int entityUid = testData.nodes().get(200000L).get(0).getUserId();
    assertEquals(1, entityUid);
  }

  @Test(expected = Test.None.class /* no exception expected */)
  public void test301() {
    // Empty username on node should not happen
    buildEntityGeometry(201000L);
  }

  @Test
  public void test302() {
    // No uid and no user name means user is anonymous
    // user name is not priority
    int entityUid = testData.nodes().get(202000L).get(0).getUserId();
    assertTrue(entityUid < 1);
  }

  @Test
  public void test303() {
    // uid 0 is the anonymous user
    int entityUid = testData.nodes().get(203000L).get(0).getUserId();
    assertEquals(0, entityUid);
  }

  @Test(expected = Test.None.class /* no exception expected */)
  public void test304() {
    // negative user ids are not allowed (but -1 could have been meant as anonymous user)
    buildEntityGeometry(204000L);
  }

  @Test(expected = Test.None.class /* no exception expected */)
  public void test305() {
    // uid < 0 and username is inconsistent and definitely wrong
    buildEntityGeometry(205000L);
  }

  @Test(expected = Test.None.class /* no exception expected */)
  public void test306() {
    // 250 characters in username is okay
    // user name is not priority
    buildEntityGeometry(206000L);
  }

  @Test(expected = Test.None.class /* no exception expected */)
  public void test307() {
    // 260 characters in username is too long
    buildEntityGeometry(207000L);
  }
}
