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

public class OSHDBGeometryBuilderTestOsmTestData3xx {
  private final OSMXmlReader testData = new OSMXmlReader();
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");
  private final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestOsmTestData3xx() {
    testData.add("./src/test/resources/osm-testdata/all.osm");
  }

  @Test
  public void test300() {
    // Normal node with uid (and user name)
    OSMEntity entity = testData.nodes().get(200000L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, null);
    assertTrue(result instanceof Point);
    Integer entity_uid = testData.nodes().get(200000L).get(0).getUserId();
    assertTrue(entity_uid instanceof Integer);
  }

}
