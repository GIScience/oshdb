package org.heigit.bigspatialdata.oshdb.util.geometry.relations;

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
import org.locationtech.jts.geom.Polygon;

public class OSHDBGeometryBuilderMultipolygonInvalidInnersTest {
  private final OSMXmlReader testData = new OSMXmlReader();
  private final TagInterpreter tagInterpreter;
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");

  public OSHDBGeometryBuilderMultipolygonInvalidInnersTest() {
    testData.add("./src/test/resources/relations/invalid-inner-rings.osm");
    tagInterpreter = new OSMXmlReaderTagInterpreter(testData);
  }

  @Test
  public void test() {
    // data has invalid (duplicate) inner rings
    OSMEntity entity = testData.relations().get(1L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
  }
}
