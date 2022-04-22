package org.heigit.ohsome.oshdb.util.geometry.relations;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.ohsome.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.ohsome.oshdb.util.xmlreader.OSMXmlReader;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

/**
 * Tests the {@link OSHDBGeometryBuilder} class for the special case of multipolygons with
 * invalid inner rings.
 */
class OSHDBGeometryBuilderMultipolygonInvalidInnersTest {
  private final OSMXmlReader testData = new OSMXmlReader();
  private final TagInterpreter tagInterpreter;
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");

  OSHDBGeometryBuilderMultipolygonInvalidInnersTest() {
    testData.add("./src/test/resources/relations/invalid-inner-rings.osm");
    tagInterpreter = new OSMXmlReaderTagInterpreter(testData);
  }

  @Test
  void testDuplicateInnerRings() {
    // data has invalid (duplicate) inner rings
    OSMEntity entity = testData.relations().get(1L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
  }

  @Test
  void testTouchingIncompleteInnerRings() {
    // data has invalid (duplicate) inner rings
    OSMEntity entity = testData.relations().get(2L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
  }
}
