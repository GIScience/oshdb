package org.heigit.ohsome.oshdb.util.geometry.relations;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;

/**
 * Tests the {@link OSHDBGeometryBuilder} class for the special case of relations which are not
 * multipolygons (e.g. geometry collections).
 */
class OSHDBGeometryBuilderRelationTypeNotMultipolygonTest {
  private final OSMXmlReader testData = new OSMXmlReader();
  private final TagInterpreter tagInterpreter;
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");

  OSHDBGeometryBuilderRelationTypeNotMultipolygonTest() {
    testData.add("./src/test/resources/relations/relationTypeNotMultipolygon.osm");
    tagInterpreter = new OSMXmlReaderTagInterpreter(testData);
  }

  @Test
  void testTypeRestriction() {
    // relation type restriction
    OSMEntity entity1 = testData.relations().get(710900L).get(0);
    Geometry result = assertDoesNotThrow(() -> {
      return OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
    });
    assertTrue(result instanceof GeometryCollection);
    assertEquals(3, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
    assertTrue(result.getGeometryN(1) instanceof Point);
    assertTrue(result.getGeometryN(2) instanceof LineString);
  }

  @Test
  void testTypeAssociatedStreet() {
    // relation type associatedStreet
    OSMEntity entity1 = testData.relations().get(710901L).get(0);
    Geometry result = assertDoesNotThrow(() -> {
      return OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
    });
    assertTrue(result instanceof GeometryCollection);
    assertEquals(3, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof Point);
    assertTrue(result.getGeometryN(1) instanceof Point);
    assertTrue(result.getGeometryN(2) instanceof Point);
  }

  @Test
  void testTypePublicTransport() {
    // relation type public_transport
    OSMEntity entity1 = testData.relations().get(710902L).get(0);
    Geometry result = assertDoesNotThrow(() -> {
      return OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
    });
    assertTrue(result instanceof GeometryCollection);
    assertEquals(4, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
    assertTrue(result.getGeometryN(1) instanceof Point);
    assertTrue(result.getGeometryN(2) instanceof LineString);
    assertTrue(result.getGeometryN(3) instanceof Point);
  }

  @Test
  void testTypeBuilding() {
    // relation type building
    OSMEntity entity1 = testData.relations().get(710903L).get(0);
    Geometry result = assertDoesNotThrow(() -> {
      return OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
    });
    assertTrue(result instanceof GeometryCollection);
    assertEquals(3, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
    assertTrue(result.getGeometryN(1) instanceof LineString);
    assertTrue(result.getGeometryN(2) instanceof LineString);
  }

}

