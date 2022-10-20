package org.heigit.ohsome.oshdb.util.geometry.relations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryTest;
import org.heigit.ohsome.oshdb.util.geometry.helpers.TimestampParser;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;

/**
 * Tests the {@link OSHDBGeometryBuilder} class for the special case of relations which are not
 * multipolygons (e.g. geometry collections).
 */
class OSHDBGeometryBuilderRelationTypeNotMultipolygonTest extends OSHDBGeometryTest {
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");

  OSHDBGeometryBuilderRelationTypeNotMultipolygonTest() {
    super("./src/test/resources/relations/relationTypeNotMultipolygon.osm");
  }

  @Test
  void testTypeRestriction() {
    // relation type restriction
    Geometry result = buildGeometry(relations(710900L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection);
    assertEquals(3, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
    assertTrue(result.getGeometryN(1) instanceof Point);
    assertTrue(result.getGeometryN(2) instanceof LineString);
  }

  @Test
  void testTypeAssociatedStreet() {
    // relation type associatedStreet
    Geometry result = buildGeometry(relations(710901L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection);
    assertEquals(3, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof Point);
    assertTrue(result.getGeometryN(1) instanceof Point);
    assertTrue(result.getGeometryN(2) instanceof Point);
  }

  @Test
  void testTypePublicTransport() {
    // relation type public_transport
    Geometry result = buildGeometry(relations(710902L, 0), timestamp);
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
    Geometry result = buildGeometry(relations(710903L, 0), timestamp);
    assertTrue(result instanceof GeometryCollection);
    assertEquals(3, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof LineString);
    assertTrue(result.getGeometryN(1) instanceof LineString);
    assertTrue(result.getGeometryN(2) instanceof LineString);
  }
}

