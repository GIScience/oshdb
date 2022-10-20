package org.heigit.ohsome.oshdb.util.geometry.relations;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryTest;
import org.heigit.ohsome.oshdb.util.geometry.helpers.TimestampParser;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

/**
 * Tests the {@link OSHDBGeometryBuilder} class for the special case of multipolygons with
 * invalid inner rings.
 */
class OSHDBGeometryBuilderMultipolygonInvalidInnersTest extends OSHDBGeometryTest {
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");

  OSHDBGeometryBuilderMultipolygonInvalidInnersTest() {
    super("./src/test/resources/relations/invalid-inner-rings.osm");
  }

  @Test
  void testDuplicateInnerRings() {
    // data has invalid (duplicate) inner rings
    Geometry result = buildGeometry(relations(1L, 0), timestamp);
    assertTrue(result instanceof Polygon);
  }

  @Test
  void testTouchingIncompleteInnerRings() {
    // data has invalid (duplicate) inner rings
    Geometry result = buildGeometry(relations(2L, 0), timestamp);
    assertTrue(result instanceof Polygon);
  }
}
