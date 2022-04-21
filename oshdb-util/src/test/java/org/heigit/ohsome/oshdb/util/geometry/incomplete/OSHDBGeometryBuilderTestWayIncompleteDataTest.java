package org.heigit.ohsome.oshdb.util.geometry.incomplete;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryTest;
import org.heigit.ohsome.oshdb.util.geometry.helpers.TimestampParser;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;

/**
 * Tests the {@link OSHDBGeometryBuilder} class on incomplete ways.
 */
class OSHDBGeometryBuilderTestWayIncompleteDataTest extends OSHDBGeometryTest {
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");

  public OSHDBGeometryBuilderTestWayIncompleteDataTest() {
    super("./src/test/resources/incomplete-osm/way.osm");
  }

  @Test
  void testOneOfNodesNotExistent() {
    // Way with four node references, one node missing
    Geometry result = buildGeometry(ways(100L, 0), timestamp);
    assertTrue(result instanceof LineString);
    assertTrue(result.isValid());
    assertTrue(result.getCoordinates().length >= 3);
  }

  @Test
  void testWayAreaYes() {
    // Way with four nodes, area = yes
    Geometry result = buildGeometry(ways(101L, 0), timestamp);
    assertTrue(result instanceof LineString);
    assertTrue(result.isValid());
    assertTrue(result.getCoordinates().length >= 3);
  }

  @Test
  void testAllNodesNotExistent() {
    // Way with two nodes, both missing
    Geometry result = buildGeometry(ways(102L, 0), timestamp);
    assertEquals(0, result.getCoordinates().length);
    assertTrue(result.isValid());
  }
}
