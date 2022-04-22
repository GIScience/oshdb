package org.heigit.ohsome.oshdb.util.geometry.osmhistorytestdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryTest;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

/**
 * Tests the {@link OSHDBGeometryBuilder} class on OSM nodes.
 */
class OSHDBGeometryBuilderTestOsmHistoryTestDataNodesTest extends OSHDBGeometryTest {
  private static final double DELTA = 1E-6;

  OSHDBGeometryBuilderTestOsmHistoryTestDataNodesTest() {
    super("./src/test/resources/different-timestamps/node.osm");
  }

  @Test
  void testGeometryChange() {
    // A single node, lat lon changed over time

    // first appearance
    Geometry result = buildGeometry(nodes(1L, 0));
    assertTrue(result instanceof Point);
    assertEquals(1.42, ((Point) result).getX(), DELTA);
    assertEquals(1.22, ((Point) result).getY(), DELTA);

    // second
    result = buildGeometry(nodes(1L, 1));
    assertTrue(result instanceof Point);
    assertEquals(1.42, ((Point) result).getX(), DELTA);
    assertEquals(1.225, ((Point) result).getY(), DELTA);

    // last
    result = buildGeometry(nodes(1L, 2));
    assertTrue(result instanceof Point);
    assertEquals(1.425, ((Point) result).getX(), DELTA);
    assertEquals(1.23, ((Point) result).getY(), DELTA);

    // timestamp after newest timestamp
    result = buildGeometry(nodes(1L, 2), "2012-01-01T00:00:00Z");
    assertTrue(result instanceof Point);
    assertEquals(1.425, ((Point) result).getX(), DELTA);
    assertEquals(1.23, ((Point) result).getY(), DELTA);
  }

  @Test()
  void testInvalidAccess() {
    // A single node, lat lon changed over time
    var testNode = nodes(1L, 0);
    // timestamp before oldest timestamp
    assertThrows(AssertionError.class, () -> {
      buildGeometry(testNode, "2007-01-01T00:00:00Z");
    });
  }

  @Test
  void testTagChange() {
    // A single node, tags changed over time
    Geometry result = buildGeometry(nodes(2L, 0));
    assertTrue(result instanceof Point);
    assertEquals(1.43, ((Point) result).getX(), DELTA);
    assertEquals(1.24, ((Point) result).getY(), DELTA);

    result = buildGeometry(nodes(2L, 1));
    assertTrue(result instanceof Point);
    assertEquals(1.43, ((Point) result).getX(), DELTA);
    assertEquals(1.24, ((Point) result).getY(), DELTA);

    result = buildGeometry(nodes(2L, 2));
    assertTrue(result instanceof Point);
    assertEquals(1.43, ((Point) result).getX(), DELTA);
    assertEquals(1.24, ((Point) result).getY(), DELTA);
  }

  @Test
  void testVisibleChange() {
    // A single node, visible changes
    Geometry result = buildGeometry(nodes(3L, 0));
    assertTrue(result instanceof Point);
    assertEquals(1.44, ((Point) result).getX(), DELTA);
    assertEquals(1.24, ((Point) result).getY(), DELTA);

    result = buildGeometry(nodes(3L, 1));
    assertTrue(result instanceof Point);
    assertTrue(result.isEmpty());

    result = buildGeometry(nodes(3L, 2));
    assertTrue(result instanceof Point);
    assertEquals(1.44, ((Point) result).getX(), DELTA);
    assertEquals(1.24, ((Point) result).getY(), DELTA);

    result = buildGeometry(nodes(3L, 3));
    assertTrue(result instanceof Point);
    assertTrue(result.isEmpty());

    result = buildGeometry(nodes(3L, 4));
    assertTrue(result instanceof Point);
    assertEquals(1.44, ((Point) result).getX(), DELTA);
    assertEquals(1.24, ((Point) result).getY(), DELTA);
  }

  @Test
  void testMultipleChanges() {
    // A single node, various changes over time
    Geometry result = buildGeometry(nodes(4L, 0));
    assertTrue(result instanceof Point);
    assertEquals(1.42, ((Point) result).getX(), DELTA);
    assertEquals(1.21, ((Point) result).getY(), DELTA);

    result = buildGeometry(nodes(4L, 1));
    assertTrue(result instanceof Point);
    assertEquals(1.425, ((Point) result).getX(), DELTA);
    assertEquals(1.20, ((Point) result).getY(), DELTA);

    result = buildGeometry(nodes(4L, 2));
    assertTrue(result instanceof Point);
    assertTrue(result.isEmpty());

    result = buildGeometry(nodes(4L, 3));
    assertTrue(result instanceof Point);
    assertEquals(1.42, ((Point) result).getX(), DELTA);
    assertEquals(1.21, ((Point) result).getY(), DELTA);

    result = buildGeometry(nodes(4L, 4));
    assertTrue(result instanceof Point);
    assertEquals(1.42, ((Point) result).getX(), DELTA);
    assertEquals(1.215, ((Point) result).getY(), DELTA);

    result = buildGeometry(nodes(4L, 5));
    assertTrue(result instanceof Point);
    assertEquals(1.42, ((Point) result).getX(), DELTA);
    assertEquals(1.215, ((Point) result).getY(), DELTA);
  }
}
