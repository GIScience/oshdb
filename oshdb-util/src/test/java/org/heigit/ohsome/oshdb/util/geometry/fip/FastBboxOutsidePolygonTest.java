package org.heigit.ohsome.oshdb.util.geometry.fip;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.junit.Test;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;

/**
 * Tests the {@link FastBboxOutsidePolygon} class.
 */
public class FastBboxOutsidePolygonTest {
  @Test
  public void testBboxInPolygon() {
    Polygon p = FastPointInPolygonTest.createPolygon();
    FastBboxOutsidePolygon bop = new FastBboxOutsidePolygon(p);

    // inside
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(-0.6, -0.1, -0.4, 0.1)));
    // partially inside
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(-1.5, -0.1, -0.4, 0.1)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(-0.6, -0.1, 1.4, 0.1)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(-0.6, -1.1, -0.4, 0.1)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(-0.6, -0.1, -0.4, 1.1)));
    // in concave part
    assertTrue(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(0.4, -0.1, 0.6, 0.1)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(0.4, -0.9, 0.6, -0.8)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(0.4, 0.8, 0.6, 0.9)));
    // in concave part, coordinates all inside
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(0.4, -0.9, 0.6, 0.9)));
    // outside poly's bbox
    assertTrue(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(1.4, -0.1, 1.6, 0.1)));
    // bbox covering
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(-11.0, -10.0, 10.0, 10.0)));
  }

  @Test
  @SuppressWarnings("java:S5961" /* has to test all cases how bbox and polygon can be aligned */)
  public void testBboxInPolygonWithHole() {
    Polygon p = FastPointInPolygonTest.createPolygonWithHole();
    FastBboxOutsidePolygon bop = new FastBboxOutsidePolygon(p);

    // inside
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(2.1, -0.1, 2.2, 0.1)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.1, -0.9, 3.2, -0.8)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.1, 0.8, 3.2, 0.9)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.8, -0.1, 3.9, 0.1)));
    // partially inside
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(1.8, -0.1, 2.2, 0.1)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.1, -1.1, 3.2, -0.8)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.1, 0.8, 3.2, 1.1)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.8, -0.1, 4.1, 0.1)));
    // in hole
    assertTrue(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(2.9, -0.1, 3.1, 0.1)));
    // partially in hole
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(2.4, -0.1, 2.6, 0.1)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.1, -0.6, 3.2, -0.4)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.1, 0.4, 3.2, 0.6)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.4, -0.1, 3.6, 0.1)));
    // intersecting hole
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(2.1, -0.1, 3.9, 0.1)));
    // outside poly's bbox
    assertTrue(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(4.1, -0.1, 4.2, 0.1)));
    assertTrue(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(1.8, -0.1, 1.9, 0.1)));
    assertTrue(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.1, -1.2, 3.2, -1.1)));
    assertTrue(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.1, 1.1, 3.2, 1.2)));
    // covering hole, but all vertices inside polygon
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(2.2, -0.8, 3.8, 0.8)));
  }

  @Test
  @SuppressWarnings("java:S5961" /* has to test all cases how bbox and polygon can be aligned */)
  public void testBboxInMultiPolygon() {
    MultiPolygon p = FastPointInPolygonTest.createMultiPolygon();
    FastBboxOutsidePolygon bop = new FastBboxOutsidePolygon(p);

    // left polygon
    // inside
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(-0.6, -0.1, -0.4, 0.1)));
    // partially inside
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(-1.5, -0.1, -0.4, 0.1)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(-0.6, -0.1, 1.4, 0.1)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(-0.6, -1.1, -0.4, 0.1)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(-0.6, -0.1, -0.4, 1.1)));
    // in concave part
    assertTrue(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(0.4, -0.1, 0.6, 0.1)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(0.4, -0.9, 0.6, -0.8)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(0.4, 0.8, 0.6, 0.9)));
    // in concave part, coordinates all inside
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(0.4, -0.9, 0.6, 0.9)));
    // outside poly's bbox
    assertTrue(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(1.4, -0.1, 1.6, 0.1)));
    // bbox covering
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(-11.0, -10.0, 10.0, 10.0)));

    // right polygon
    // inside
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(2.1, -0.1, 2.2, 0.1)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.1, -0.9, 3.2, -0.8)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.1, 0.8, 3.2, 0.9)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.8, -0.1, 3.9, 0.1)));
    // partially inside
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(1.8, -0.1, 2.2, 0.1)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.1, -1.1, 3.2, -0.8)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.1, 0.8, 3.2, 1.1)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.8, -0.1, 4.1, 0.1)));
    // in hole
    assertTrue(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(2.9, -0.1, 3.1, 0.1)));
    // partially in hole
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(2.4, -0.1, 2.6, 0.1)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.1, -0.6, 3.2, -0.4)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.1, 0.4, 3.2, 0.6)));
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.4, -0.1, 3.6, 0.1)));
    // intersecting hole
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(2.1, -0.1, 3.9, 0.1)));
    // outside poly's bbox
    assertTrue(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(4.1, -0.1, 4.2, 0.1)));
    assertTrue(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(1.8, -0.1, 1.9, 0.1)));
    assertTrue(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.1, -1.2, 3.2, -1.1)));
    assertTrue(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(3.1, 1.1, 3.2, 1.2)));
    // covering hole, but all vertices inside polygon
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(2.2, -0.8, 3.8, 0.8)));
  }

  @Test
  public void testBboxInSquareSquareMultiPolygon() {
    MultiPolygon p = FastBboxInPolygonTest.createSquareSquareMultiPolygon();
    FastBboxOutsidePolygon bop = new FastBboxOutsidePolygon(p);

    // not inside
    assertFalse(bop.test(OSHDBBoundingBox.bboxWgs84Coordinates(-1.0, -1.0, 1.0, 1.0)));
  }
}
