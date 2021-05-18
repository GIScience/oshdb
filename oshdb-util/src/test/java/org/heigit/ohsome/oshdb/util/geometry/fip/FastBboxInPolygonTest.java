package org.heigit.ohsome.oshdb.util.geometry.fip;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxLonLatCoordinates;
import static org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder.getGeometry;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;

/**
 * Tests the {@link FastBboxInPolygon} class.
 */
public class FastBboxInPolygonTest {
  /**
   * Returns a {@link MultiPolygon} of four small squares arranged in a square.
   */
  public static MultiPolygon createSquareSquareMultiPolygon() {
    GeometryFactory gf = new GeometryFactory();
    Polygon poly1 = getGeometry(bboxLonLatCoordinates(-1.5, -1.5, -0.5, -0.5));
    Polygon poly2 = getGeometry(bboxLonLatCoordinates(0.5, -1.5, 1.5, -0.5));
    Polygon poly3 = getGeometry(bboxLonLatCoordinates(-1.5, 0.5, -0.5, 1.5));
    Polygon poly4 = getGeometry(bboxLonLatCoordinates(0.5, 0.5, 1.5, 1.5));
    return new MultiPolygon(new Polygon[] { poly1, poly2, poly3, poly4 }, gf);
  }

  @Test
  public void testBboxInPolygon() {
    Polygon p = FastPointInPolygonTest.createPolygon();
    FastBboxInPolygon bip = new FastBboxInPolygon(p);

    // inside
    assertTrue(bip.test(bboxLonLatCoordinates(-0.6, -0.1, -0.4, 0.1)));
    // partially inside
    assertFalse(bip.test(bboxLonLatCoordinates(-1.5, -0.1, -0.4, 0.1)));
    assertFalse(bip.test(bboxLonLatCoordinates(-0.6, -0.1, 1.4, 0.1)));
    assertFalse(bip.test(bboxLonLatCoordinates(-0.6, -1.1, -0.4, 0.1)));
    assertFalse(bip.test(bboxLonLatCoordinates(-0.6, -0.1, -0.4, 1.1)));
    // in concave part
    assertFalse(bip.test(bboxLonLatCoordinates(0.4, -0.1, 0.6, 0.1)));
    assertTrue(bip.test(bboxLonLatCoordinates(0.4, -0.9, 0.6, -0.8)));
    assertTrue(bip.test(bboxLonLatCoordinates(0.4, 0.8, 0.6, 0.9)));
    // in concave part, coordinates all inside
    assertFalse(bip.test(bboxLonLatCoordinates(0.4, -0.9, 0.6, 0.9)));
    // outside poly's bbox
    assertFalse(bip.test(bboxLonLatCoordinates(1.4, -0.1, 1.6, 0.1)));
    // bbox covering
    assertFalse(bip.test(bboxLonLatCoordinates(-11.0, -10.0, 10.0, 10.0)));
  }

  @Test
  @SuppressWarnings("java:S5961" /* has to test all cases how bbox and polygon can be aligned */)
  public void testBboxInPolygonWithHole() {
    Polygon p = FastPointInPolygonTest.createPolygonWithHole();
    FastBboxInPolygon bip = new FastBboxInPolygon(p);

    // inside
    assertTrue(bip.test(bboxLonLatCoordinates(2.1, -0.1, 2.2, 0.1)));
    assertTrue(bip.test(bboxLonLatCoordinates(3.1, -0.9, 3.2, -0.8)));
    assertTrue(bip.test(bboxLonLatCoordinates(3.1, 0.8, 3.2, 0.9)));
    assertTrue(bip.test(bboxLonLatCoordinates(3.8, -0.1, 3.9, .1)));
    // partially inside
    assertFalse(bip.test(bboxLonLatCoordinates(1.8, -0.1, 2.2, 0.1)));
    assertFalse(bip.test(bboxLonLatCoordinates(3.1, -1.1, 3.2, -0.8)));
    assertFalse(bip.test(bboxLonLatCoordinates(3.1, 0.8, 3.2, 1.1)));
    assertFalse(bip.test(bboxLonLatCoordinates(3.8, -0.1, 4.1, 0.1)));
    // in hole
    assertFalse(bip.test(bboxLonLatCoordinates(2.9, -0.1, 3.1, 0.1)));
    // partially in hole
    assertFalse(bip.test(bboxLonLatCoordinates(2.4, -0.1, 2.6, 0.1)));
    assertFalse(bip.test(bboxLonLatCoordinates(3.1, -0.6, 3.2, -0.4)));
    assertFalse(bip.test(bboxLonLatCoordinates(3.1, 0.4, 3.2, 0.6)));
    assertFalse(bip.test(bboxLonLatCoordinates(3.4, -0.1, 3.6, 0.1)));
    // intersecting hole
    assertFalse(bip.test(bboxLonLatCoordinates(2.1, -0.1, 3.9, 0.1)));
    // outside poly's bbox
    assertFalse(bip.test(bboxLonLatCoordinates(4.1, -0.1, 4.2, 0.1)));
    assertFalse(bip.test(bboxLonLatCoordinates(1.8, -0.1, 1.9, 0.1)));
    assertFalse(bip.test(bboxLonLatCoordinates(3.1, -1.2, 3.2, -1.1)));
    assertFalse(bip.test(bboxLonLatCoordinates(3.1, 1.1, 3.2, 1.2)));
    // covering hole, but all vertices inside polygon
    assertFalse(bip.test(bboxLonLatCoordinates(2.2, -0.8, 3.8, 0.8)));
  }

  @Test
  @SuppressWarnings("java:S5961" /* has to test all cases how bbox and polygon can be aligned */)
  public void testBboxInMultiPolygon() {
    MultiPolygon p = FastPointInPolygonTest.createMultiPolygon();
    FastBboxInPolygon bip = new FastBboxInPolygon(p);

    // left polygon
    // inside
    assertTrue(bip.test(bboxLonLatCoordinates(-0.6, -0.1, -0.4, 0.1)));
    // partially inside
    assertFalse(bip.test(bboxLonLatCoordinates(-1.5, -0.1, -0.4, 0.1)));
    assertFalse(bip.test(bboxLonLatCoordinates(-0.6, -0.1, 1.4, 0.1)));
    assertFalse(bip.test(bboxLonLatCoordinates(-0.6, -1.1, -0.4, 0.1)));
    assertFalse(bip.test(bboxLonLatCoordinates(-0.6, -0.1, -0.4, 1.1)));
    // in concave part
    assertFalse(bip.test(bboxLonLatCoordinates(0.4, -0.1, 0.6, 0.1)));
    assertTrue(bip.test(bboxLonLatCoordinates(0.4, -0.9, 0.6, -0.8)));
    assertTrue(bip.test(bboxLonLatCoordinates(0.4, 0.8, 0.6, 0.9)));
    // in concave part, coordinates all inside
    assertFalse(bip.test(bboxLonLatCoordinates(0.4, -0.9, 0.6, 0.9)));
    // outside poly's bbox
    assertFalse(bip.test(bboxLonLatCoordinates(1.4, -0.1, 1.6, 0.1)));
    // bbox covering
    assertFalse(bip.test(bboxLonLatCoordinates(-11.0, -10.0, 10.0, 10.0)));

    // right polygon
    // inside
    assertTrue(bip.test(bboxLonLatCoordinates(2.1, -0.1, 2.2, 0.1)));
    assertTrue(bip.test(bboxLonLatCoordinates(3.1, -0.9, 3.2, -0.8)));
    assertTrue(bip.test(bboxLonLatCoordinates(3.1, 0.8, 3.2, 0.9)));
    assertTrue(bip.test(bboxLonLatCoordinates(3.8, -0.1, 3.9, 0.1)));
    // partially inside
    assertFalse(bip.test(bboxLonLatCoordinates(1.8, -0.1, 2.2, 0.1)));
    assertFalse(bip.test(bboxLonLatCoordinates(3.1, -1.1, 3.2, -0.8)));
    assertFalse(bip.test(bboxLonLatCoordinates(3.1, 0.8, 3.2, 1.1)));
    assertFalse(bip.test(bboxLonLatCoordinates(3.8, -0.1, 4.1, 0.1)));
    // in hole
    assertFalse(bip.test(bboxLonLatCoordinates(2.9, -0.1, 3.1, 0.1)));
    // partially in hole
    assertFalse(bip.test(bboxLonLatCoordinates(2.4, -0.1, 2.6, 0.1)));
    assertFalse(bip.test(bboxLonLatCoordinates(3.1, -0.6, 3.2, -0.4)));
    assertFalse(bip.test(bboxLonLatCoordinates(3.1, 0.4, 3.2, 0.6)));
    assertFalse(bip.test(bboxLonLatCoordinates(3.4, -0.1, 3.6, 0.1)));
    // intersecting hole
    assertFalse(bip.test(bboxLonLatCoordinates(2.1, -0.1, 3.9, 0.1)));
    // outside poly's bbox
    assertFalse(bip.test(bboxLonLatCoordinates(4.1, -0.1, 4.2, 0.1)));
    assertFalse(bip.test(bboxLonLatCoordinates(1.8, -0.1, 1.9, 0.1)));
    assertFalse(bip.test(bboxLonLatCoordinates(3.1, -1.2, 3.2, -1.1)));
    assertFalse(bip.test(bboxLonLatCoordinates(3.1, 1.1, 3.2, 1.2)));
    // covering hole, but all vertices inside polygon
    assertFalse(bip.test(bboxLonLatCoordinates(2.2, -0.8, 3.8, 0.8)));
  }

  @Test
  public void testBboxInSquareSquareMultiPolygon() {
    MultiPolygon p = createSquareSquareMultiPolygon();
    FastBboxInPolygon bip = new FastBboxInPolygon(p);

    // not inside
    assertFalse(bip.test(bboxLonLatCoordinates(-1.0, -1.0, 1.0, 1.0)));
  }
}
