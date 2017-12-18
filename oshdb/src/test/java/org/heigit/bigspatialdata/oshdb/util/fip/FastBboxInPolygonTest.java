package org.heigit.bigspatialdata.oshdb.util.fip;

import com.vividsolutions.jts.geom.*;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class FastBboxInPolygonTest {
  /**
   * @return a multipolygon of four small squares arranged in a square
   */
  public static MultiPolygon createSquareSquareMultiPolygon() {
    GeometryFactory gf = new GeometryFactory();
    Polygon poly1 = new BoundingBox(-1.5,-0.5,-1.5,-0.5).getGeometry();
    Polygon poly2 = new BoundingBox(0.5,1.5,-1.5,-0.5).getGeometry();
    Polygon poly3 = new BoundingBox(-1.5,-0.5,0.5,1.5).getGeometry();
    Polygon poly4 = new BoundingBox(0.5,1.5,0.5,1.5).getGeometry();
    return new MultiPolygon(new Polygon[] { poly1, poly2, poly3, poly4 }, gf);
  }

  @Test
  public void testBboxInPolygon() {
    Polygon p = FastPointInPolygonTest.createPolygon();
    FastBboxInPolygon bip = new FastBboxInPolygon(p);

    // inside
    assertEquals(bip.test(new BoundingBox(-0.6,-0.4,-0.1,0.1)), true);
    // partially inside
    assertEquals(bip.test(new BoundingBox(-1.5,-0.4,-0.1,0.1)), false);
    assertEquals(bip.test(new BoundingBox(-0.6,1.4,-0.1,0.1)), false);
    assertEquals(bip.test(new BoundingBox(-0.6,-0.4,-1.1,0.1)), false);
    assertEquals(bip.test(new BoundingBox(-0.6,-0.4,-0.1,1.1)), false);
    // in concave part
    assertEquals(bip.test(new BoundingBox(0.4,0.6,-0.1,0.1)), false);
    assertEquals(bip.test(new BoundingBox(0.4,0.6,-0.9,-0.8)), true);
    assertEquals(bip.test(new BoundingBox(0.4,0.6,0.8,0.9)), true);
    // in concave part, coordinates all inside
    assertEquals(bip.test(new BoundingBox(0.4,0.6,-0.9,0.9)), false);
    // outside poly's bbox
    assertEquals(bip.test(new BoundingBox(1.4,1.6,-0.1,0.1)), false);
    // bbox covering
    assertEquals(bip.test(new BoundingBox(-11,10,-10,10)), false);
  }

  @Test
  public void testBboxInPolygonWithHole() {
    Polygon p = FastPointInPolygonTest.createPolygonWithHole();
    FastBboxInPolygon bip = new FastBboxInPolygon(p);

    // inside
    assertEquals(bip.test(new BoundingBox(2.1,2.2,-0.1,0.1)), true);
    assertEquals(bip.test(new BoundingBox(3.1,3.2,-0.9,-0.8)), true);
    assertEquals(bip.test(new BoundingBox(3.1,3.2,0.8,0.9)), true);
    assertEquals(bip.test(new BoundingBox(3.8,3.9,-0.1,0.1)), true);
    // partially inside
    assertEquals(bip.test(new BoundingBox(1.8,2.2,-0.1,0.1)), false);
    assertEquals(bip.test(new BoundingBox(3.1,3.2,-1.1,-0.8)), false);
    assertEquals(bip.test(new BoundingBox(3.1,3.2,0.8,1.1)), false);
    assertEquals(bip.test(new BoundingBox(3.8,4.1,-0.1,0.1)), false);
    // in hole
    assertEquals(bip.test(new BoundingBox(2.9,3.1,-0.1,0.1)), false);
    // partially in hole
    assertEquals(bip.test(new BoundingBox(2.4,2.6,-0.1,0.1)), false);
    assertEquals(bip.test(new BoundingBox(3.1,3.2,-0.6,-0.4)), false);
    assertEquals(bip.test(new BoundingBox(3.1,3.2,0.4,0.6)), false);
    assertEquals(bip.test(new BoundingBox(3.4,3.6,-0.1,0.1)), false);
    // intersecting hole
    assertEquals(bip.test(new BoundingBox(2.9,3.1,-0.1,0.1)), false);
    // outside poly's bbox
    assertEquals(bip.test(new BoundingBox(4.1,4.2,-0.1,0.1)), false);
    assertEquals(bip.test(new BoundingBox(1.8,1.9,-0.1,0.1)), false);
    assertEquals(bip.test(new BoundingBox(3.1,3.2,-1.2,-1.1)), false);
    assertEquals(bip.test(new BoundingBox(3.1,3.2,1.1,1.2)), false);
    // covering hole, but all vertices inside polygon
    assertEquals(bip.test(new BoundingBox(2.2,3.8,-0.8,0.8)), false);
  }

  @Test
  public void testBboxInMultiPolygon() {
    MultiPolygon p = FastPointInPolygonTest.createMultiPolygon();
    FastBboxInPolygon bip = new FastBboxInPolygon(p);

    // left polygon
    // inside
    assertEquals(bip.test(new BoundingBox(-0.6,-0.4,-0.1,0.1)), true);
    // partially inside
    assertEquals(bip.test(new BoundingBox(-1.5,-0.4,-0.1,0.1)), false);
    assertEquals(bip.test(new BoundingBox(-0.6,1.4,-0.1,0.1)), false);
    assertEquals(bip.test(new BoundingBox(-0.6,-0.4,-1.1,0.1)), false);
    assertEquals(bip.test(new BoundingBox(-0.6,-0.4,-0.1,1.1)), false);
    // in concave part
    assertEquals(bip.test(new BoundingBox(0.4,0.6,-0.1,0.1)), false);
    assertEquals(bip.test(new BoundingBox(0.4,0.6,-0.9,-0.8)), true);
    assertEquals(bip.test(new BoundingBox(0.4,0.6,0.8,0.9)), true);
    // in concave part, coordinates all inside
    assertEquals(bip.test(new BoundingBox(0.4,0.6,-0.9,0.9)), false);
    // outside poly's bbox
    assertEquals(bip.test(new BoundingBox(1.4,1.6,-0.1,0.1)), false);
    // bbox covering
    assertEquals(bip.test(new BoundingBox(-11,10,-10,10)), false);

    // right polygon
    // inside
    assertEquals(bip.test(new BoundingBox(2.1,2.2,-0.1,0.1)), true);
    assertEquals(bip.test(new BoundingBox(3.1,3.2,-0.9,-0.8)), true);
    assertEquals(bip.test(new BoundingBox(3.1,3.2,0.8,0.9)), true);
    assertEquals(bip.test(new BoundingBox(3.8,3.9,-0.1,0.1)), true);
    // partially inside
    assertEquals(bip.test(new BoundingBox(1.8,2.2,-0.1,0.1)), false);
    assertEquals(bip.test(new BoundingBox(3.1,3.2,-1.1,-0.8)), false);
    assertEquals(bip.test(new BoundingBox(3.1,3.2,0.8,1.1)), false);
    assertEquals(bip.test(new BoundingBox(3.8,4.1,-0.1,0.1)), false);
    // in hole
    assertEquals(bip.test(new BoundingBox(2.9,3.1,-0.1,0.1)), false);
    // partially in hole
    assertEquals(bip.test(new BoundingBox(2.4,2.6,-0.1,0.1)), false);
    assertEquals(bip.test(new BoundingBox(3.1,3.2,-0.6,-0.4)), false);
    assertEquals(bip.test(new BoundingBox(3.1,3.2,0.4,0.6)), false);
    assertEquals(bip.test(new BoundingBox(3.4,3.6,-0.1,0.1)), false);
    // intersecting hole
    assertEquals(bip.test(new BoundingBox(2.9,3.1,-0.1,0.1)), false);
    // outside poly's bbox
    assertEquals(bip.test(new BoundingBox(4.1,4.2,-0.1,0.1)), false);
    assertEquals(bip.test(new BoundingBox(1.8,1.9,-0.1,0.1)), false);
    assertEquals(bip.test(new BoundingBox(3.1,3.2,-1.2,-1.1)), false);
    assertEquals(bip.test(new BoundingBox(3.1,3.2,1.1,1.2)), false);
    // covering hole, but all vertices inside polygon
    assertEquals(bip.test(new BoundingBox(2.2,3.8,-0.8,0.8)), false);
  }

  @Test
  public void testBboxInSquareSquareMultiPolygon() {
    MultiPolygon p = createSquareSquareMultiPolygon();
    FastBboxInPolygon bip = new FastBboxInPolygon(p);

    // not inside
    assertEquals(bip.test(new BoundingBox(-1,1,-1,1)), false);
  }
}
