package org.heigit.bigspatialdata.oshdb.util.geometry.fip;

import com.vividsolutions.jts.geom.*;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class FastBboxInPolygonTest {
  /**
   * @return a multipolygon of four small squares arranged in a square
   */
  public static MultiPolygon createSquareSquareMultiPolygon() {
    GeometryFactory gf = new GeometryFactory();
    Polygon poly1 = OSHDBGeometryBuilder.getGeometry(new OSHDBBoundingBox(-1.5,-1.5,-0.5,-0.5));
    Polygon poly2 = OSHDBGeometryBuilder.getGeometry(new OSHDBBoundingBox(0.5,-1.5,1.5,-0.5));
    Polygon poly3 = OSHDBGeometryBuilder.getGeometry(new OSHDBBoundingBox(-1.5,0.5,-0.5,1.5));
    Polygon poly4 = OSHDBGeometryBuilder.getGeometry(new OSHDBBoundingBox(0.5,0.5,1.5,1.5));
    return new MultiPolygon(new Polygon[] { poly1, poly2, poly3, poly4 }, gf);
  }

  @Test
  public void testBboxInPolygon() {
    Polygon p = FastPointInPolygonTest.createPolygon();
    FastBboxInPolygon bip = new FastBboxInPolygon(p);

    // inside
    assertEquals(bip.test(new OSHDBBoundingBox(-0.6,-0.1,-0.4,0.1)), true);
    // partially inside
    assertEquals(bip.test(new OSHDBBoundingBox(-1.5,-0.1,-0.4,0.1)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(-0.6,-0.1,1.4,0.1)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(-0.6,-1.1,-0.4,0.1)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(-0.6,-0.1,-0.4,1.1)), false);
    // in concave part
    assertEquals(bip.test(new OSHDBBoundingBox(0.4,-0.1,0.6,0.1)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(0.4,-0.9,0.6,-0.8)), true);
    assertEquals(bip.test(new OSHDBBoundingBox(0.4,0.8,0.6,0.9)), true);
    // in concave part, coordinates all inside
    assertEquals(bip.test(new OSHDBBoundingBox(0.4,-0.9,0.6,0.9)), false);
    // outside poly's bbox
    assertEquals(bip.test(new OSHDBBoundingBox(1.4,-0.1,1.6,0.1)), false);
    // bbox covering
    assertEquals(bip.test(new OSHDBBoundingBox(-11,-10,10,10)), false);
  }

  @Test
  public void testBboxInPolygonWithHole() {
    Polygon p = FastPointInPolygonTest.createPolygonWithHole();
    FastBboxInPolygon bip = new FastBboxInPolygon(p);

    // inside
    assertEquals(bip.test(new OSHDBBoundingBox(2.1,-0.1,2.2,0.1)), true);
    assertEquals(bip.test(new OSHDBBoundingBox(3.1,-0.9,3.2,-0.8)), true);
    assertEquals(bip.test(new OSHDBBoundingBox(3.1,0.8,3.2,0.9)), true);
    assertEquals(bip.test(new OSHDBBoundingBox(3.8,-0.1,3.9,0.1)), true);
    // partially inside
    assertEquals(bip.test(new OSHDBBoundingBox(1.8,-0.1,2.2,0.1)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(3.1,-1.1,3.2,-0.8)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(3.1,0.8,3.2,1.1)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(3.8,-0.1,4.1,0.1)), false);
    // in hole
    assertEquals(bip.test(new OSHDBBoundingBox(2.9,-0.1,3.1,0.1)), false);
    // partially in hole
    assertEquals(bip.test(new OSHDBBoundingBox(2.4,-0.1,2.6,0.1)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(3.1,-0.6,3.2,-0.4)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(3.1,0.4,3.2,0.6)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(3.4,-0.1,3.6,0.1)), false);
    // intersecting hole
    assertEquals(bip.test(new OSHDBBoundingBox(2.1,-0.1,3.9,0.1)), false);
    // outside poly's bbox
    assertEquals(bip.test(new OSHDBBoundingBox(4.1,-0.1,4.2,0.1)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(1.8,-0.1,1.9,0.1)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(3.1,-1.2,3.2,-1.1)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(3.1,1.1,3.2,1.2)), false);
    // covering hole, but all vertices inside polygon
    assertEquals(bip.test(new OSHDBBoundingBox(2.2,-0.8,3.8,0.8)), false);
  }

  @Test
  public void testBboxInMultiPolygon() {
    MultiPolygon p = FastPointInPolygonTest.createMultiPolygon();
    FastBboxInPolygon bip = new FastBboxInPolygon(p);

    // left polygon
    // inside
    assertEquals(bip.test(new OSHDBBoundingBox(-0.6,-0.1,-0.4,0.1)), true);
    // partially inside
    assertEquals(bip.test(new OSHDBBoundingBox(-1.5,-0.1,-0.4,0.1)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(-0.6,-0.1,1.4,0.1)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(-0.6,-1.1,-0.4,0.1)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(-0.6,-0.1,-0.4,1.1)), false);
    // in concave part
    assertEquals(bip.test(new OSHDBBoundingBox(0.4,-0.1,0.6,0.1)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(0.4,-0.9,0.6,-0.8)), true);
    assertEquals(bip.test(new OSHDBBoundingBox(0.4,0.8,0.6,0.9)), true);
    // in concave part, coordinates all inside
    assertEquals(bip.test(new OSHDBBoundingBox(0.4,-0.9,0.6,0.9)), false);
    // outside poly's bbox
    assertEquals(bip.test(new OSHDBBoundingBox(1.4,-0.1,1.6,0.1)), false);
    // bbox covering
    assertEquals(bip.test(new OSHDBBoundingBox(-11,-10,10,10)), false);

    // right polygon
    // inside
    assertEquals(bip.test(new OSHDBBoundingBox(2.1,-0.1,2.2,0.1)), true);
    assertEquals(bip.test(new OSHDBBoundingBox(3.1,-0.9,3.2,-0.8)), true);
    assertEquals(bip.test(new OSHDBBoundingBox(3.1,0.8,3.2,0.9)), true);
    assertEquals(bip.test(new OSHDBBoundingBox(3.8,-0.1,3.9,0.1)), true);
    // partially inside
    assertEquals(bip.test(new OSHDBBoundingBox(1.8,-0.1,2.2,0.1)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(3.1,-1.1,3.2,-0.8)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(3.1,0.8,3.2,1.1)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(3.8,-0.1,4.1,0.1)), false);
    // in hole
    assertEquals(bip.test(new OSHDBBoundingBox(2.9,-0.1,3.1,0.1)), false);
    // partially in hole
    assertEquals(bip.test(new OSHDBBoundingBox(2.4,-0.1,2.6,0.1)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(3.1,-0.6,3.2,-0.4)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(3.1,0.4,3.2,0.6)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(3.4,-0.1,3.6,0.1)), false);
    // intersecting hole
    assertEquals(bip.test(new OSHDBBoundingBox(2.1,-0.1,3.9,0.1)), false);
    // outside poly's bbox
    assertEquals(bip.test(new OSHDBBoundingBox(4.1,-0.1,4.2,0.1)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(1.8,-0.1,1.9,0.1)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(3.1,-1.2,3.2,-1.1)), false);
    assertEquals(bip.test(new OSHDBBoundingBox(3.1,1.1,3.2,1.2)), false);
    // covering hole, but all vertices inside polygon
    assertEquals(bip.test(new OSHDBBoundingBox(2.2,-0.8,3.8,0.8)), false);
  }

  @Test
  public void testBboxInSquareSquareMultiPolygon() {
    MultiPolygon p = createSquareSquareMultiPolygon();
    FastBboxInPolygon bip = new FastBboxInPolygon(p);

    // not inside
    assertEquals(bip.test(new OSHDBBoundingBox(-1,-1,1,1)), false);
  }
}
