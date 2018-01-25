package org.heigit.bigspatialdata.oshdb.util.geometry.fip;

import static org.junit.Assert.assertEquals;

import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.junit.Test;


public class FastBboxOutsidePolygonTest {
  @Test
  public void testBboxInPolygon() {
    Polygon p = FastPointInPolygonTest.createPolygon();
    FastBboxOutsidePolygon bop = new FastBboxOutsidePolygon(p);

    // inside
    assertEquals(bop.test(new OSHDBBoundingBox(-0.6,-0.1,-0.4,0.1)), false);
    // partially inside
    assertEquals(bop.test(new OSHDBBoundingBox(-1.5,-0.1,-0.4,0.1)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(-0.6,-0.1,1.4,0.1)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(-0.6,-1.1,-0.4,0.1)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(-0.6,-0.1,-0.4,1.1)), false);
    // in concave part
    assertEquals(bop.test(new OSHDBBoundingBox(0.4,-0.1,0.6,0.1)), true);
    assertEquals(bop.test(new OSHDBBoundingBox(0.4,-0.9,0.6,-0.8)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(0.4,0.8,0.6,0.9)), false);
    // in concave part, coordinates all inside
    assertEquals(bop.test(new OSHDBBoundingBox(0.4,-0.9,0.6,0.9)), false);
    // outside poly's bbox
    assertEquals(bop.test(new OSHDBBoundingBox(1.4,-0.1,1.6,0.1)), true);
    // bbox covering
    assertEquals(bop.test(new OSHDBBoundingBox(-11,-10,10,10)), false);
  }

  @Test
  public void testBboxInPolygonWithHole() {
    Polygon p = FastPointInPolygonTest.createPolygonWithHole();
    FastBboxOutsidePolygon bop = new FastBboxOutsidePolygon(p);

    // inside
    assertEquals(bop.test(new OSHDBBoundingBox(2.1,-0.1,2.2,0.1)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(3.1,-0.9,3.2,-0.8)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(3.1,0.8,3.2,0.9)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(3.8,-0.1,3.9,0.1)), false);
    // partially inside
    assertEquals(bop.test(new OSHDBBoundingBox(1.8,-0.1,2.2,0.1)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(3.1,-1.1,3.2,-0.8)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(3.1,0.8,3.2,1.1)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(3.8,-0.1,4.1,0.1)), false);
    // in hole
    assertEquals(bop.test(new OSHDBBoundingBox(2.9,-0.1,3.1,0.1)), true);
    // partially in hole
    assertEquals(bop.test(new OSHDBBoundingBox(2.4,-0.1,2.6,0.1)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(3.1,-0.6,3.2,-0.4)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(3.1,0.4,3.2,0.6)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(3.4,-0.1,3.6,0.1)), false);
    // intersecting hole
    assertEquals(bop.test(new OSHDBBoundingBox(2.1,-0.1,3.9,0.1)), false);
    // outside poly's bbox
    assertEquals(bop.test(new OSHDBBoundingBox(4.1,-0.1,4.2,0.1)), true);
    assertEquals(bop.test(new OSHDBBoundingBox(1.8,-0.1,1.9,0.1)), true);
    assertEquals(bop.test(new OSHDBBoundingBox(3.1,-1.2,3.2,-1.1)), true);
    assertEquals(bop.test(new OSHDBBoundingBox(3.1,1.1,3.2,1.2)), true);
    // covering hole, but all vertices inside polygon
    assertEquals(bop.test(new OSHDBBoundingBox(2.2,-0.8,3.8,0.8)), false);
  }

  @Test
  public void testBboxInMultiPolygon() {
    MultiPolygon p = FastPointInPolygonTest.createMultiPolygon();
    FastBboxOutsidePolygon bop = new FastBboxOutsidePolygon(p);

    // left polygon
    // inside
    assertEquals(bop.test(new OSHDBBoundingBox(-0.6,-0.1,-0.4,0.1)), false);
    // partially inside
    assertEquals(bop.test(new OSHDBBoundingBox(-1.5,-0.1,-0.4,0.1)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(-0.6,-0.1,1.4,0.1)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(-0.6,-1.1,-0.4,0.1)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(-0.6,-0.1,-0.4,1.1)), false);
    // in concave part
    assertEquals(bop.test(new OSHDBBoundingBox(0.4,-0.1,0.6,0.1)), true);
    assertEquals(bop.test(new OSHDBBoundingBox(0.4,-0.9,0.6,-0.8)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(0.4,0.8,0.6,0.9)), false);
    // in concave part, coordinates all inside
    assertEquals(bop.test(new OSHDBBoundingBox(0.4,-0.9,0.6,0.9)), false);
    // outside poly's bbox
    assertEquals(bop.test(new OSHDBBoundingBox(1.4,-0.1,1.6,0.1)), true);
    // bbox covering
    assertEquals(bop.test(new OSHDBBoundingBox(-11,-10,10,10)), false);

    // right polygon
    // inside
    assertEquals(bop.test(new OSHDBBoundingBox(2.1,-0.1,2.2,0.1)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(3.1,-0.9,3.2,-0.8)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(3.1,0.8,3.2,0.9)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(3.8,-0.1,3.9,0.1)), false);
    // partially inside
    assertEquals(bop.test(new OSHDBBoundingBox(1.8,-0.1,2.2,0.1)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(3.1,-1.1,3.2,-0.8)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(3.1,0.8,3.2,1.1)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(3.8,-0.1,4.1,0.1)), false);
    // in hole
    assertEquals(bop.test(new OSHDBBoundingBox(2.9,-0.1,3.1,0.1)), true);
    // partially in hole
    assertEquals(bop.test(new OSHDBBoundingBox(2.4,-0.1,2.6,0.1)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(3.1,-0.6,3.2,-0.4)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(3.1,0.4,3.2,0.6)), false);
    assertEquals(bop.test(new OSHDBBoundingBox(3.4,-0.1,3.6,0.1)), false);
    // intersecting hole
    assertEquals(bop.test(new OSHDBBoundingBox(2.1,-0.1,3.9,0.1)), false);
    // outside poly's bbox
    assertEquals(bop.test(new OSHDBBoundingBox(4.1,-0.1,4.2,0.1)), true);
    assertEquals(bop.test(new OSHDBBoundingBox(1.8,-0.1,1.9,0.1)), true);
    assertEquals(bop.test(new OSHDBBoundingBox(3.1,-1.2,3.2,-1.1)), true);
    assertEquals(bop.test(new OSHDBBoundingBox(3.1,1.1,3.2,1.2)), true);
    // covering hole, but all vertices inside polygon
    assertEquals(bop.test(new OSHDBBoundingBox(2.2,-0.8,3.8,0.8)), false);
  }

  @Test
  public void testBboxInSquareSquareMultiPolygon() {
    MultiPolygon p = FastBboxInPolygonTest.createSquareSquareMultiPolygon();
    FastBboxOutsidePolygon bop = new FastBboxOutsidePolygon(p);

    // not inside
    assertEquals(bop.test(new OSHDBBoundingBox(-1,-1,1,1)), false);
  }
}
