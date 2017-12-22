package org.heigit.bigspatialdata.oshdb.util.fip;

import static org.junit.Assert.assertEquals;

import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.junit.Test;


public class FastBboxOutsidePolygonTest {
  @Test
  public void testBboxInPolygon() {
    Polygon p = FastPointInPolygonTest.createPolygon();
    FastBboxOutsidePolygon bop = new FastBboxOutsidePolygon(p);

    // inside
    assertEquals(bop.test(new BoundingBox(-0.6,-0.4,-0.1,0.1)), false);
    // partially inside
    assertEquals(bop.test(new BoundingBox(-1.5,-0.4,-0.1,0.1)), false);
    assertEquals(bop.test(new BoundingBox(-0.6,1.4,-0.1,0.1)), false);
    assertEquals(bop.test(new BoundingBox(-0.6,-0.4,-1.1,0.1)), false);
    assertEquals(bop.test(new BoundingBox(-0.6,-0.4,-0.1,1.1)), false);
    // in concave part
    assertEquals(bop.test(new BoundingBox(0.4,0.6,-0.1,0.1)), true);
    assertEquals(bop.test(new BoundingBox(0.4,0.6,-0.9,-0.8)), false);
    assertEquals(bop.test(new BoundingBox(0.4,0.6,0.8,0.9)), false);
    // in concave part, coordinates all inside
    assertEquals(bop.test(new BoundingBox(0.4,0.6,-0.9,0.9)), false);
    // outside poly's bbox
    assertEquals(bop.test(new BoundingBox(1.4,1.6,-0.1,0.1)), true);
    // bbox covering
    assertEquals(bop.test(new BoundingBox(-11,10,-10,10)), false);
  }

  @Test
  public void testBboxInPolygonWithHole() {
    Polygon p = FastPointInPolygonTest.createPolygonWithHole();
    FastBboxOutsidePolygon bop = new FastBboxOutsidePolygon(p);

    // inside
    assertEquals(bop.test(new BoundingBox(2.1,2.2,-0.1,0.1)), false);
    assertEquals(bop.test(new BoundingBox(3.1,3.2,-0.9,-0.8)), false);
    assertEquals(bop.test(new BoundingBox(3.1,3.2,0.8,0.9)), false);
    assertEquals(bop.test(new BoundingBox(3.8,3.9,-0.1,0.1)), false);
    // partially inside
    assertEquals(bop.test(new BoundingBox(1.8,2.2,-0.1,0.1)), false);
    assertEquals(bop.test(new BoundingBox(3.1,3.2,-1.1,-0.8)), false);
    assertEquals(bop.test(new BoundingBox(3.1,3.2,0.8,1.1)), false);
    assertEquals(bop.test(new BoundingBox(3.8,4.1,-0.1,0.1)), false);
    // in hole
    assertEquals(bop.test(new BoundingBox(2.9,3.1,-0.1,0.1)), true);
    // partially in hole
    assertEquals(bop.test(new BoundingBox(2.4,2.6,-0.1,0.1)), false);
    assertEquals(bop.test(new BoundingBox(3.1,3.2,-0.6,-0.4)), false);
    assertEquals(bop.test(new BoundingBox(3.1,3.2,0.4,0.6)), false);
    assertEquals(bop.test(new BoundingBox(3.4,3.6,-0.1,0.1)), false);
    // intersecting hole
    assertEquals(bop.test(new BoundingBox(2.1,3.9,-0.1,0.1)), false);
    // outside poly's bbox
    assertEquals(bop.test(new BoundingBox(4.1,4.2,-0.1,0.1)), true);
    assertEquals(bop.test(new BoundingBox(1.8,1.9,-0.1,0.1)), true);
    assertEquals(bop.test(new BoundingBox(3.1,3.2,-1.2,-1.1)), true);
    assertEquals(bop.test(new BoundingBox(3.1,3.2,1.1,1.2)), true);
    // covering hole, but all vertices inside polygon
    assertEquals(bop.test(new BoundingBox(2.2,3.8,-0.8,0.8)), false);
  }

  @Test
  public void testBboxInMultiPolygon() {
    MultiPolygon p = FastPointInPolygonTest.createMultiPolygon();
    FastBboxOutsidePolygon bop = new FastBboxOutsidePolygon(p);

    // left polygon
    // inside
    assertEquals(bop.test(new BoundingBox(-0.6,-0.4,-0.1,0.1)), false);
    // partially inside
    assertEquals(bop.test(new BoundingBox(-1.5,-0.4,-0.1,0.1)), false);
    assertEquals(bop.test(new BoundingBox(-0.6,1.4,-0.1,0.1)), false);
    assertEquals(bop.test(new BoundingBox(-0.6,-0.4,-1.1,0.1)), false);
    assertEquals(bop.test(new BoundingBox(-0.6,-0.4,-0.1,1.1)), false);
    // in concave part
    assertEquals(bop.test(new BoundingBox(0.4,0.6,-0.1,0.1)), true);
    assertEquals(bop.test(new BoundingBox(0.4,0.6,-0.9,-0.8)), false);
    assertEquals(bop.test(new BoundingBox(0.4,0.6,0.8,0.9)), false);
    // in concave part, coordinates all inside
    assertEquals(bop.test(new BoundingBox(0.4,0.6,-0.9,0.9)), false);
    // outside poly's bbox
    assertEquals(bop.test(new BoundingBox(1.4,1.6,-0.1,0.1)), true);
    // bbox covering
    assertEquals(bop.test(new BoundingBox(-11,10,-10,10)), false);

    // right polygon
    // inside
    assertEquals(bop.test(new BoundingBox(2.1,2.2,-0.1,0.1)), false);
    assertEquals(bop.test(new BoundingBox(3.1,3.2,-0.9,-0.8)), false);
    assertEquals(bop.test(new BoundingBox(3.1,3.2,0.8,0.9)), false);
    assertEquals(bop.test(new BoundingBox(3.8,3.9,-0.1,0.1)), false);
    // partially inside
    assertEquals(bop.test(new BoundingBox(1.8,2.2,-0.1,0.1)), false);
    assertEquals(bop.test(new BoundingBox(3.1,3.2,-1.1,-0.8)), false);
    assertEquals(bop.test(new BoundingBox(3.1,3.2,0.8,1.1)), false);
    assertEquals(bop.test(new BoundingBox(3.8,4.1,-0.1,0.1)), false);
    // in hole
    assertEquals(bop.test(new BoundingBox(2.9,3.1,-0.1,0.1)), true);
    // partially in hole
    assertEquals(bop.test(new BoundingBox(2.4,2.6,-0.1,0.1)), false);
    assertEquals(bop.test(new BoundingBox(3.1,3.2,-0.6,-0.4)), false);
    assertEquals(bop.test(new BoundingBox(3.1,3.2,0.4,0.6)), false);
    assertEquals(bop.test(new BoundingBox(3.4,3.6,-0.1,0.1)), false);
    // intersecting hole
    assertEquals(bop.test(new BoundingBox(2.1,3.9,-0.1,0.1)), false);
    // outside poly's bbox
    assertEquals(bop.test(new BoundingBox(4.1,4.2,-0.1,0.1)), true);
    assertEquals(bop.test(new BoundingBox(1.8,1.9,-0.1,0.1)), true);
    assertEquals(bop.test(new BoundingBox(3.1,3.2,-1.2,-1.1)), true);
    assertEquals(bop.test(new BoundingBox(3.1,3.2,1.1,1.2)), true);
    // covering hole, but all vertices inside polygon
    assertEquals(bop.test(new BoundingBox(2.2,3.8,-0.8,0.8)), false);
  }

  @Test
  public void testBboxInSquareSquareMultiPolygon() {
    MultiPolygon p = FastBboxInPolygonTest.createSquareSquareMultiPolygon();
    FastBboxOutsidePolygon bop = new FastBboxOutsidePolygon(p);

    // not inside
    assertEquals(bop.test(new BoundingBox(-1,1,-1,1)), false);
  }
}
