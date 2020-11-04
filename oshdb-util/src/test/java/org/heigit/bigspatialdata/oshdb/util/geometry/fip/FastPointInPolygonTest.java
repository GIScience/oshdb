package org.heigit.bigspatialdata.oshdb.util.geometry.fip;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;


public class FastPointInPolygonTest {
  /**
   * Returns a reversed "Σ"-shaped concave polygon.
   */
  public static Polygon createPolygon() {
    final GeometryFactory gf = new GeometryFactory();
    Coordinate[] coordinates = new Coordinate[100];
    coordinates[0] = new Coordinate(0,0);
    coordinates[1] = new Coordinate(1,1);
    coordinates[2] = new Coordinate(-1,1);
    for (int i = 3; i <= 96; i++) {
      coordinates[i] = new Coordinate(-1.0, 1.0 - 2.0 * (i - 2) / 95);
    }
    coordinates[97] = new Coordinate(-1,-1);
    coordinates[98] = new Coordinate(1,-1);
    coordinates[99] = new Coordinate(0,0);
    LinearRing linear = gf.createLinearRing(coordinates);
    return new Polygon(linear, null, gf);
  }

  /**
   * Returns a square with a central square hole.
   */
  public static Polygon createPolygonWithHole() {
    final GeometryFactory gf = new GeometryFactory();
    Coordinate[] coordinates1 = new Coordinate[5];
    coordinates1[0] = new Coordinate(4,-1);
    coordinates1[1] = new Coordinate(4,1);
    coordinates1[2] = new Coordinate(2,1);
    coordinates1[3] = new Coordinate(2,-1);
    coordinates1[4] = new Coordinate(4,-1);
    final LinearRing linear1 = gf.createLinearRing(coordinates1);
    Coordinate[] coordinates2 = new Coordinate[5];
    coordinates2[0] = new Coordinate(3.5,-0.5);
    coordinates2[1] = new Coordinate(3.5,0.5);
    coordinates2[2] = new Coordinate(2.5,0.5);
    coordinates2[3] = new Coordinate(2.5,-0.5);
    coordinates2[4] = new Coordinate(3.5,-0.5);
    final LinearRing linear2 = gf.createLinearRing(coordinates2);
    return new Polygon(linear1, new LinearRing[] { linear2 }, gf);
  }

  /**
   * Returns a reversed "Σ"-shaped concave polygon next to a square with a central square hole.
   */
  public static MultiPolygon createMultiPolygon() {
    GeometryFactory gf = new GeometryFactory();
    Polygon poly1 = createPolygon();
    Polygon poly2 = createPolygonWithHole();
    return new MultiPolygon(new Polygon[] { poly1, poly2 }, gf);
  }

  @Test
  public void testPointInPolygon() {
    Polygon p = createPolygon();
    FastPointInPolygon pip = new FastPointInPolygon(p);

    GeometryFactory gf = new GeometryFactory();

    // inside
    assertEquals(pip.test(gf.createPoint(new Coordinate(-0.5,0))), true);
    // in concave part
    assertEquals(pip.test(gf.createPoint(new Coordinate(0.5,0))), false);
    // outside poly's bbox
    assertEquals(pip.test(gf.createPoint(new Coordinate(1.5,0))), false);
  }

  @Test
  public void testPointInPolygonWithHole() {
    Polygon p = createPolygonWithHole();
    FastPointInPolygon pip = new FastPointInPolygon(p);

    GeometryFactory gf = new GeometryFactory();

    // inside
    assertEquals(pip.test(gf.createPoint(new Coordinate(2.25,0))), true);
    // in hole
    assertEquals(pip.test(gf.createPoint(new Coordinate(3,0))), false);
    // outside poly's bbox
    assertEquals(pip.test(gf.createPoint(new Coordinate(4.5,0))), false);
  }

  @Test
  public void testPointInMultiPolygon() {
    MultiPolygon p = createMultiPolygon();
    FastPointInPolygon pip = new FastPointInPolygon(p);

    GeometryFactory gf = new GeometryFactory();

    // inside left polygon
    assertEquals(pip.test(gf.createPoint(new Coordinate(-0.5,0))), true);
    // in concave part of left polygon
    assertEquals(pip.test(gf.createPoint(new Coordinate(0.5,0))), false);
    // outside left polygon
    assertEquals(pip.test(gf.createPoint(new Coordinate(1.5,0))), false);
    // inside right polygon
    assertEquals(pip.test(gf.createPoint(new Coordinate(2.25,0))), true);
    // in hole of right polygon
    assertEquals(pip.test(gf.createPoint(new Coordinate(3,0))), false);
    // outside right polygon
    assertEquals(pip.test(gf.createPoint(new Coordinate(4.5,0))), false);
  }
}
