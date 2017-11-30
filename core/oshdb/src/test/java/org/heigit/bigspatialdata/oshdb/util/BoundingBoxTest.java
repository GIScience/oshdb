package org.heigit.bigspatialdata.oshdb.util;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class BoundingBoxTest {

  public BoundingBoxTest() {
  }

  @Test
  public void testToString() {
    BoundingBox instance = new BoundingBox(0.0, 1.0, 89.0, 90.0);
    String expResult = "(0.000000,1.000000) (89.000000,90.000000)";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

  @Test
  public void testIntersect() {
    BoundingBox first = new BoundingBox(0.0, 1.0, 89.0, 90.0);
    BoundingBox second = new BoundingBox(0.0, 2.0, 89.9, 90.0);
    BoundingBox expResult = new BoundingBox(0.0, 1.0, 89.9, 90.0);
    BoundingBox result = BoundingBox.intersect(first, second);
    assertEquals(expResult, result);
  }

  @Test
  public void testUnion() {
    BoundingBox first = new BoundingBox(0.0, 1.0, 89.0, 90.0);
    BoundingBox second = new BoundingBox(1.0, 2.0, 88.0, 89.0);
    BoundingBox expResult = new BoundingBox(0.0, 2.0, 88.0, 90.0);
    BoundingBox result = BoundingBox.union(first, second);
    assertEquals(expResult, result);
  }

  @Test
  public void testOverlap() {
    BoundingBox a = new BoundingBox(0.1, 0.9, 89.1, 89.9);
    BoundingBox b = new BoundingBox(0.0, 1.0, 89.0, 90.0);
    BoundingBox.OVERLAP expResult = BoundingBox.OVERLAP.A_COMPLETE_IN_B;
    BoundingBox.OVERLAP result = BoundingBox.overlap(a, b);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetMinLon() {
    BoundingBox instance = new BoundingBox(0.0, 1.0, 89.0, 90.0);
    double expResult = 0.0;
    double result = instance.getMinLon();
    assertEquals(expResult, result, 0.0);
  }

  @Test
  public void testGetMaxLon() {
    BoundingBox instance = new BoundingBox(0.0, 1.0, 89.0, 90.0);
    double expResult = 1.0;
    double result = instance.getMaxLon();
    assertEquals(expResult, result, 0.0);
  }

  @Test
  public void testGetMinLat() {
    BoundingBox instance = new BoundingBox(0.0, 1.0, 89.0, 90.0);
    double expResult = 89.0;
    double result = instance.getMinLat();
    assertEquals(expResult, result, 0.0);
  }

  @Test
  public void testGetMaxLat() {
    BoundingBox instance = new BoundingBox(0.0, 1.0, 89.0, 90.0);
    double expResult = 90.0;
    double result = instance.getMaxLat();
    assertEquals(expResult, result, 0.0);
  }

  @Test
  public void testGetLon() {
    BoundingBox instance = new BoundingBox(0.0, 1.0, 89.0, 90.0);
    long[] expResult = new long[]{0L, 10000000L};
    long[] result = instance.getLon();
    assertArrayEquals(expResult, result);
  }

  @Test
  public void testGetLat() {
    BoundingBox instance = new BoundingBox(0.0, 1.0, 89.0, 90.0);
    long[] expResult = new long[]{890000000L, 900000000L};
    long[] result = instance.getLat();
    assertArrayEquals(expResult, result);
  }

  @Test
  public void testGetGeometry() {
    BoundingBox instance = new BoundingBox(0.0, 1.0, 0.0, 1.0);
    Polygon expResult = (new GeometryFactory()).createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(1, 0), new Coordinate(1, 1), new Coordinate(0, 1), new Coordinate(0, 0)});
    Polygon result = instance.getGeometry();
    assertEquals(expResult, result);
  }

  @Test
  public void testHashCode() {
    BoundingBox instance = new BoundingBox(0.0, 1.0, 89.0, 90.0);
    int expResult = 748664391;
    int result = instance.hashCode();
    assertEquals(expResult, result);
  }

  @Test
  public void testEquals() {
    Object obj = new BoundingBox(0.0, 1.0, 89.0, 90.0);
    BoundingBox instance = new BoundingBox(0.0, 1.0, 89.0, 90.0);
    boolean expResult = true;
    boolean result = instance.equals(obj);
    assertEquals(expResult, result);
  }

}
