package org.heigit.bigspatialdata.oshdb.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.geotools.geometry.jts.JTS;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

public class OSHDBBoundingBoxTest {

  public OSHDBBoundingBoxTest() {
  }

  @Test
  public void testToString() {
    OSHDBBoundingBox instance = new OSHDBBoundingBox(0.0, 1.0, 89.0, 90.0);
    String expResult = "(0.000000,1.000000) (89.000000,90.000000)";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

  @Test
  public void testIntersect() {
    OSHDBBoundingBox first = new OSHDBBoundingBox(0.0, 89.0, 1.0, 90.0);
    OSHDBBoundingBox second = new OSHDBBoundingBox(0.0, 89.9, 2.0, 90.0);
    OSHDBBoundingBox expResult = new OSHDBBoundingBox(0.0, 89.9, 1.0, 90.0);
    OSHDBBoundingBox result = OSHDBBoundingBox.intersect(first, second);
    assertEquals(expResult, result);
  }

  @Test
  public void testOverlap() {
    OSHDBBoundingBox a = new OSHDBBoundingBox(0.1, 89.1, 0.9, 89.9);
    OSHDBBoundingBox b = new OSHDBBoundingBox(0.0, 89.0, 1.0, 90.0);
    OSHDBBoundingBox.OVERLAP expResult = OSHDBBoundingBox.OVERLAP.A_COMPLETE_IN_B;
    OSHDBBoundingBox.OVERLAP result = OSHDBBoundingBox.overlap(a, b);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetMinLon() {
    OSHDBBoundingBox instance = new OSHDBBoundingBox(0.0, 89.0, 1.0, 90.0);
    double expResult = 0.0;
    double result = instance.getMinLon();
    assertEquals(expResult, result, 0.0);
  }

  @Test
  public void testGetMaxLon() {
    OSHDBBoundingBox instance = new OSHDBBoundingBox(0.0, 89.0, 1.0, 90.0);
    double expResult = 1.0;
    double result = instance.getMaxLon();
    assertEquals(expResult, result, 0.0);
  }

  @Test
  public void testGetMinLat() {
    OSHDBBoundingBox instance = new OSHDBBoundingBox(0.0, 89.0, 1.0, 90.0);
    double expResult = 89.0;
    double result = instance.getMinLat();
    assertEquals(expResult, result, 0.0);
  }

  @Test
  public void testGetMaxLat() {
    OSHDBBoundingBox instance = new OSHDBBoundingBox(0.0, 89.0, 1.0, 90.0);
    double expResult = 90.0;
    double result = instance.getMaxLat();
    assertEquals(expResult, result, 0.0);
  }

  @Test
  public void testGetLon() {
    OSHDBBoundingBox instance = new OSHDBBoundingBox(0.0, 89.0, 1.0, 90.0);
    long[] expResult = new long[]{0L, 10000000L};
    long[] result = instance.getLon();
    assertArrayEquals(expResult, result);
  }

  @Test
  public void testGetLat() {
    OSHDBBoundingBox instance = new OSHDBBoundingBox(0.0, 89.0, 1.0, 90.0);
    long[] expResult = new long[]{890000000L, 900000000L};
    long[] result = instance.getLat();
    assertArrayEquals(expResult, result);
  }

  @Test
  public void testGetGeometry() {
    OSHDBBoundingBox instance = new OSHDBBoundingBox(0.0, 0.0, 1.0, 1.0);
    Polygon expResult = (new GeometryFactory()).createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(1, 0), new Coordinate(1, 1), new Coordinate(0, 1), new Coordinate(0, 0)});

    Polygon result = JTS.toGeometry(new Envelope(instance.getMinLon(), instance.getMaxLon(), instance.getMinLat(), instance.getMaxLat()));
    assertEquals(expResult, result);
  }

  @Test
  public void testHashCode() {
    OSHDBBoundingBox instance = new OSHDBBoundingBox(0.0, 89.0, 1.0, 90.0);
    int expResult = 748664391;
    int result = instance.hashCode();
    assertEquals(expResult, result);
  }

  @Test
  public void testEquals() {
    Object obj = new OSHDBBoundingBox(0.0, 89.0, 1.0, 90.0);
    OSHDBBoundingBox instance = new OSHDBBoundingBox(0.0, 89.0, 1.0, 90.0);
    boolean expResult = true;
    boolean result = instance.equals(obj);
    assertEquals(expResult, result);
  }

}
