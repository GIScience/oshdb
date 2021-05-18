package org.heigit.ohsome.oshdb.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.junit.Test;

public class OSHDBBoundingBoxTest {

  public OSHDBBoundingBoxTest() {
  }

  @Test
  public void testToString() {
    OSHDBBoundingBox instance = OSHDBBoundingBox.bboxLonLatCoordinates(0.0, 1.0, 89.0, 90.0);
    String expResult = "(0.0000000,1.0000000,89.0000000,90.0000000)";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

  @Test
  public void testIntersect() {
    OSHDBBoundingBox first = OSHDBBoundingBox.bboxLonLatCoordinates(0.0, 89.0, 1.0, 90.0);
    OSHDBBoundingBox second = OSHDBBoundingBox.bboxLonLatCoordinates(0.0, 89.9, 2.0, 90.0);
    OSHDBBoundingBox expResult = OSHDBBoundingBox.bboxLonLatCoordinates(0.0, 89.9, 1.0, 90.0);
    OSHDBBoundingBox result = first.intersection(second);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetMinLon() {
    OSHDBBoundingBox instance = OSHDBBoundingBox.bboxLonLatCoordinates(0.0, 89.0, 1.0, 90.0);
    double expResult = 0.0;
    double result = instance.getMinLongitude();
    assertEquals(expResult, result, 0.0);
  }

  @Test
  public void testGetMaxLon() {
    OSHDBBoundingBox instance = OSHDBBoundingBox.bboxLonLatCoordinates(0.0, 89.0, 1.0, 90.0);
    double expResult = 1.0;
    double result = instance.getMaxLongitude();
    assertEquals(expResult, result, 0.0);
  }

  @Test
  public void testGetMinLat() {
    OSHDBBoundingBox instance = OSHDBBoundingBox.bboxLonLatCoordinates(0.0, 89.0, 1.0, 90.0);
    double expResult = 89.0;
    double result = instance.getMinLatitude();
    assertEquals(expResult, result, 0.0);
  }

  @Test
  public void testGetMaxLat() {
    OSHDBBoundingBox instance = OSHDBBoundingBox.bboxLonLatCoordinates(0.0, 89.0, 1.0, 90.0);
    double expResult = 90.0;
    double result = instance.getMaxLatitude();
    assertEquals(expResult, result, 0.0);
  }

  @Test
  public void testGetLon() {
    OSHDBBoundingBox instance = OSHDBBoundingBox.bboxLonLatCoordinates(0.0, 89.0, 1.0, 90.0);
    int[] expResult = new int[]{0, 10000000};
    int[] result = instance.getLon();
    assertArrayEquals(expResult, result);
  }

  @Test
  public void testGetLat() {
    OSHDBBoundingBox instance = OSHDBBoundingBox.bboxLonLatCoordinates(0.0, 89.0, 1.0, 90.0);
    int[] expResult = new int[]{890000000, 900000000};
    int[] result = instance.getLat();
    assertArrayEquals(expResult, result);
  }

  @Test
  public void testHashCode() {
    OSHDBBoundingBox instance = OSHDBBoundingBox.bboxLonLatCoordinates(0.0, 89.0, 1.0, 90.0);
    int expResult = 1260356225;
    int result = instance.hashCode();
    assertEquals(expResult, result);
  }

  @Test
  public void testEquals() {
    Object obj = OSHDBBoundingBox.bboxLonLatCoordinates(0.0, 89.0, 1.0, 90.0);
    assertEquals(obj, obj);
    assertNotEquals("", obj);
    assertEquals(obj, OSHDBBoundingBox.bboxLonLatCoordinates(0.0, 89.0, 1.0, 90.0));
    assertNotEquals(obj, OSHDBBoundingBox.bboxLonLatCoordinates(0.1, 89.0, 1.0, 90.0));
  }

}
