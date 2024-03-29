package org.heigit.ohsome.oshdb.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.junit.jupiter.api.Test;

class OSHDBBoundingBoxTest {

  @Test
  void testToString() {
    OSHDBBoundingBox instance = OSHDBBoundingBox.bboxWgs84Coordinates(0.0, 1.0, 89.0, 90.0);
    String expResult = "(0.0000000,1.0000000,89.0000000,90.0000000)";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

  @Test
  void testIntersect() {
    OSHDBBoundingBox first = OSHDBBoundingBox.bboxWgs84Coordinates(0.0, 89.0, 1.0, 90.0);
    OSHDBBoundingBox second = OSHDBBoundingBox.bboxWgs84Coordinates(0.0, 89.9, 2.0, 90.0);
    OSHDBBoundingBox expResult = OSHDBBoundingBox.bboxWgs84Coordinates(0.0, 89.9, 1.0, 90.0);
    OSHDBBoundingBox result = first.intersection(second);
    assertEquals(expResult, result);
  }

  @Test
  void testGetMinLon() {
    OSHDBBoundingBox instance = OSHDBBoundingBox.bboxWgs84Coordinates(0.0, 89.0, 1.0, 90.0);
    int expResult = 0;
    int result = instance.getMinLongitude();
    assertEquals(expResult, result);
  }

  @Test
  void testGetMaxLon() {
    OSHDBBoundingBox instance = OSHDBBoundingBox.bboxWgs84Coordinates(0.0, 89.0, 1.0, 90.0);
    int expResult = 1_0000000;
    int result = instance.getMaxLongitude();
    assertEquals(expResult, result);
  }

  @Test
  void testGetMinLat() {
    OSHDBBoundingBox instance = OSHDBBoundingBox.bboxWgs84Coordinates(0.0, 89.0, 1.0, 90.0);
    int expResult = 89_0000000;
    int result = instance.getMinLatitude();
    assertEquals(expResult, result);
  }

  @Test
  void testGetMaxLat() {
    OSHDBBoundingBox instance = OSHDBBoundingBox.bboxWgs84Coordinates(0.0, 89.0, 1.0, 90.0);
    int expResult = 90_0000000;
    int result = instance.getMaxLatitude();
    assertEquals(expResult, result);
  }

  @Test
  void testHashCode() {
    OSHDBBoundingBox instance = OSHDBBoundingBox.bboxWgs84Coordinates(0.0, 89.0, 1.0, 90.0);
    int expResult = 1260356225;
    int result = instance.hashCode();
    assertEquals(expResult, result);
  }

  @Test
  void testEquals() {
    Object obj = OSHDBBoundingBox.bboxWgs84Coordinates(0.0, 89.0, 1.0, 90.0);
    assertEquals(obj, obj);
    assertNotEquals("", obj);
    assertEquals(obj, OSHDBBoundingBox.bboxWgs84Coordinates(0.0, 89.0, 1.0, 90.0));
    assertNotEquals(obj, OSHDBBoundingBox.bboxWgs84Coordinates(0.1, 89.0, 1.0, 90.0));
  }
}
