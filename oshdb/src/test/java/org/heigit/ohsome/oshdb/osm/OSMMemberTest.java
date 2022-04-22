package org.heigit.ohsome.oshdb.osm;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class OSMMemberTest {

  OSMMemberTest() {}

  @Test
  void testGetId() {
    OSMMember instance = new OSMMember(1L, OSMType.WAY, 1);
    long expResult = 1L;
    long result = instance.getId();
    assertEquals(expResult, result);
  }

  @Test
  void testGetType() {
    OSMMember instance = new OSMMember(1L, OSMType.WAY, 1);
    OSMType expResult = OSMType.WAY;
    OSMType result = instance.getType();
    assertEquals(expResult, result);
  }

  @Test
  void testGetRoleId() {
    OSMMember instance = new OSMMember(1L, OSMType.WAY, 1);
    int expResult = 1;
    int result = instance.getRawRoleId();
    assertEquals(expResult, result);
  }

  @Test
  void testGetData() {
    // getEntity (explicit null)
    OSMMember instance = new OSMMember(1L, OSMType.WAY, 1, null);
    Object expResult = null;
    Object result = instance.getEntity();
    assertEquals(expResult, result);
  }

  @Test
  void testGetData2() {
    // getEntity (implicit null)
    OSMMember instance = new OSMMember(1L, OSMType.WAY, 1);
    Object expResult = null;
    Object result = instance.getEntity();
    assertEquals(expResult, result);
  }

  @Test
  void testToString() {
    OSMMember instance = new OSMMember(1L, OSMType.WAY, 1);
    String expResult = "T:WAY ID:1 R:1";
    String result = instance.toString();
    assertEquals(expResult, result);
  }
}
