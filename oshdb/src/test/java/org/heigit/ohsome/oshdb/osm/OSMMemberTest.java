package org.heigit.ohsome.oshdb.osm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
    int result = instance.getRole().getId();
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
    String expResult = "T:way ID:1 R:1";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

  @Test
  void testEqualsAndEquals() {
    var member = new OSMMember(1234L, OSMType.NODE, 1);
    assertEquals(member, member);
    assertEquals(member, new OSMMember(1234L, OSMType.NODE, 1));
    assertEquals(member.hashCode(), new OSMMember(1234L, OSMType.NODE, 1).hashCode());
  }

  @ParameterizedTest
  @MethodSource("provideMembersForNotEqualsAndHash")
  void testNotEquals(long id, OSMType type, int role) {
    var member = new OSMMember(1234L, OSMType.NODE, 1);
    assertNotEquals(member, new OSMMember(id, type, role));
  }

  private static Stream<Arguments> provideMembersForNotEqualsAndHash() {
    return Stream.of(
        // different types
        Arguments.of(1234L, OSMType.WAY, 1),
        Arguments.of(1234L, OSMType.RELATION, 1),
        // different role
        Arguments.of(1234L, OSMType.NODE, 2),
        // diffrent id
        Arguments.of(23, OSMType.NODE, 1)
      );
  }

}
