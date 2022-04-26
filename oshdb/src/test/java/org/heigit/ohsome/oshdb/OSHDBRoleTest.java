package org.heigit.ohsome.oshdb;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class OSHDBRoleTest {

  @Test
  void testEmptyRole() {
    var empty = OSHDBRole.of(-1);
    assertEquals(-1, empty.getId());
    assertEquals(OSHDBRole.EMPTY, empty);
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 0, 1, 3, 5, -3, 15, Integer.MAX_VALUE})
  void testHashCodeAndEquals(int id) {
    var role = OSHDBRole.of(1);
    assertEquals(role, role);
    assertEquals(role, OSHDBRole.of(1));
    assertEquals(role.hashCode(), OSHDBRole.of(1).hashCode());
    assertNotEquals(role, OSHDBRole.of(2));
    assertNotEquals(role, "string");
  }

}
