package org.heigit.ohsome.oshdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

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
  @ValueSource(ints = {-2, -1, 0, 1, 3, 5, 15, 256, 525, Integer.MAX_VALUE})
  void testHashCodeAndEquals(int id) {
    var expected = OSHDBRole.of(id);
    var role = OSHDBRole.of(id);
    assertEquals(role, role);
    assertEquals(expected, role);
    assertEquals(expected.hashCode(), role.hashCode());

    var unexpect = OSHDBRole.of(2);
    assertNotEquals(unexpect, role);
  }

  @Test
  void testNotEqualsOtherType() {
    var unexpect = OSHDBRole.of(2);
    assertNotEquals(unexpect, unexpect.toString());
  }

}
