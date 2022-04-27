package org.heigit.ohsome.oshdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class OSHDBTagTest {

  @Test
  void test() {
    var tag = new OSHDBTag(10, 20);
    assertEquals(10, tag.getKey());
    assertEquals(20, tag.getValue());
  }

  @Test
  void testComparable() {
    var tag = new OSHDBTag(10, 10);

    assertEquals(0, tag.compareTo(new OSHDBTag(10, 10)));
    assertTrue(tag.compareTo(new OSHDBTag(5, 10)) > 0);
    assertTrue(tag.compareTo(new OSHDBTag(10, 5)) > 0);
    assertTrue(tag.compareTo(new OSHDBTag(20, 10)) < 0);
    assertTrue(tag.compareTo(new OSHDBTag(10, 15)) < 0);
  }

  @Test
  void testHashEqual() {
    var tag = new OSHDBTag(10, 10);

    assertEquals(tag, tag);
    assertEquals(tag, new OSHDBTag(10, 10));
    assertEquals(tag.hashCode(), new OSHDBTag(10, 10).hashCode());

    assertNotEquals(tag, new OSHDBTag(10, 20));
    assertNotEquals(tag, new OSHDBTag(20, 10));

    assertNotEquals(tag, tag.toString());;
  }

}
