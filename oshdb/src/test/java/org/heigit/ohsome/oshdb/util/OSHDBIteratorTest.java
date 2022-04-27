package org.heigit.ohsome.oshdb.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

class OSHDBIteratorTest {

  @Test
  void test() {
    var obj = new Object();
    var list = List.of(obj);
    var itr = OSHDBIterator.peeking(list);

    assertTrue(itr.hasNext());
    assertEquals(obj, itr.peek());
    assertEquals(obj, itr.next());
    assertFalse(itr.hasNext());

    assertThrows(NoSuchElementException.class, () -> itr.peek());
    assertThrows(NoSuchElementException.class, () -> itr.next());
  }
}
