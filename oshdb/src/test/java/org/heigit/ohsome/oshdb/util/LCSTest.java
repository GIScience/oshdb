package org.heigit.ohsome.oshdb.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.heigit.ohsome.oshdb.util.LCS.Action;
import org.junit.jupiter.api.Test;

class LCSTest {

  @Test
  void test() {
    int[] prev = {1, 2, 3, 7, 8, 4, 5};
    int[] next = {1, 2, 6, 7, 8, 9, 4, 5};
    var actions = LCS.lcs(prev, prev.length, next, next.length);
    System.out.println(actions);
    assertEquals(5, actions.size());

    assertEquals(Map.entry(Action.TAKE, 2), actions.get(0));
    assertEquals(Map.entry(Action.UPDATE, 1), actions.get(1));
    assertEquals(Map.entry(Action.TAKE, 2), actions.get(2));
    assertEquals(Map.entry(Action.ADD, 1), actions.get(3));
    assertEquals(Map.entry(Action.TAKE, 2), actions.get(4));
  }

  @Test
  void testSkipMiddle() {
    int[] prev = {1, 2, 3, 4, 5};
    int[] next = {1, 2, 4, 5};
    var actions = LCS.lcs(prev, prev.length, next, next.length);


    assertEquals(3, actions.size());
    assertEquals(Map.entry(Action.TAKE, 2), actions.get(0));
    assertEquals(Map.entry(Action.SKIP, 1), actions.get(1));
    assertEquals(Map.entry(Action.TAKE, 2), actions.get(2));
  }

  @Test
  void testAppendOnly() {
    int[] prev = {1, 2};
    int[] next = {1, 2, 4, 5};
    var actions = LCS.lcs(prev, prev.length, next, next.length);

    assertEquals(2, actions.size());
    assertEquals(Map.entry(Action.TAKE, 2), actions.get(0));
    assertEquals(Map.entry(Action.ADD, 2), actions.get(1));
  }

  @Test
  void testRemoveFront() {
    int[] prev = {1, 2, 4, 5};
    int[] next = {4, 5};
    var actions = LCS.lcs(prev, prev.length, next, next.length);

    assertEquals(2, actions.size());
    assertEquals(Map.entry(Action.SKIP, 2), actions.get(0));
    assertEquals(Map.entry(Action.TAKE, 2), actions.get(1));
  }

  @Test
  void testRemoveBack() {
    int[] prev = {1, 2, 4, 5};
    int[] next = {1, 2};
    var actions = LCS.lcs(prev, prev.length, next, next.length);

    assertEquals(1, actions.size());
    assertEquals(Map.entry(Action.TAKE, 2), actions.get(0));
  }

  @Test
  void testEquals() {
    int[] prev = {1, 2};
    int[] next = {1, 2};
    var actions = LCS.lcs(prev, prev.length, next, next.length);

    assertEquals(0, actions.size());
  }

  @Test
  void testUpdateBack() {
    int[] prev = {1, 2, 3, 4};
    int[] next = {1, 2, 5, 6};
    var actions = LCS.lcs(prev, prev.length, next, next.length);

    assertEquals(2, actions.size());
    assertEquals(Map.entry(Action.TAKE, 2), actions.get(0));
    assertEquals(Map.entry(Action.UPDATE, 2), actions.get(1));
  }

}
