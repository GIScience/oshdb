package org.heigit.ohsome.oshdb.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class CellIdTest {
  public CellIdTest() {
  }

  @Test
  public void testGetid() {
    CellId instance = new CellId(1, 1L);
    long expResult = 1L;
    long result = instance.getId();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetzoomlevel() {
    CellId instance = new CellId(1, 1L);
    int expResult = 1;
    int result = instance.getZoomLevel();
    assertEquals(expResult, result);
  }

}
