/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.util;

import static org.junit.Assert.assertEquals;

import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.junit.Test;

/**
 *
 * @author Moritz Schott <m.schott@stud.uni-heidelberg.de>
 */
public class CellIdTest {
  public CellIdTest() {
  }

  @Test
  public void testGetid() {
    CellId instance = new CellId(1,1L);
    long expResult = 1L;
    long result = instance.getId();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetzoomlevel() {
    CellId instance = new CellId(1,1L);
    int expResult = 1;
    int result = instance.getZoomLevel();
    assertEquals(expResult, result);
  }
  
}
