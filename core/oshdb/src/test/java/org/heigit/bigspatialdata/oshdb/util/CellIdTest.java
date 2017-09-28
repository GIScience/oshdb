/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.util;

import static org.junit.Assert.assertEquals;

import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Moritz Schott <m.schott@stud.uni-heidelberg.de>
 */
public class CellIdTest {
  private static final Logger LOG = LoggerFactory.getLogger(CellIdTest.class);
  
  public CellIdTest() {
  }

  @Test
  public void testGetid() {
    try {
      System.out.println("getid");
      CellId instance = new CellId(1,1L);
      long expResult = 1L;
      long result = instance.getId();
      assertEquals(expResult, result);
    } catch (CellId.cellIdExeption ex) {
      LOG.error("", ex);
    }
  }

  @Test
  public void testGetzoomlevel() {
    try {
      System.out.println("getzoomlevel");
      CellId instance = new CellId(1,1L);
      int expResult = 1;
      int result = instance.getZoomLevel();
      assertEquals(expResult, result);
    } catch (CellId.cellIdExeption ex) {
      LOG.error("", ex);
    }
  }
  
}
