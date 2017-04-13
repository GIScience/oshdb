/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.hosmdb.osm;

import java.util.logging.Logger;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author Moritz Schott <m.schott@stud.uni-heidelberg.de>
 */
public class OSMMemberTest {
  private static final Logger LOG = Logger.getLogger(OSMMemberTest.class.getName());
  
  public OSMMemberTest() {
  }

  @Test
  public void testGetId() {
    System.out.println("getId");
    OSMMember instance = new OSMMember(1L,1,1);
    long expResult = 1L;
    long result = instance.getId();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetType() {
    System.out.println("getType");
    OSMMember instance = new OSMMember(1L,1,1);
    int expResult = 1;
    int result = instance.getType();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetRoleId() {
    System.out.println("getRoleId");
    OSMMember instance = new OSMMember(1L,1,1);
    int expResult = 1;
    int result = instance.getRoleId();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetData() {
    System.out.println("getData");
    OSMMember instance = new OSMMember(1L,1,1,1);
    Object expResult = 1;
    Object result = instance.getData();
    assertEquals(expResult, result);
  }
  
    @Test
  public void testGetDataII() {
    System.out.println("getData");
    OSMMember instance = new OSMMember(1L,1,1);
    Object expResult = null;
    Object result = instance.getData();
    assertEquals(expResult, result);
  }


  @Test
  public void testToString() {
    System.out.println("toString");
    OSMMember instance = new OSMMember(1L,1,1);
    String expResult = "T:1 ID:1 R:1";
    String result = instance.toString();
    assertEquals(expResult, result);
  }
  
}
