/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.util;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author Moritz Schott <m.schott@stud.uni-heidelberg.de>
 */
public class BoundingBoxTest {

  public BoundingBoxTest() {
  }

  @Test
  public void testToString() {
    System.out.println("toString");
    BoundingBox instance = new BoundingBox(0.0, 0.0, 90.0, 90.0);
    System.out.println(instance.toString());
    String expResult = "(0.000000,90.000000) (0.000000,90.000000)";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

}
