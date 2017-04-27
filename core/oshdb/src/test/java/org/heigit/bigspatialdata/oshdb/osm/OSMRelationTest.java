package org.heigit.bigspatialdata.oshdb.osm;

import java.util.logging.Logger;

import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.junit.Assert;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author Moritz Schott <m.schott@stud.uni-heidelberg.de>
 */
public class OSMRelationTest {

  private static final Logger LOG = Logger.getLogger(OSMRelationTest.class.getName());

  public OSMRelationTest() {
  }

  @Test
  public void testGetMembers() {
    System.out.println("getMembers");
    OSMMember part = new OSMMember(1L, 1, 1);
    OSMRelation instance = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    OSMMember[] expResult = new OSMMember[]{part, part};
    OSMMember[] result = instance.getMembers();
    assertArrayEquals(expResult, result);
  }

  @Test
  public void testCompareTo() {
    System.out.println("compareTo");
    OSMMember part = new OSMMember(1L, 1, 1);
    OSMRelation instance = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    OSMRelation o = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    assertTrue(instance.compareTo(o) < 0);
  }

  @Test
  public void testCompareToII() {
    System.out.println("compareTo");
    OSMMember part = new OSMMember(1L, 1, 1);
    OSMRelation instance = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    OSMRelation o = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    assertTrue(instance.compareTo(o) == 0);
  }

  @Test
  public void testCompareToIII() {
    System.out.println("compareTo");
    OSMMember part = new OSMMember(1L, 1, 1);
    OSMRelation instance = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    OSMRelation o = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    assertTrue(instance.compareTo(o) > 0);
  }

  //-----------------------
  @Test
  public void testGetId() {
    System.out.println("getId");
    OSMMember part = new OSMMember(1L, 1, 1);
    OSMRelation instance = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    long expResult = 1L;
    long result = instance.getId();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetVersion() {
    System.out.println("getVersion");
    OSMMember part = new OSMMember(1L, 1, 1);
    OSMRelation instance = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    int expResult = 2;
    int result = instance.getVersion();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetTimestamp() {
    System.out.println("getTimestamp");
    OSMMember part = new OSMMember(1L, 1, 1);
    OSMRelation instance = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    long expResult = 1L;
    long result = instance.getTimestamp();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetChangeset() {
    System.out.println("getChangeset");
    OSMMember part = new OSMMember(1L, 1, 1);
    OSMRelation instance = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    long expResult = 1L;
    long result = instance.getChangeset();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetUserId() {
    System.out.println("getUserId");
    OSMMember part = new OSMMember(1L, 1, 1);
    OSMRelation instance = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    int expResult = 1;
    int result = instance.getUserId();
    assertEquals(expResult, result);
  }

  @Test
  public void testisVisible() {
    System.out.println("isVisible");
    OSMMember part = new OSMMember(1L, 1, 1);
    OSMRelation instance = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    boolean expResult = true;
    boolean result = instance.isVisible();
    assertEquals(expResult, result);
  }

  @Test
  public void testisVisibleII() {
    System.out.println("isVisible");
    OSMMember part = new OSMMember(1L, 1, 1);
    OSMRelation instance = new OSMRelation(1L, -2, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    boolean expResult = false;
    boolean result = instance.isVisible();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetTags() {
    System.out.println("getTags");
    OSMMember part = new OSMMember(1L, 1, 1);
    OSMRelation instance = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    int[] expResult = new int[]{1, 1, 2, 2};
    int[] result = instance.getTags();
    Assert.assertArrayEquals(expResult, result);
  }

  @Test
  public void testHasTagKey() {
    System.out.println("hasTagKey");
    OSMMember part = new OSMMember(1L, 1, 1);
    OSMRelation instance = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{}, new OSMMember[]{part, part});
    boolean expResult = false;
    boolean result = instance.hasTagKey(1);
    assertEquals(expResult, result);
  }

  @Test
  public void testHasTagKeyII() {
    System.out.println("hasTagKey");
    OSMMember part = new OSMMember(1L, 1, 1);
    OSMRelation instance = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2, 3, 3}, new OSMMember[]{part, part});
    boolean expResult = true;
    boolean result = instance.hasTagKey(1);
    assertEquals(expResult, result);
  }

  @Test
  public void testHasTagKeyIII() {
    System.out.println("hasTagKey");
    OSMMember part = new OSMMember(1L, 1, 1);
    OSMRelation instance = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 2, 2, 2, 3, 3}, new OSMMember[]{part, part});
    boolean expResult = false;
    boolean result = instance.hasTagKey(1, new int[]{2, 3});
    assertEquals(expResult, result);
  }

  @Test
  public void testHasTagKeyIV() {
    System.out.println("hasTagKey");
    OSMMember part = new OSMMember(1L, 1, 1);
    OSMRelation instance = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2, 3, 3}, new OSMMember[]{part, part});
    boolean expResult = true;
    boolean result = instance.hasTagKey(1, new int[]{2, 3});
    assertEquals(expResult, result);
  }

  @Test
  public void testHasTagKeyV() {
    System.out.println("hasTagKey");
    OSMMember part = new OSMMember(1L, 1, 1);
    OSMRelation instance = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{2, 1, 3, 3}, new OSMMember[]{part, part});
    boolean expResult = false;
    boolean result = instance.hasTagKey(1, new int[]{1, 3});
    assertEquals(expResult, result);
  }

  @Test
  public void testHasTagValue() {
    System.out.println("hasTagValue");
    OSMMember part = new OSMMember(1L, 1, 1);
    OSMRelation instance = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 2, 2, 3}, new OSMMember[]{part, part});
    boolean expResult = false;
    boolean result = instance.hasTagValue(1, 1);
    assertEquals(expResult, result);
  }

  @Test
  public void testHasTagValueII() {
    System.out.println("hasTagValue");
    OSMMember part = new OSMMember(1L, 1, 1);
    OSMRelation instance = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 3}, new OSMMember[]{part, part});
    boolean expResult = true;
    boolean result = instance.hasTagValue(1, 1);
    assertEquals(expResult, result);
  }

  @Test
  public void testToString() {
    System.out.println("toString");
    OSMMember part = new OSMMember(1L, 1, 1);
    OSMRelation instance = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{}, new OSMMember[]{part, part});
    String expResult = "ID:1 V:+2+ TS:1 CS:1 VIS:true USER:1 TAGS:[]";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

  @Test
  public void testToStringII() {
    System.out.println("toString");
    OSMRelation instance = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{});
    String expResult = "ID:1 V:+1+ TS:1 CS:1 VIS:true USER:1 TAGS:[1, 1, 2, 2]";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

  @Test
  public void testToStringIII() {
    System.out.println("toString");
    OSMRelation instance = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{}, null);
    String expResult = "ID:1 V:+1+ TS:1 CS:1 VIS:true USER:1 TAGS:[]";
    String result = instance.toString();
    assertEquals(expResult, result);
  }
}
