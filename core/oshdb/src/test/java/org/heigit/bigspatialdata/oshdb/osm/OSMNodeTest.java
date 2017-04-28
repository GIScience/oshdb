package org.heigit.bigspatialdata.oshdb.osm;

import java.util.logging.Logger;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author Rafael Troilo <rafael.troilo@uni-heidelberg.de>
 * @author Moritz Schott <m.schott@stud.uni-heidelberg.de>
 */
public class OSMNodeTest {

  private static final Logger LOG = Logger.getLogger(OSMNodeTest.class.getName());

  public OSMNodeTest() {
  }

  @Test
  public void testGetLongitude() {
    System.out.println("getLongitude");
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{}, 1000000000L, 1L);
    double expResult = 1.0;
    double result = instance.getLongitude();
    assertEquals(expResult, result, 0.0);
  }

  @Test
  public void testGetLatitude() {
    System.out.println("getLatitude");
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{}, 1000000000L, 1000000000L);
    double expResult = 1.0;
    double result = instance.getLatitude();
    assertEquals(expResult, result, 0.0);
  }

  @Test
  public void testGetLon() {
    System.out.println("getLon");
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{}, 1000000000L, 1000000000L);
    long expResult = 1000000000L;
    long result = instance.getLon();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetLat() {
    System.out.println("getLat");
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{}, 1000000000L, 1000000000L);
    long expResult = 1000000000L;
    long result = instance.getLat();
    assertEquals(expResult, result);
  }

  @Test
  public void testToString() {
    System.out.println("toString");
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{}, 1000000000L, 1000000000L);
    String expResult = "NODE: ID:1 V:+1+ TS:1 CS:1 VIS:true USER:1 TAGS:[] 1.000000:1.000000";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

  @Test
  public void testEqualsTo() {
    System.out.println("equalsTo");
    OSMNode o = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2, 3, 3}, 1000000000L, 1000000000L);
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2, 3, 3}, 1000000000L, 1000000000L);
    boolean expResult = true;
    boolean result = instance.equalsTo(o);
    assertEquals(expResult, result);
  }

  @Test
  public void testEqualsToII() {
    System.out.println("equalsTo");
    OSMNode o = new OSMNode(2L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2, 3, 3}, 1000000000L, 1000000000L);
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2, 3, 3}, 1000000000L, 1000000000L);
    boolean expResult = false;
    boolean result = instance.equalsTo(o);
    assertEquals(expResult, result);
  }

  @Test
  public void testCompareTo() {
    System.out.println("compareTo");
    OSMNode o = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2, 3, 3}, 1000000000L, 1000000000L);
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2, 3, 3}, 1000000000L, 1000000000L);
    assertTrue(instance.compareTo(o) == 0);
  }

  @Test
  public void testCompareToII() {
    System.out.println("compareTo");
    OSMNode o = new OSMNode(1L, 3, 1L, 1L, 1, new int[]{1, 1, 2, 2, 3, 3}, 1000000000L, 1000000000L);
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2, 3, 3}, 1000000000L, 1000000000L);
    assertTrue(instance.compareTo(o) < 0);
  }

  @Test
  public void testCompareToIII() {
    System.out.println("compareTo");
    OSMNode o = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2, 3, 3}, 1000000000L, 1000000000L);
    OSMNode instance = new OSMNode(1L, 3, 1L, 1L, 1, new int[]{1, 1, 2, 2, 3, 3}, 1000000000L, 1000000000L);
    assertTrue(instance.compareTo(o) > 0);
  }

  @Test
  public void testCompareToIV() {
    System.out.println("compareTo");
    OSMNode o = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2, 3, 3}, 1000000000L, 1000000000L);
    OSMNode instance = new OSMNode(1L, -6, 1L, 1L, 1, new int[]{1, 1, 2, 2, 3, 3}, 1000000000L, 1000000000L);
    assertTrue(instance.compareTo(o) > 0);
  }
  
  //-------------------

  @Test
  public void testGetId() {
    System.out.println("getId");
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{}, 1000000000L, 1000000000L);
    long expResult = 1L;
    long result = instance.getId();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetVersion() {
    System.out.println("getVersion");
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{}, 1000000000L, 1000000000L);
    int expResult = 1;
    int result = instance.getVersion();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetTimestamp() {
    System.out.println("getTimestamp");
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{}, 1000000000L, 1000000000L);
    long expResult = 1L;
    long result = instance.getTimestamp();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetChangeset() {
    System.out.println("getChangeset");
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{}, 1000000000L, 1000000000L);
    long expResult = 1L;
    long result = instance.getChangeset();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetUserId() {
    System.out.println("getUserId");
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{}, 1000000000L, 1000000000L);
    int expResult = 1;
    int result = instance.getUserId();
    assertEquals(expResult, result);
  }

  @Test
  public void testisVisible() {
    System.out.println("isVisible");
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{}, 1000000000L, 1000000000L);
    boolean expResult = true;
    boolean result = instance.isVisible();
    assertEquals(expResult, result);
  }

  @Test
  public void testisVisibleII() {
    System.out.println("isVisible");
    OSMNode instance = new OSMNode(1L, -1, 1L, 1L, 1, new int[]{}, 1000000000L, 1000000000L);
    boolean expResult = false;
    boolean result = instance.isVisible();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetTags() {
    System.out.println("getTags");
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{}, 1000000000L, 1000000000L);
    int[] expResult = new int[]{};
    int[] result = instance.getTags();
    Assert.assertArrayEquals(expResult, result);
  }

  @Test
  public void testHasTagKey() {
    System.out.println("hasTagKey");
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{}, 1000000000L, 1000000000L);
    boolean expResult = false;
    boolean result = instance.hasTagKey(1);
    assertEquals(expResult, result);
  }

  @Test
  public void testHasTagKeyII() {
    System.out.println("hasTagKey");
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2, 3, 3}, 1000000000L, 1000000000L);
    boolean expResult = true;
    boolean result = instance.hasTagKey(1);
    assertEquals(expResult, result);
  }

  @Test
  public void testHasTagKeyIII() {
    System.out.println("hasTagKey");
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{1, 2, 2, 2, 3, 3}, 1000000000L, 1000000000L);
    boolean expResult = false;
    boolean result = instance.hasTagKey(1, new int[]{2, 3});
    assertEquals(expResult, result);
  }

  @Test
  public void testHasTagKeyIV() {
    System.out.println("hasTagKey");
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2, 3, 3}, 1000000000L, 1000000000L);
    boolean expResult = true;
    boolean result = instance.hasTagKey(1, new int[]{2, 3});
    assertEquals(expResult, result);
  }

  @Test
  public void testHasTagKeyV() {
    System.out.println("hasTagKey");
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{2, 1, 3, 3}, 1000000000L, 1000000000L);
    boolean expResult = false;
    boolean result = instance.hasTagKey(1, new int[]{1, 3});
    assertEquals(expResult, result);
  }

  @Test
  public void testHasTagValue() {
    System.out.println("hasTagValue");
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{1, 2, 2, 3}, 1000000000L, 1000000000L);
    boolean expResult = false;
    boolean result = instance.hasTagValue(1, 1);
    assertEquals(expResult, result);
  }

  @Test
  public void testHasTagValueII() {
    System.out.println("hasTagValue");
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 3}, 1000000000L, 1000000000L);
    boolean expResult = true;
    boolean result = instance.hasTagValue(1, 1);
    assertEquals(expResult, result);
  }

  //--------------------

  @Test
  public void testEqualsToOSMNode() {

    long id = 123;
    int version = 1;
    long timestamp = 310172400000l;
    long changeset = 4444;
    int userId = 23;
    int[] tags = new int[]{1, 1, 2, 2, 3, 3};
    long longitude = 86809727l;
    long latitude = 494094984l;

    OSMNode a = new OSMNode(id, version, timestamp, changeset, userId, tags, longitude, latitude);
    OSMNode b = new OSMNode(id, version, timestamp, changeset, userId, tags, longitude, latitude);
    assertTrue(a.equalsTo(b));
  }

  @Test
  public void testCompareToV() {
    long id = 123;
    int version = 1;
    long timestamp = 310172400000l;
    long changeset = 4444;
    int userId = 23;
    int[] tags = new int[]{1, 1, 2, 2, 3, 3};
    long longitude = 86809727l;
    long latitude = 494094984l;

    OSMNode a = new OSMNode(id, version, timestamp, changeset, userId, tags, longitude, latitude);

    OSMNode b;

    b = new OSMNode(id, version + 2, timestamp, changeset, userId, tags, longitude, latitude);

    assertTrue(a.compareTo(b) < 0);

  }
}
