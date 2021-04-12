package org.heigit.ohsome.oshdb.osm;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.heigit.ohsome.oshdb.util.OSHDBTimestamp;
import org.junit.Assert;
import org.junit.Test;

public class OSMRelationTest {

  public OSMRelationTest() {}

  @Test
  public void testGetMembers() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        new OSMRelation(1L, 1, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    OSMMember[] expResult = new OSMMember[] {part, part};
    OSMMember[] result = instance.getMembers();
    assertArrayEquals(expResult, result);
  }

  @Test
  public void testCompareTo() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        new OSMRelation(1L, 1, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    OSMRelation o =
        new OSMRelation(1L, 2, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    assertTrue(instance.compareTo(o) < 0);

    part = new OSMMember(1L, OSMType.WAY, 1);
    instance =
        new OSMRelation(1L, 1, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    o = new OSMRelation(1L, 1, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    assertTrue(instance.compareTo(o) == 0);

    part = new OSMMember(1L, OSMType.WAY, 1);
    instance =
        new OSMRelation(1L, 2, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    o = new OSMRelation(1L, 1, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    assertTrue(instance.compareTo(o) > 0);
  }

  // -----------------------
  @Test
  public void testGetId() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        new OSMRelation(1L, 2, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    long expResult = 1L;
    long result = instance.getId();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetVersion() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        new OSMRelation(1L, 2, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    int expResult = 2;
    int result = instance.getVersion();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetTimestamp() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        new OSMRelation(1L, 2, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    long expResult = 1L;
    long result = instance.getTimestamp().getEpochSecond();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetChangeset() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        new OSMRelation(1L, 2, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    long expResult = 1L;
    long result = instance.getChangesetId();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetUserId() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        new OSMRelation(1L, 2, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    int expResult = 1;
    int result = instance.getUserId();
    assertEquals(expResult, result);
  }

  @Test
  public void testisVisible() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        new OSMRelation(1L, 2, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    boolean expResult = true;
    boolean result = instance.isVisible();
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.WAY, 1);
    instance =
        new OSMRelation(1L, -2, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    expResult = false;
    result = instance.isVisible();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetTags() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        new OSMRelation(1L, 2, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    int[] expResult = new int[] {1, 1, 2, 2};
    int[] result = instance.getRawTags();
    Assert.assertArrayEquals(expResult, result);
  }

  @Test
  public void testHasTagKey() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        new OSMRelation(1L, 2, new OSHDBTimestamp(1L), 1L, 1, new int[] {}, new OSMMember[] {part, part});
    boolean expResult = false;
    boolean result = instance.hasTagKey(1);
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.WAY, 1);
    instance = new OSMRelation(1L, 1, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 1, 2, 2, 3, 3},
        new OSMMember[] {part, part});
    expResult = true;
    result = instance.hasTagKey(1);
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.WAY, 1);
    instance = new OSMRelation(1L, 1, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 2, 2, 2, 3, 3},
        new OSMMember[] {part, part});
    expResult = false;
    result = instance.hasTagKeyExcluding(1, new int[] {2, 3});
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.WAY, 1);
    instance = new OSMRelation(1L, 1, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 1, 2, 2, 3, 3},
        new OSMMember[] {part, part});
    expResult = true;
    result = instance.hasTagKeyExcluding(1, new int[] {2, 3});
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.WAY, 1);
    instance =
        new OSMRelation(1L, 1, new OSHDBTimestamp(1L), 1L, 1, new int[] {2, 1, 3, 3}, new OSMMember[] {part, part});
    expResult = false;
    result = instance.hasTagKeyExcluding(1, new int[] {1, 3});
    assertEquals(expResult, result);
  }

  @Test
  public void testHasTagValue() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        new OSMRelation(1L, 1, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 2, 2, 3}, new OSMMember[] {part, part});
    boolean expResult = false;
    boolean result = instance.hasTagValue(1, 1);
    assertEquals(expResult, result);
  }

  @Test
  public void testHasTagValueII() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        new OSMRelation(1L, 1, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 1, 2, 3}, new OSMMember[] {part, part});
    boolean expResult = true;
    boolean result = instance.hasTagValue(1, 1);
    assertEquals(expResult, result);
  }

  @Test
  public void testToString() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        new OSMRelation(1L, 2, new OSHDBTimestamp(1L), 1L, 1, new int[] {1, 2}, new OSMMember[] {part, part});
    String expResult =
        "Relation-> ID:1 V:+2+ TS:1 CS:1 VIS:true UID:1 TAGS:[1, 2] Mem:[T:WAY ID:1 R:1, T:WAY ID:1 R:1]";
    String result = instance.toString();
    assertEquals(expResult, result);
  }



}
