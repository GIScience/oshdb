package org.heigit.ohsome.oshdb.osm;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.heigit.ohsome.oshdb.OSHDBTags;
import org.junit.jupiter.api.Test;

class OSMRelationTest {

  OSMRelationTest() {}

  @Test
  void testGetMembers() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        OSM.relation(1L, 1, 1L, 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    OSMMember[] expResult = new OSMMember[] {part, part};
    OSMMember[] result = instance.getMembers();
    assertArrayEquals(expResult, result);
  }

  // -----------------------
  @Test
  void testGetId() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        OSM.relation(1L, 2, 1L, 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    long expResult = 1L;
    long result = instance.getId();
    assertEquals(expResult, result);
  }

  @Test
  void testGetVersion() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        OSM.relation(1L, 2, 1L, 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    int expResult = 2;
    int result = instance.getVersion();
    assertEquals(expResult, result);
  }

  @Test
  void testGetTimestamp() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        OSM.relation(1L, 2, 1L, 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    long expResult = 1L;
    long result = instance.getEpochSecond();
    assertEquals(expResult, result);
  }

  @Test
  void testGetChangeset() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        OSM.relation(1L, 2, 1L, 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    long expResult = 1L;
    long result = instance.getChangesetId();
    assertEquals(expResult, result);
  }

  @Test
  void testGetUserId() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        OSM.relation(1L, 2, 1L, 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    int expResult = 1;
    int result = instance.getUserId();
    assertEquals(expResult, result);
  }

  @Test
  void testisVisible() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        OSM.relation(1L, 2, 1L, 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    boolean expResult = true;
    boolean result = instance.isVisible();
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.WAY, 1);
    instance =
        OSM.relation(1L, -2, 1L, 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    expResult = false;
    result = instance.isVisible();
    assertEquals(expResult, result);
  }

  @Test
  void testGetTags() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        OSM.relation(1L, 2, 1L, 1L, 1, new int[] {1, 1, 2, 2}, new OSMMember[] {part, part});
    var expResult = OSHDBTags.of(new int[] {1, 1, 2, 2});
    var result = instance.getTags();
    assertEquals(expResult, result);
  }

  @Test
  void testHasTagKey() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        OSM.relation(1L, 2, 1L, 1L, 1, new int[] {}, new OSMMember[] {part, part});
    boolean expResult = false;
    boolean result = instance.getTags().hasTagKey(1);
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.WAY, 1);
    instance = OSM.relation(1L, 1, 1L, 1L, 1, new int[] {1, 1, 2, 2, 3, 3},
        new OSMMember[] {part, part});
    expResult = true;
    result = instance.getTags().hasTagKey(1);
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.WAY, 1);
    instance = OSM.relation(1L, 1, 1L, 1L, 1, new int[] {1, 2, 2, 2, 3, 3},
        new OSMMember[] {part, part});
    expResult = false;
    result = instance.getTags().hasTagKeyExcluding(1, new int[] {2, 3});
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.WAY, 1);
    instance = OSM.relation(1L, 1, 1L, 1L, 1, new int[] {1, 1, 2, 2, 3, 3},
        new OSMMember[] {part, part});
    expResult = true;
    result = instance.getTags().hasTagKeyExcluding(1, new int[] {2, 3});
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.WAY, 1);
    instance =
        OSM.relation(1L, 1, 1L, 1L, 1, new int[] {2, 1, 3, 3}, new OSMMember[] {part, part});
    expResult = false;
    result = instance.getTags().hasTagKeyExcluding(1, new int[] {1, 3});
    assertEquals(expResult, result);
  }

  @Test
  void testHasTagValue() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        OSM.relation(1L, 1, 1L, 1L, 1, new int[] {1, 2, 2, 3}, new OSMMember[] {part, part});
    boolean expResult = false;
    boolean result = instance.getTags().hasTag(1, 1);
    assertEquals(expResult, result);
  }

  @Test
  void testHasTagValue2() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance =
        OSM.relation(1L, 1, 1L, 1L, 1, new int[] {1, 1, 2, 3}, new OSMMember[] {part, part});
    boolean expResult = true;
    boolean result = instance.getTags().hasTag(1, 1);
    assertEquals(expResult, result);
  }
}
