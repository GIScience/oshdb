package org.heigit.ohsome.oshdb.osm;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.heigit.ohsome.oshdb.OSHDBTags;
import org.junit.Assert;
import org.junit.Test;

public class OSMWayTest {

  public OSMWayTest() {}

  @Test
  public void testGetRefs() {
    OSMMember part = new OSMMember(1L, OSMType.NODE, 1);
    OSMWay instance = OSM.way(1L, 1, 1L, 1L, 1, new int[] {}, new OSMMember[] {part, part});
    OSMMember[] expResult = new OSMMember[] {part, part};
    OSMMember[] result = instance.getMembers();
    assertArrayEquals(expResult, result);

    instance = OSM.way(1L, 1, 1L, 1L, 1, new int[] {}, new OSMMember[] {});
    expResult = new OSMMember[] {};
    result = instance.getMembers();
    assertArrayEquals(expResult, result);

    instance = OSM.way(1L, 1, 1L, 1L, 1, new int[] {}, null);
    expResult = null;
    result = instance.getMembers();
    assertArrayEquals(expResult, result);
  }

  // ---------------
  @Test
  public void testGetId() {
    OSMMember part = new OSMMember(1L, OSMType.NODE, 1);
    OSMWay instance = OSM.way(1L, 1, 1L, 1L, 1, new int[] {}, new OSMMember[] {part, part});
    long expResult = 1L;
    long result = instance.getId();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetVersion() {
    OSMMember part = new OSMMember(1L, OSMType.NODE, 1);
    OSMWay instance = OSM.way(1L, 1, 1L, 1L, 1, new int[] {}, new OSMMember[] {part, part});
    int expResult = 1;
    int result = instance.getVersion();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetTimestamp() {
    OSMMember part = new OSMMember(1L, OSMType.NODE, 1);
    OSMWay instance = OSM.way(1L, 1, 1L, 1L, 1, new int[] {}, new OSMMember[] {part, part});
    long expResult = 1L;
    long result = instance.getEpochSecond();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetChangeset() {
    OSMMember part = new OSMMember(1L, OSMType.NODE, 1);
    OSMWay instance = OSM.way(1L, 1, 1L, 1L, 1, new int[] {}, new OSMMember[] {part, part});
    long expResult = 1L;
    long result = instance.getChangesetId();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetUserId() {
    OSMMember part = new OSMMember(1L, OSMType.NODE, 1);
    OSMWay instance = OSM.way(1L, 1, 1L, 1L, 1, new int[] {}, new OSMMember[] {part, part});
    int expResult = 1;
    int result = instance.getUserId();
    assertEquals(expResult, result);
  }

  @Test
  public void testisVisible() {
    OSMMember part = new OSMMember(1L, OSMType.NODE, 1);
    OSMWay instance = OSM.way(1L, 1, 1L, 1L, 1, new int[] {}, new OSMMember[] {part, part});
    boolean expResult = true;
    boolean result = instance.isVisible();
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.NODE, 1);
    instance = OSM.way(1L, -1, 1L, 1L, 1, new int[] {}, new OSMMember[] {part, part});
    expResult = false;
    result = instance.isVisible();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetTags() {
    OSMMember part = new OSMMember(1L, OSMType.NODE, 1);
    OSMWay instance = OSM.way(1L, 1, 1L, 1L, 1, new int[] {1, 1}, new OSMMember[] {part, part});
    var expResult = OSHDBTags.of(new int[] {1, 1});
    var result = instance.getTags();
    Assert.assertEquals(expResult, result);
  }

  @Test
  public void testHasTagKey() {
    OSMMember part = new OSMMember(1L, OSMType.NODE, 1);
    OSMWay instance = OSM.way(1L, 1, 1L, 1L, 1, new int[] {}, new OSMMember[] {part, part});
    boolean expResult = false;
    boolean result = instance.getTags().hasTagKey(1);
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.NODE, 1);
    instance =
        OSM.way(1L, 1, 1L, 1L, 1, new int[] {1, 1, 2, 2, 3, 3}, new OSMMember[] {part, part});
    expResult = true;
    result = instance.getTags().hasTagKey(1);
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.NODE, 1);
    instance =
        OSM.way(1L, 1, 1L, 1L, 1, new int[] {1, 2, 2, 2, 3, 3}, new OSMMember[] {part, part});
    expResult = false;
    result = instance.getTags().hasTagKeyExcluding(1, new int[] {2, 3});
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.NODE, 1);
    instance =
        OSM.way(1L, 1, 1L, 1L, 1, new int[] {1, 1, 2, 2, 3, 3}, new OSMMember[] {part, part});
    expResult = true;
    result = instance.getTags().hasTagKeyExcluding(1, new int[] {2, 3});
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.NODE, 1);
    instance = OSM.way(1L, 1, 1L, 1L, 1, new int[] {2, 1, 3, 3}, new OSMMember[] {part, part});
    expResult = false;
    result = instance.getTags().hasTagKeyExcluding(1, new int[] {1, 3});
    assertEquals(expResult, result);
  }

  @Test
  public void testHasTagValue() {
    OSMMember part = new OSMMember(1L, OSMType.NODE, 1);
    OSMWay instance =
        OSM.way(1L, 1, 1L, 1L, 1, new int[] {1, 2, 2, 3}, new OSMMember[] {part, part});
    boolean expResult = false;
    boolean result = instance.getTags().hasTagValue(1, 1);
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.NODE, 1);
    instance = OSM.way(1L, 1, 1L, 1L, 1, new int[] {1, 1, 2, 3}, new OSMMember[] {part, part});
    expResult = true;
    result = instance.getTags().hasTagValue(1, 1);
    assertEquals(expResult, result);
  }

}
