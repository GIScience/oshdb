package org.heigit.ohsome.oshdb.osm;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.heigit.ohsome.oshdb.OSHDBTags;
import org.junit.jupiter.api.Test;

class OSMNodeTest {

  public OSMNodeTest() {}

  @Test
  void testGetLongitude() {
    OSMNode instance = OSM.node(1L, 1, 1L, 1L, 1, new int[] {}, 1000000000, 1);
    double expResult = 100.0;
    double result = instance.getLongitude();
    assertEquals(expResult, result, 0.0);
  }

  @Test
  void testGetLatitude() {
    OSMNode instance = OSM.node(1L, 1, 1L, 1L, 1, new int[] {}, 1000000000, 1000000000);
    double expResult = 100.0;
    double result = instance.getLatitude();
    assertEquals(expResult, result, 0.0);
  }

  @Test
  void testGetLon() {
    OSMNode instance = OSM.node(1L, 1, 1L, 1L, 1, new int[] {}, 1000000000, 1000000000);
    long expResult = 1000000000L;
    long result = instance.getLon();
    assertEquals(expResult, result);
  }

  @Test
  void testGetLat() {
    OSMNode instance = OSM.node(1L, 1, 1L, 1L, 1, new int[] {}, 1000000000, 1000000000);
    long expResult = 1000000000L;
    long result = instance.getLat();
    assertEquals(expResult, result);
  }

  @Test
  void testToString() {
    OSMNode instance = OSM.node(1L, 1, 1L, 1L, 1, new int[] {}, 1100000000, 100000000);
    String expResult = "NODE: ID:1 V:+1+ TS:1 CS:1 VIS:true UID:1 TAGS:[] 110.0000000:10.0000000";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

  @Test
  void testEquals() {
    OSMNode o =
        OSM.node(1L, 1, 1L, 1L, 1, new int[] {1, 1, 2, 2, 3, 3}, 1000000000, 1000000000);
    OSMNode instance =
        OSM.node(1L, 1, 1L, 1L, 1, new int[] {1, 1, 2, 2, 3, 3}, 1000000000, 1000000000);
    boolean expResult = true;
    boolean result = instance.equals(o);
    assertEquals(expResult, result);
  }

  @Test
  void testEquals2() {
    OSMNode o =
        OSM.node(2L, 1, 1L, 1L, 1, new int[] {1, 1, 2, 2, 3, 3}, 1000000000, 1000000000);
    OSMNode instance =
        OSM.node(1L, 1, 1L, 1L, 1, new int[] {1, 1, 2, 2, 3, 3}, 1000000000, 1000000000);
    boolean expResult = false;
    boolean result = instance.equals(o);
    assertEquals(expResult, result);
  }

  // -------------------
  @Test
  void testGetId() {
    OSMNode instance = OSM.node(1L, 1, 1L, 1L, 1, new int[] {}, 1000000000, 1000000000);
    long expResult = 1L;
    long result = instance.getId();
    assertEquals(expResult, result);
  }

  @Test
  void testGetVersion() {
    OSMNode instance = OSM.node(1L, 1, 1L, 1L, 1, new int[] {}, 1000000000, 1000000000);
    int expResult = 1;
    int result = instance.getVersion();
    assertEquals(expResult, result);
  }

  @Test
  void testGetTimestamp() {
    OSMNode instance = OSM.node(1L, 1, 1L, 1L, 1, new int[] {}, 1000000000, 1000000000);
    long expResult = 1L;
    long result = instance.getEpochSecond();
    assertEquals(expResult, result);
  }

  @Test
  void testGetChangeset() {
    OSMNode instance = OSM.node(1L, 1, 1L, 1L, 1, new int[] {}, 1000000000, 1000000000);
    long expResult = 1L;
    long result = instance.getChangesetId();
    assertEquals(expResult, result);
  }

  @Test
  void testGetUserId() {
    OSMNode instance = OSM.node(1L, 1, 1L, 1L, 1, new int[] {}, 1000000000, 1000000000);
    int expResult = 1;
    int result = instance.getUserId();
    assertEquals(expResult, result);
  }

  @Test
  void testisVisible() {
    OSMNode instance = OSM.node(1L, 1, 1L, 1L, 1, new int[] {}, 1000000000, 1000000000);
    boolean expResult = true;
    boolean result = instance.isVisible();
    assertEquals(expResult, result);
  }

  @Test
  void testisVisible2() {
    OSMNode instance = OSM.node(1L, -1, 1L, 1L, 1, new int[] {}, 1000000000, 1000000000);
    boolean expResult = false;
    boolean result = instance.isVisible();
    assertEquals(expResult, result);
  }

  @Test
  void testGetTags() {
    OSMNode instance = OSM.node(1L, 1, 1L, 1L, 1, new int[] {}, 1000000000, 1000000000);
    var expResult = OSHDBTags.empty();
    var result = instance.getTags();
    assertEquals(expResult, result);
  }

  @Test
  void testHasTagKey() {
    OSMNode instance = OSM.node(1L, 1, 1L, 1L, 1, new int[] {}, 1000000000, 1000000000);
    boolean expResult = false;
    boolean result = instance.getTags().hasTagKey(1);
    assertEquals(expResult, result);

    instance =
        OSM.node(1L, 1, 1L, 1L, 1, new int[] {1, 1, 2, 2, 3, 3}, 1000000000, 1000000000);
    expResult = true;
    result = instance.getTags().hasTagKey(1);
    assertEquals(expResult, result);

    instance =
        OSM.node(1L, 1, 1L, 1L, 1, new int[] {1, 2, 2, 2, 3, 3}, 1000000000, 1000000000);
    expResult = false;
    result = instance.getTags().hasTagKeyExcluding(1, new int[] {2, 3});
    assertEquals(expResult, result);

    instance =
        OSM.node(1L, 1, 1L, 1L, 1, new int[] {1, 1, 2, 2, 3, 3}, 1000000000, 1000000000);
    expResult = true;
    result = instance.getTags().hasTagKeyExcluding(1, new int[] {2, 3});
    assertEquals(expResult, result);

    instance = OSM.node(1L, 1, 1L, 1L, 1, new int[] {2, 1, 3, 3}, 1000000000, 1000000000);
    expResult = false;
    result = instance.getTags().hasTagKeyExcluding(1, new int[] {1, 3});
    assertEquals(expResult, result);
  }

  @Test
  void testHasTagValue() {
    OSMNode instance =
        OSM.node(1L, 1, 1L, 1L, 1, new int[] {1, 2, 2, 3}, 1000000000, 1000000000);
    boolean expResult = false;
    boolean result = instance.getTags().hasTagValue(1, 1);
    assertEquals(expResult, result);

    instance = OSM.node(1L, 1, 1L, 1L, 1, new int[] {1, 1, 2, 3}, 1000000000, 1000000000);
    expResult = true;
    result = instance.getTags().hasTagValue(1, 1);
    assertEquals(expResult, result);
  }

  // --------------------
  @Test
  void testEqualsToOSMNode() {
    long id = 123;
    int version = 1;
    long timestamp = 310172400000L;
    long changeset = 4444;
    int userId = 23;
    int[] tags = new int[] {1, 1, 2, 2, 3, 3};
    int longitude = 86809727;
    int latitude = 494094984;

    OSMNode a = OSM.node(id, version, timestamp, changeset, userId, tags, longitude, latitude);
    OSMNode b = OSM.node(id, version, timestamp, changeset, userId, tags, longitude, latitude);
    assertEquals(true, a.equals(b));
  }
}
