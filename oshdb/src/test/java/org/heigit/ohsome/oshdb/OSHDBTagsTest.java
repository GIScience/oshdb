package org.heigit.ohsome.oshdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;
import org.junit.Test;

/**
 * Test class for OSHDBTags interface.
 *
 */
@SuppressWarnings("javadoc")
public class OSHDBTagsTest {
  int[] kvs = new int[] {1, 2, 2, 3, 4, 5};

  @Test
  public void testArrayHasTagKey() {
    var tags = OSHDBTags.of(kvs);

    var tagKey2 = new OSHDBTagKey(2);
    var tagKey3 = new OSHDBTagKey(3);

    assertTrue("tag key should exist", tags.hasTagKey(tagKey2));
    assertFalse("tag key should not exist", tags.hasTagKey(tagKey3));
    assertFalse("tag key should not exist", tags.hasTagKey(5));
  }

  @Test
  public void testArrayHasTagKeyExcluding() {
    var tags = OSHDBTags.of(kvs);

    assertTrue(tags.hasTagKeyExcluding(2, new int[] {1, 2, 4}));

    assertFalse(tags.hasTagKeyExcluding(2, new int[] {3}));
    assertFalse(tags.hasTagKeyExcluding(3, new int[0]));
    assertFalse(tags.hasTagKeyExcluding(5, new int[0]));

  }

  @Test
  public void testArrayHasTagValue() {
    var tags = OSHDBTags.of(kvs);

    assertTrue(tags.hasTagValue(1, 2));

    assertFalse(tags.hasTagValue(2, 2));
    assertFalse(tags.hasTagValue(3, 4));
    assertFalse(tags.hasTagValue(5, 6));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testImmutableAdd() {
    var tags = OSHDBTags.of(kvs);

    tags.add(new OSHDBTag(5, 6));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testImmutableRemove() {
    var tags = OSHDBTags.of(kvs);

    tags.removeIf(tag -> tag.getKey() == 2);
  }

  @Test
  public void testArrayEquality() {
    var tags = OSHDBTags.of(new int[] {2, 2, 4, 4});

    assertEquals(tags, tags);
    assertEquals(tags, OSHDBTags.of(new int[] {2, 2, 4, 4}));
    assertEquals(tags, Set.of(new OSHDBTag(2, 2), new OSHDBTag(4, 4)));

    assertNotEquals(tags, OSHDBTags.of(new int[] {1, 1, 4, 4}));
    assertNotEquals(tags, List.of(new OSHDBTag(2, 2), new OSHDBTag(4, 4)));
  }

}
