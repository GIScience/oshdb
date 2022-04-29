package org.heigit.ohsome.oshdb;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.IntStream;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;

/**
 * Collection class for OSHDBTag.
 *
 */
public abstract class OSHDBTags extends AbstractSet<OSHDBTag> implements Serializable {
  private static final OSHDBTags EMPTY = new IntArrayOSHDBTags(new int[0]);

  public static OSHDBTags empty() {
    return EMPTY;
  }

  public boolean hasTagKey(OSHDBTagKey key) {
    return hasTagKey(key.toInt());
  }

  /**
   * Test this {@code OSMEntity} if it contains a certain tag key(integer).
   */
  public abstract boolean hasTagKey(int key);

  /**
   * Tests if any a given key is present but ignores certain values. Useful when looking for example
   * "TagKey" != "no"
   *
   * @param key the key to search for
   * @param uninterestingValues list of values, that should return false although the key is
   *        actually present
   * @return true if the key is present and is NOT in a combination with the given values, false
   *         otherwise
   */
  public abstract boolean hasTagKeyExcluding(int key, int[] uninterestingValues);

  /**
   * Test for a certain key/value combination.
   */
  public abstract boolean hasTagValue(int key, int value);

  /**
   * KV based OSHDBTags.
   *
   * @param tags kv based array
   * @return OSHDBTags instance
   */
  public static OSHDBTags of(int[] tags) {
    return new IntArrayOSHDBTags(tags);
  }

  private static class IntArrayOSHDBTags extends OSHDBTags {

    private final int[] tags;

    private IntArrayOSHDBTags(int[] tags) {
      this.tags = tags;
    }

    @Override
    public int size() {
      return tags.length / 2;
    }

    @Override
    public Iterator<OSHDBTag> iterator() {
      return IntStream.range(0, tags.length / 2).map(i -> i * 2)
          .mapToObj(i -> new OSHDBTag(tags[i], tags[i + 1])).iterator();
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(tags);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj instanceof IntArrayOSHDBTags) {
        var other = (IntArrayOSHDBTags) obj;
        return Arrays.equals(tags, other.tags);
      }
      return super.equals(obj);
    }

    @Override
    public boolean hasTagKey(int key) {
      for (int i = 0; i < tags.length; i += 2) {
        if (tags[i] < key) {
          continue;
        }
        if (tags[i] == key) {
          return true;
        }
        if (tags[i] > key) {
          return false;
        }
      }
      return false;
    }

    @Override
    public boolean hasTagKeyExcluding(int key, int[] uninterestingValues) {
      for (int i = 0; i < tags.length; i += 2) {
        if (tags[i] < key) {
          continue;
        }
        if (tags[i] == key) {
          final int value = tags[i + 1];
          return !IntStream.of(uninterestingValues).anyMatch(x -> x == value);
        }
        if (tags[i] > key) {
          return false;
        }
      }
      return false;
    }

    @Override
    public boolean hasTagValue(int key, int value) {
      for (int i = 0; i < tags.length; i += 2) {
        if (tags[i] < key) {
          continue;
        }
        if (tags[i] == key) {
          return tags[i + 1] == value;
        }
        if (tags[i] > key) {
          return false;
        }
      }
      return false;
    }
  }

  public OSHDBTags view(int[] readTags) {
    return null;
  }
}
