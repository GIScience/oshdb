package org.heigit.ohsome.oshdb.osm;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.OSHDBTemporal;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;

public abstract class OSMEntity implements OSHDBTemporal, Serializable {

  protected final long id;

  protected final int version;
  protected final long timestamp;
  protected final long changesetId;
  protected final int userId;
  protected final int[] tags;

  /**
   * Constructor for a OSMEntity. Holds the basic information, every OSM-Object has.
   *
   * @param id ID
   * @param version Version. Versions &lt;=0 define visible Entities, &gt;0 deleted Entities.
   * @param timestamp Timestamp in seconds since 01.01.1970 00:00:00 UTC.
   * @param changesetId Changeset-ID
   * @param userId UserID
   * @param tags An array of OSHDB key-value ids. The format is [KID1,VID1,KID2,VID2...KIDn,VIDn].
   */
  protected OSMEntity(final long id, final int version, final long timestamp,
      final long changesetId, final int userId, final int[] tags) {
    this.id = id;
    this.version = version;
    this.timestamp = timestamp;
    this.changesetId = changesetId;
    this.userId = userId;
    this.tags = tags;
  }

  public long getId() {
    return id;
  }

  public abstract OSMType getType();

  public int getVersion() {
    return Math.abs(version);
  }

  @Override
  public long getEpochSecond() {
    return timestamp;
  }

  public long getChangesetId() {
    return changesetId;
  }

  public int getUserId() {
    return userId;
  }

  public boolean isVisible() {
    return version >= 0;
  }

  /**
   * Returns a "view" of the current osm tags.
   */
  public Iterable<OSHDBTag> getTags() {
    return new Iterable<>() {
      @Nonnull
      @Override
      public Iterator<OSHDBTag> iterator() {
        return new Iterator<>() {
          int pos = 0;

          @Override
          public boolean hasNext() {
            return pos < tags.length;
          }

          @Override
          public OSHDBTag next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            return new OSHDBTag(tags[pos++], tags[pos++]);
          }
        };
      }
    };
  }

  @Deprecated(since = "0.7.0", forRemoval = true)
  public int[] getRawTags() {
    return tags;
  }

  public boolean hasTagKey(OSHDBTagKey key) {
    return this.hasTagKey(key.toInt());
  }

  /**
   * Test this {@code OSMEntity} if it contains a certain tag key(integer).
   */
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

  /**
   * Test for a certain key/value combination.
   */
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

  @Override
  public int hashCode() {
    return Objects.hash(getType(), id, version);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof OSMEntity)) {
      return false;
    }
    OSMEntity other = (OSMEntity) obj;
    return getType() == other.getType() && id == other.id && version == other.version;
  }

  @Override
  public String toString() {
    return String.format("ID:%d V:+%d+ TS:%d CS:%d VIS:%s UID:%d TAGS:%S", getId(), getVersion(),
        getEpochSecond(), getChangesetId(), isVisible(), getUserId(),
        Arrays.toString(getRawTags()));
  }
}
