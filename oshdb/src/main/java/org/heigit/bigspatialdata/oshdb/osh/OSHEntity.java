package org.heigit.bigspatialdata.oshdb.osh;

import java.io.IOException;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTagKey;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;

public abstract class OSHEntity<OSM extends OSMEntity>
    implements Comparable<OSHEntity<OSM>>, Iterable<OSM> {

  public static class Builder {

    private static final int CHANGED_USER_ID = 1 << 0;
    private static final int CHANGED_TAGS = 1 << 1;

    private final ByteArrayOutputWrapper output;
    private final long baseTimestamp;

    int lastVersion = 0;
    long lastTimestamp = 0;
    long lastChangeset = 0;
    int lastUserId = 0;
    int[] lastKeyValues = new int[0];

    SortedSet<Integer> keySet = new TreeSet<>();

    boolean firstVersion = true;
    boolean timestampsNotInOrder = false;

    public Builder(final ByteArrayOutputWrapper output, final long baseTimestamp) {
      this.output = output;
      this.baseTimestamp = baseTimestamp;
    }

    public boolean getTimestampsNotInOrder() {
      return timestampsNotInOrder;
    }

    public SortedSet<Integer> getKeySet() {
      return keySet;
    }

    public void build(OSMEntity version, byte changed) throws IOException {
      int v = (version.getVersion() * (!version.isVisible() ? -1 : 1));
      output.writeSInt32(v - lastVersion);
      lastVersion = v;

      output.writeSInt64(
          (version.getTimestamp().getRawUnixTimestamp() - lastTimestamp) - baseTimestamp);
      if (!firstVersion && lastTimestamp < version.getTimestamp().getRawUnixTimestamp())
        timestampsNotInOrder = true;
      lastTimestamp = version.getTimestamp().getRawUnixTimestamp();

      output.writeSInt64(version.getChangeset() - lastChangeset);
      lastChangeset = version.getChangeset();

      int userId = version.getUserId();
      if (userId != lastUserId)
        changed |= CHANGED_USER_ID;

      int[] keyValues = version.getRawTags();

      if (version.isVisible() && !Arrays.equals(keyValues, lastKeyValues)) {
        changed |= CHANGED_TAGS;
      }

      output.writeByte(changed);

      if ((changed & CHANGED_USER_ID) != 0) {
        output.writeSInt32(userId - lastUserId);
        lastUserId = userId;
      }

      if ((changed & CHANGED_TAGS) != 0) {
        output.writeUInt32(keyValues.length);
        for (int kv = 0; kv < keyValues.length; kv++) {
          output.writeUInt32(keyValues[kv]);
          if (kv % 2 == 0) // only keys
            keySet.add(Integer.valueOf(keyValues[kv]));
        }
        lastKeyValues = keyValues;
      }

      firstVersion = false;
    }

  }

  protected final byte[] data;
  protected final int offset;
  protected final int length;
  protected final long baseTimestamp;
  protected final long baseLongitude;
  protected final long baseLatitude;

  protected final long id;
  protected final byte header;
  protected final OSHDBBoundingBox bbox;
  protected final int[] keys;
  protected final int dataOffset;
  protected final int dataLength;

  public OSHEntity(final byte[] data, final int offset, final int length, final long baseId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude,
      final byte header, final long id, final OSHDBBoundingBox bbox, final int[] keys,
      final int dataOffset, final int dataLength) {
    this.data = data;
    this.offset = offset;
    this.length = length;

    this.baseTimestamp = baseTimestamp;
    this.baseLongitude = baseLongitude;
    this.baseLatitude = baseLatitude;

    this.header = header;
    this.id = id;
    this.bbox = bbox;
    this.keys = keys;
    this.dataOffset = dataOffset;
    this.dataLength = dataLength;
  }

  public byte[] getData() {
    if (offset == 0 && length == data.length) {
      return data;
    }
    byte[] result = new byte[length];
    System.arraycopy(data, offset, result, 0, length);
    return result;
  }

  public long getId() {
    return id;
  }

  public abstract OSMType getType();

  public int getLength() {
    return length;
  }

  public OSHDBBoundingBox getBoundingBox() {
    return bbox;
  }

  public Iterable<OSHDBTagKey> getTagKeys() {
    return new Iterable<OSHDBTagKey>() {
      @Nonnull
      @Override
      public Iterator<OSHDBTagKey> iterator() {
        return new Iterator<OSHDBTagKey>() {
          int i = 0;

          @Override
          public boolean hasNext() {
            return i < keys.length;
          }

          @Override
          public OSHDBTagKey next() {
            return new OSHDBTagKey(keys[i++]);
          }
        };
      }
    };
  }

  public int[] getRawTagKeys() {
    return keys;
  }

  public abstract List<OSM> getVersions();

  public OSM getLatest() {
    return iterator().next();
  }

  /*
   * byTimestamps is assumed to be presorted, otherwise output is undetermined
   */
  @Deprecated
  public SortedMap<OSHDBTimestamp, OSM> getByTimestamps(List<OSHDBTimestamp> byTimestamps) {
    return OSHEntities.getByTimestamps(this.getVersions(), byTimestamps);
  }

  @Deprecated
  public Map<OSHDBTimestamp, OSM> getByTimestamps() { // todo: name of method?
    return OSHEntities.getByTimestamps(this.getVersions());
  }

  @Deprecated
  public OSM getByTimestamp(OSHDBTimestamp timestamp) {
    return OSHEntities.getByTimestamp(this.getVersions(), timestamp);
  }

  @Deprecated
  public List<OSM> getBetweenTimestamps(final OSHDBTimestamp t1, final OSHDBTimestamp t2) {
    return OSHEntities.getBetweenTimestamps(this.getVersions(), t1, t2);
  }

  public boolean hasTagKey(OSHDBTagKey tag) {
    return this.hasTagKey(tag.toInt());
  }

  public boolean hasTagKey(int key) {
    // todo: replace with binary search (keys are sorted)
    for (int i = 0; i < keys.length; i++) {
      if (keys[i] == key) {
        return true;
      }
      if (keys[i] > key) {
        break;
      }
    }
    return false;
  }

  public abstract OSHEntity<OSM> rebase(long baseId, long baseTimestamp, long baseLongitude,
      long baseLatitude) throws IOException;

  @Override
  public int compareTo(OSHEntity<OSM> o) {
    int c = Long.compare(id, o.id);
    return c;
  }

  public void writeTo(ByteArrayOutputWrapper out) throws IOException {
    out.writeByteArray(data, offset, length);
  }

  public int writeTo(ObjectOutput out) throws IOException {
    out.write(data, offset, length);
    return length;
  }

  /**
   * Returns all timestamps at which this entity (or one or more of its child entities) has been
   * modified.
   *
   * @return a list of timestamps where this entity has been modified
   * @deprecated use {@link OSHEntities#getModificationTimestamps(OSHEntity)}
   */
  @Deprecated
  public List<OSHDBTimestamp> getModificationTimestamps() {
    return OSHEntities.getModificationTimestamps(this);
  }

  /**
   * Returns the list of timestamps at which this entity was modified.
   *
   * If the parameter "recurse" is set to true, it will also include modifications of the object's
   * child elements (useful to find out when the geometry of this object has been altered).
   *
   * @param recurse specifies if times of modifications of child entities should also be returned or
   *        not
   * @return a list of timestamps where this entity has been modified
   * @deprecated use {@link OSHEntities#getModificationTimestamps(OSHEntity, boolean)}
   */
  @Deprecated
  public List<OSHDBTimestamp> getModificationTimestamps(boolean recurse) {
    return OSHEntities.getModificationTimestamps(this, recurse);
  }

  /**
   * Returns all timestamps at which this entity (or one or more of its child entities) has been
   * modified and matches a given condition/filter.
   *
   * @param osmEntityFilter only timestamps for which the entity matches this filter are returned
   * @return a list of timestamps where this entity has been modified
   * @deprecated use {@link OSHEntities#getModificationTimestamps(OSHEntity, Predicate)}
   */
  @Deprecated
  public List<OSHDBTimestamp> getModificationTimestamps(Predicate<OSMEntity> osmEntityFilter) {
    return OSHEntities.getModificationTimestamps(this, osmEntityFilter);
  }

  /**
   * Returns all timestamps at which this entity (or one or more of its child entities) has been
   * modified and matches a given condition/filter.
   *
   * Consecutive modifications from a single changeset are grouped together (only the last
   * modification timestamp of the corresponding changeset is considered). This can reduce the
   * amount of geometry modifications by a lot (e.g. when sequential node uploads of a way
   * modification causes many intermediate modification states), making results more
   * "accurate"/comparable as well as allowing faster processing of geometries.
   *
   * @param osmEntityFilter only timestamps for which the entity matches this filter are returned
   * @param changesetTimestamps association between timestamps and changeset-ids, can be obtained
   *        from oshEntity by calling {@link #getChangesetTimestamps}.
   * @return a list of timestamps where this entity has been modified
   * @deprecated use {@link OSHEntities#getModificationTimestamps(OSHEntity, Predicate, Map)}
   */
  @Deprecated
  public List<OSHDBTimestamp> getModificationTimestamps(Predicate<OSMEntity> osmEntityFilter,
      Map<OSHDBTimestamp, Long> changesetTimestamps) {
    return OSHEntities.getModificationTimestamps(this, osmEntityFilter, changesetTimestamps);
  }

  /**
   * Returns the changeset ids which correspond to modifications of this entity.
   *
   * Used internally to group modifications by changeset.
   *
   * @return a map between timestamps and changeset ids
   * @deprecated use {@link OSHEntities#getChangesetTimestamps(OSHEntity)}
   */
  @Deprecated
  public Map<OSHDBTimestamp, Long> getChangesetTimestamps() {
    return OSHEntities.getChangesetTimestamps(this);
  }

  @Override
  public String toString() {
    return String.format(Locale.ENGLISH, "ID:%d Vmax:+%d+ Creation:%d BBox:(%f,%f),(%f,%f)", id,
        getVersions().get(0).getVersion(),
        getVersions().get(getVersions().size() - 1).getTimestamp().getRawUnixTimestamp(),
        getBoundingBox().getMinLat(), getBoundingBox().getMinLon(), getBoundingBox().getMaxLat(),
        getBoundingBox().getMaxLon());
  }

}
