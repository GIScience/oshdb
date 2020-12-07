package org.heigit.ohsome.oshdb.impl.osh;

import java.io.IOException;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Locale;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayOutputWrapper;

public abstract class OSHEntityImpl
    implements OSHEntity, Comparable<OSHEntity>{

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

      output.writeSInt64(version.getChangesetId() - lastChangeset);
      lastChangeset = version.getChangesetId();

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

  public OSHEntityImpl(final byte[] data, final int offset, final int length, final long baseId,
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

  @Override
  public int compareTo(OSHEntity o) {
    int c = Long.compare(getId(), o.getId());
    return c;
  }

  protected int writeTo(ObjectOutput out) throws IOException {
    out.write(data, offset, length);
    return length;
  }

  @Override
  public String toString() {
    Iterator<? extends OSMEntity> itr = getVersions().iterator();
    OSMEntity last, first;    
    last = first = itr.next();
    while(itr.hasNext())
      first = itr.next();
    
    return String.format(Locale.ENGLISH, "ID:%d Vmax:+%d+ Creation:%d BBox:(%f,%f),(%f,%f)", id,
        last.getVersion(),
        first.getTimestamp().getRawUnixTimestamp(),
        getBoundingBox().getMinLat(), getBoundingBox().getMinLon(), getBoundingBox().getMaxLat(),
        getBoundingBox().getMaxLon());
  }

}
