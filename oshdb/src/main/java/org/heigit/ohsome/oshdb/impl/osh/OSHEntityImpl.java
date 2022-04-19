package org.heigit.ohsome.oshdb.impl.osh;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.OSHDBTags;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMCoordinates;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayOutputWrapper;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayWrapper;

public abstract class OSHEntityImpl implements OSHEntity, Comparable<OSHEntity>, Serializable {

  protected static final int CHANGED_USER_ID = 1 << 0;
  protected static final int CHANGED_TAGS = 1 << 1;

  protected static final int HEADER_MULTIVERSION = 1 << 0;
  protected static final int HEADER_TIMESTAMPS_NOT_IN_ORDER = 1 << 1;
  protected static final int HEADER_HAS_TAGS = 1 << 2;

  protected static final Comparator<OSMEntity> VERSION_REVERSE_ORDER = Comparator
      .comparingLong(OSMEntity::getId)
      .thenComparing(OSMEntity::getVersion)
      .reversed();

  protected static class Builder {

    private final ByteArrayOutputWrapper output;
    private final long baseTimestamp;

    int lastVersion = 0;
    long lastTimestamp = 0;
    long lastChangeset = 0;
    int lastUserId = 0;
    OSHDBTags lastKeyValues = OSHDBTags.empty();

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

    protected void build(OSMEntity version, byte changed) {
      int v = version.getVersion() * (!version.isVisible() ? -1 : 1);
      output.writeS32(v - lastVersion);
      lastVersion = v;

      output.writeS64(version.getEpochSecond() - lastTimestamp - baseTimestamp);
      if (!firstVersion && lastTimestamp < version.getEpochSecond()) {
        timestampsNotInOrder = true;
      }
      lastTimestamp = version.getEpochSecond();

      output.writeS64(version.getChangesetId() - lastChangeset);
      lastChangeset = version.getChangesetId();

      int userId = version.getUserId();
      if (userId != lastUserId) {
        changed |= CHANGED_USER_ID;
      }

      var keyValues = version.getTags();

      if (version.isVisible() && !keyValues.equals(lastKeyValues)) {
        changed |= CHANGED_TAGS;
      }

      output.writeByte(changed);

      if ((changed & CHANGED_USER_ID) != 0) {
        output.writeS32(userId - lastUserId);
        lastUserId = userId;
      }

      if ((changed & CHANGED_TAGS) != 0) {
        output.writeU32(keyValues.size() * 2);
        for (var kv : keyValues) {
          output.writeU32(kv.getKey());
          output.writeU32(kv.getValue());
          keySet.add(Integer.valueOf(kv.getKey()));
        }
        lastKeyValues = keyValues;
      }

      firstVersion = false;
    }

    protected byte getHeader(boolean multiVersion) {
      byte header = 0;
      if (multiVersion) {
        header |= HEADER_MULTIVERSION;
      }
      if (getTimestampsNotInOrder()) {
        header |= HEADER_TIMESTAMPS_NOT_IN_ORDER;
      }
      if (!getKeySet().isEmpty()) {
        header |= HEADER_HAS_TAGS;
      }
      return header;
    }

    protected ByteArrayOutputWrapper writeCommon(byte header, long id, boolean bbox, int minLon,
        int minLat, int maxLon, int maxLat) {
      var buffer = new ByteArrayOutputWrapper();
      buffer.writeByte(header);
      if (bbox) {
        buffer.writeS32(minLon);
        buffer.writeU64((long) maxLon - minLon);
        buffer.writeS32(minLat);
        buffer.writeU64((long) maxLat - minLat);
      }
      if ((header & HEADER_HAS_TAGS) != 0) {
        buffer.writeU32(getKeySet().size());
        for (Integer key : getKeySet()) {
          buffer.writeU32(key.intValue());
        }
      }
      buffer.writeU64(id);
      return buffer;
    }
  }

  protected final byte[] data;
  protected final int offset;
  protected final int length;
  protected final long baseId;
  protected final long baseTimestamp;
  protected final int baseLongitude;
  protected final int baseLatitude;

  protected final long id;
  protected final byte header;
  protected int minLon;
  protected int maxLon;
  protected int minLat;
  protected int maxLat;
  protected final int[] keys;
  protected final int dataOffset;
  protected final int dataLength;

  protected static class CommonEntityProps {
    private final byte[] data;
    private final int offset;
    private final int length;

    private long id;
    private byte header;
    private long baseId;
    private long baseTimestamp;
    private int baseLongitude;
    private int baseLatitude;
    private int minLon;
    private int minLat;
    private int maxLon;
    private int maxLat;
    private int[] keys;
    private int dataOffset;
    private int dataLength;

    public CommonEntityProps(byte[] data, int offset, int length) {
      this.data = data;
      this.offset = offset;
      this.length = length;
    }

    public long getId() {
      return id;
    }

    public void setId(long id) {
      this.id = id;
    }

    public byte getHeader() {
      return header;
    }

    public void setHeader(byte header) {
      this.header = header;
    }

    public long getBaseId() {
      return baseId;
    }

    public void setBaseId(long baseId) {
      this.baseId = baseId;
    }

    public int getBaseLongitude() {
      return baseLongitude;
    }

    public void setBaseLongitude(int baseLongitude) {
      this.baseLongitude = baseLongitude;
    }

    public long getBaseTimestamp() {
      return baseTimestamp;
    }

    public void setBaseTimestamp(long baseTimestamp) {
      this.baseTimestamp = baseTimestamp;
    }

    public int getBaseLatitude() {
      return baseLatitude;
    }

    public void setBaseLatitude(int baseLatitude) {
      this.baseLatitude = baseLatitude;
    }

    public int getMinLon() {
      return minLon;
    }

    public void setMinLon(int minLon) {
      this.minLon = minLon;
    }

    public int getMinLat() {
      return minLat;
    }

    public void setMinLat(int minLat) {
      this.minLat = minLat;
    }

    public int getMaxLon() {
      return maxLon;
    }

    public void setMaxLon(int maxLon) {
      this.maxLon = maxLon;
    }

    public int getMaxLat() {
      return maxLat;
    }

    public void setMaxLat(int maxLat) {
      this.maxLat = maxLat;
    }

    public int[] getKeys() {
      return keys;
    }

    public void setKeys(int[] keys) {
      this.keys = keys;
    }

    public int getDataOffset() {
      return dataOffset;
    }

    public void setDataOffset(int dataOffset) {
      this.dataOffset = dataOffset;
    }

    public int getDataLength() {
      return dataLength;
    }

    public void setDataLength(int dataLength) {
      this.dataLength = dataLength;
    }

    public byte[] getData() {
      return data;
    }

    public int getOffset() {
      return offset;
    }

    public int getLength() {
      return length;
    }
  }

  protected OSHEntityImpl(final CommonEntityProps p) {
    this.data = p.getData();
    this.offset = p.getOffset();
    this.length = p.getLength();

    this.baseId = p.getBaseId();
    this.baseTimestamp = p.getBaseTimestamp();
    this.baseLongitude = p.getBaseLongitude();
    this.baseLatitude = p.getBaseLatitude();

    this.header = p.getHeader();
    this.id = p.getId();
    this.minLon = p.getMinLon();
    this.minLat = p.getMinLat();
    this.maxLon = p.getMaxLon();
    this.maxLat = p.getMaxLat();

    this.keys = p.getKeys();
    this.dataOffset = p.getDataOffset();
    this.dataLength = p.getDataLength();
  }

  @Deprecated
  protected OSHEntityImpl(final byte[] data, final int offset, final int length,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude,
      final byte header, final long id, long minLon, long minLat, long maxLon, long maxLat,
      final int[] keys, final int dataOffset, final int dataLength) {
    this.data = data;
    this.offset = offset;
    this.length = length;

    this.baseId = 0;
    this.baseTimestamp = baseTimestamp;
    this.baseLongitude = Math.toIntExact(baseLongitude);
    this.baseLatitude = Math.toIntExact(baseLatitude);

    this.header = header;
    this.id = id;
    this.minLon = Math.toIntExact(minLon);
    this.minLat = Math.toIntExact(minLat);
    this.maxLon = Math.toIntExact(maxLon);
    this.maxLat = Math.toIntExact(maxLat);

    this.keys = keys;
    this.dataOffset = dataOffset;
    this.dataLength = dataLength;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), id);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof OSHEntity)) {
      return false;
    }
    OSHEntity other = (OSHEntity) obj;
    return getType() == other.getType() && id == other.getId();
  }

  /**
   * Return the underlying data/byte array and creates a copy if necessary.
   */
  public byte[] getData() {
    if (offset == 0 && length == data.length) {
      return data;
    }
    var result = new byte[length];
    System.arraycopy(data, offset, result, 0, length);
    return result;
  }

  @Override
  public long getId() {
    return id;
  }

  public int getLength() {
    return length;
  }

  @Override
  public int getMinLongitude() {
    return minLon;
  }

  @Override
  public int getMinLatitude() {
    return minLat;
  }

  @Override
  public int getMaxLongitude() {
    return maxLon;
  }

  @Override
  public int getMaxLatitude() {
    return maxLat;
  }

  @Override
  public Iterable<OSHDBTagKey> getTagKeys() {
    return new Iterable<>() {
      @Nonnull
      @Override
      public Iterator<OSHDBTagKey> iterator() {
        return new Iterator<>() {
          int pos = 0;

          @Override
          public boolean hasNext() {
            return pos < keys.length;
          }

          @Override
          public OSHDBTagKey next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            return new OSHDBTagKey(keys[pos++]);
          }
        };
      }
    };
  }

  @Override
  public boolean hasTagKey(OSHDBTagKey tag) {
    return this.hasTagKey(tag.toInt());
  }

  @Override
  public boolean hasTagKey(int key) {
    // todo: replace with binary search (keys are sorted)
    for (var i = 0; i < keys.length; i++) {
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
    return Long.compare(getId(), o.getId());
  }

  protected int writeTo(ObjectOutput out) throws IOException {
    out.writeLong(baseTimestamp);
    out.writeLong(baseLongitude);
    out.writeLong(baseLatitude);
    out.write(data, offset, length);
    return length;
  }

  @Override
  public String toString() {
    Iterator<? extends OSMEntity> itr = getVersions().iterator();
    OSMEntity last;
    OSMEntity first;
    last = first = itr.next();
    while (itr.hasNext()) {
      first = itr.next();
    }

    return String.format(Locale.ENGLISH, "ID:%d Vmax:+%d+ Creation:%d BBox:(%f,%f),(%f,%f)", id,
        last.getVersion(), first.getEpochSecond(),
        OSMCoordinates.toWgs84(minLat),
        OSMCoordinates.toWgs84(minLon),
        OSMCoordinates.toWgs84(maxLat),
        OSMCoordinates.toWgs84(maxLon));
  }

  protected static void readBbox(ByteArrayWrapper wrapper, CommonEntityProps p, int baseLongitude,
      int baseLatitude) {
    p.setMinLon(baseLongitude + wrapper.readS32());
    p.setMaxLon((int) (p.getMinLon() + wrapper.readU64()));
    p.setMinLat(baseLatitude + wrapper.readS32());
    p.setMaxLat((int) (p.getMinLat() + wrapper.readU64()));
  }

  protected static  void readBaseAndKeys(ByteArrayWrapper wrapper, CommonEntityProps p,
      final long baseId, final long baseTimestamp, final int baseLongitude,
      final int baseLatitude) {
    p.setBaseId(baseId);
    p.setBaseTimestamp(baseTimestamp);
    p.setBaseLongitude(baseLongitude);
    p.setBaseLatitude(baseLatitude);

    final int[] keys;
    if ((p.getHeader() & HEADER_HAS_TAGS) != 0) {
      final int size = wrapper.readU32();
      keys = new int[size];
      for (var i = 0; i < size; i++) {
        keys[i] = wrapper.readU32();
      }
    } else {
      keys = new int[0];
    }
    p.setKeys(keys);
    p.setId(wrapper.readU64() + baseId);
  }

  protected static void readCommon(ByteArrayWrapper wrapper, CommonEntityProps p,
      final long baseId, final long baseTimestamp, final int baseLongitude,
      final int baseLatitude) {
    p.setHeader(wrapper.readRawByte());
    readBbox(wrapper, p, baseLongitude, baseLatitude);
    readBaseAndKeys(wrapper, p, baseId, baseTimestamp, baseLongitude, baseLatitude);
  }

  protected abstract static class EntityVersionIterator<T extends OSMEntity>
      implements Iterator<T> {
    protected final ByteArrayWrapper wrapper;
    protected final long id;
    protected final long baseTimestamp;
    protected int version = 0;
    protected long timestamp = 0;
    protected long changeset = 0;
    protected int userId = 0;
    protected int[] keyValues = new int[0];

    protected  EntityVersionIterator(ByteArrayWrapper wrapper, long id, long baseTimestamp) {
      this.wrapper = wrapper;
      this.id = id;
      this.baseTimestamp = baseTimestamp;
    }

    @Override
    public boolean hasNext() {
      return wrapper.hasLeft() > 0;
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      version = wrapper.readS32() + version;
      timestamp = wrapper.readS64() + timestamp;
      changeset = wrapper.readS64() + changeset;

      var changed = wrapper.readRawByte();

      if ((changed & CHANGED_USER_ID) != 0) {
        userId = wrapper.readS32() + userId;
      }

      if ((changed & CHANGED_TAGS) != 0) {
        var size = wrapper.readU32();
        keyValues = new int[size];
        for (var i = 0; i < size; i += 2) {
          keyValues[i] = wrapper.readU32();
          keyValues[i + 1] = wrapper.readU32();
        }
      }

      return extension(changed);
    }

    protected abstract T extension(byte changed);
  }

  /**
   * Common serialization proxy for OSH(Node|Way|Relation)Impl.
   *
   */
  protected abstract static class OSHEntitySerializationProxy implements Externalizable {
    private final OSHEntityImpl osh;
    private long baseId;
    private long baseTimestamp;
    private int baseLongitude;
    private int baseLatitude;
    private byte[] data;

    protected OSHEntitySerializationProxy(OSHEntityImpl osh) {
      this.osh = osh;
    }

    protected OSHEntitySerializationProxy() {
      osh = null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeLong(osh.baseId);
      out.writeLong(osh.baseTimestamp);
      out.writeInt(osh.baseLongitude);
      out.writeInt(osh.baseLatitude);
      data = osh.getData();
      out.writeInt(data.length);
      out.write(data);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      baseId = in.readLong();
      baseTimestamp = in.readLong();
      baseLongitude = in.readInt();
      baseLatitude = in.readInt();
      var length = in.readInt();
      data = new byte[length];
      in.readFully(data);
    }

    protected Object readResolve() {
      return newInstance(data, baseId, baseTimestamp, baseLongitude, baseLatitude);
    }

    protected abstract Object newInstance(byte[] data, long baseId, long baseTimestamp,
        int baseLongitude, int baseLatitude);
  }
}
