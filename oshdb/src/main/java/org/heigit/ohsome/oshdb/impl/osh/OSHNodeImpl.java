package org.heigit.ohsome.oshdb.impl.osh;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayOutputWrapper;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayWrapper;

public class OSHNodeImpl extends OSHEntityImpl implements OSHNode, Iterable<OSMNode>, Serializable {

  private static final long serialVersionUID = 1L;

  private static final int CHANGED_USER_ID = 1 << 0;
  private static final int CHANGED_TAGS = 1 << 1;
  private static final int CHANGED_LOCATION = 1 << 2;

  private static final int HEADER_MULTIVERSION = 1 << 0;
  private static final int HEADER_TIMESTAMPS_NOT_IN_ORDER = 1 << 1;
  private static final int HEADER_HAS_TAGS = 1 << 2;
  private static final int HEADER_HAS_BOUNDINGBOX = 1 << 3;

  public static OSHNodeImpl instance(final byte[] data, final int offset, final int length)
      throws IOException {
    return instance(data, offset, length, 0, 0, 0, 0);
  }

  /**
   * Creates an instances of {@code OSHNodeImpl} from the given byte array.
   */
  public static OSHNodeImpl instance(final byte[] data, final int offset, final int length,
      final long baseNodeId, final long baseTimestamp, final long baseLongitude,
      final long baseLatitude) throws IOException {

    ByteArrayWrapper wrapper = ByteArrayWrapper.newInstance(data, offset, length);
    // header holds data on bitlevel and can then be compared to stereotypical
    // bitcombinations (e.g. this.HEADER_HAS_TAGS)
    final byte header = wrapper.readRawByte();
    final long minLon;
    final long minLat;
    final long maxLon;
    final long maxLat;
    if ((header & HEADER_HAS_BOUNDINGBOX) != 0) {
      minLon = baseLongitude + wrapper.readS64();
      maxLon = minLon + wrapper.readU64();
      minLat = baseLatitude + wrapper.readS64();
      maxLat = minLat + wrapper.readU64();
    } else {
      minLon = 1;
      minLat = 1;
      maxLon = -1;
      maxLat = -1;
    }
    final int[] keys;
    if ((header & HEADER_HAS_TAGS) != 0) {
      final int size = wrapper.readU32();
      keys = new int[size];
      for (int i = 0; i < size; i++) {
        keys[i] = wrapper.readU32();
      }
    } else {
      keys = new int[0];
    }
    final long id = wrapper.readU64() + baseNodeId;
    final int dataOffset = wrapper.getPos();

    // TODO do we need dataLength?
    // TODO maybe better to store number of versions instead
    final int dataLength = length - (dataOffset - offset);

    return new OSHNodeImpl(data, offset, length, baseNodeId, baseTimestamp, baseLongitude,
        baseLatitude, header, id, minLon, minLat, maxLon, maxLat, keys, dataOffset, dataLength);
  }

  private OSHNodeImpl(final byte[] data, final int offset, final int length, final long baseNodeId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude,
      final byte header, final long id, long minLon, long minLat, long maxLon, long maxLat,
      final int[] keys, final int dataOffset, final int dataLength) {
    super(data, offset, length, baseNodeId, baseTimestamp, baseLongitude, baseLatitude, header, id,
        minLon, minLat, maxLon, maxLat, keys, dataOffset, dataLength);

    // correct bbox!
    if (!((minLon <= maxLon) && (minLat <= maxLat))) {
      minLon = Long.MAX_VALUE;
      maxLon = Long.MIN_VALUE;
      minLat = Long.MAX_VALUE;
      maxLat = Long.MIN_VALUE;
      for (OSMNode osm : this) {
        if (osm.isVisible()) {
          minLon = Math.min(minLon, osm.getLon());
          maxLon = Math.max(maxLon, osm.getLon());

          minLat = Math.min(minLat, osm.getLat());
          maxLat = Math.max(maxLat, osm.getLat());
        }
      }

      this.minLon = minLon;
      this.minLat = minLat;
      this.maxLon = maxLon;
      this.maxLat = maxLat;
    }
  }

  @Override
  public OSMType getType() {
    return OSMType.NODE;
  }

  @Override
  public Iterable<OSMNode> getVersions() {
    return this;
  }

  @Override
  public Iterator<OSMNode> iterator() {
    return new Iterator<OSMNode>() {
      ByteArrayWrapper wrapper = ByteArrayWrapper.newInstance(data, dataOffset, dataLength);

      int version = 0;
      long timestamp = 0;
      long changeset = 0;
      int userId = 0;
      int[] keyValues = new int[0];

      private long longitude = 0;
      private long latitude = 0;

      @Override
      public boolean hasNext() {
        return wrapper.hasLeft() > 0;
      }

      @Override
      public OSMNode next() {
        try {
          version = wrapper.readS32() + version;
          timestamp = wrapper.readS64() + timestamp;
          changeset = wrapper.readS64() + changeset;

          byte changed = wrapper.readRawByte();

          if ((changed & CHANGED_USER_ID) != 0) {
            userId = wrapper.readS32() + userId;
          }

          if ((changed & CHANGED_TAGS) != 0) {
            int size = wrapper.readU32();
            if (size < 0) {
              System.out.println("Something went wrong!");
            }
            keyValues = new int[size];
            for (int i = 0; i < size; i++) {
              keyValues[i] = wrapper.readU32();
            }
          }

          if ((changed & CHANGED_LOCATION) != 0) {
            longitude = wrapper.readS64() + longitude;
            latitude = wrapper.readS64() + latitude;
          }

          return new OSMNode(id, version, (baseTimestamp + timestamp), changeset, userId, keyValues,
              (version > 0) ? baseLongitude + longitude : 0,
              (version > 0) ? baseLatitude + latitude : 0);
        } catch (IOException e) {
          e.printStackTrace();
        }

        return null;
      }
    };
  }

  public static OSHNodeImpl build(List<OSMNode> versions) throws IOException {
    return build(versions, 0, 0, 0, 0);
  }

  /**
   * Creates a {@code OSHNode} bases on the given list of node versions.
   */
  public static OSHNodeImpl build(List<OSMNode> versions, final long baseId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude)
      throws IOException {
    ByteBuffer bb = buildRecord(versions, baseId, baseTimestamp, baseLongitude, baseLatitude);

    return OSHNodeImpl.instance(bb.array(), 0, bb.remaining(), baseId, baseTimestamp, baseLongitude,
        baseLatitude);
  }

  /**
   * Creates a {@code OSHNode} bases on the given list of node versions, but returns the underlying
   * ByteBuffer instead of an instance of {@code OSHNode}.
   */
  public static ByteBuffer buildRecord(List<OSMNode> versions, final long baseId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude)
      throws IOException {
    Collections.sort(versions, Collections.reverseOrder());

    long lastLongitude = baseLongitude;
    long lastLatitude = baseLatitude;

    long minLon = Long.MAX_VALUE;
    long maxLon = Long.MIN_VALUE;
    long minLat = Long.MAX_VALUE;
    long maxLat = Long.MIN_VALUE;

    ByteArrayOutputWrapper output = new ByteArrayOutputWrapper();
    Builder builder = new Builder(output, baseTimestamp);

    for (OSMNode node : versions) {
      OSMEntity version = node;

      byte changed = 0;

      if (version.isVisible()
          && ((node.getLon() != lastLongitude) || (node.getLat() != lastLatitude))) {
        changed |= CHANGED_LOCATION;
      }
      builder.build(version, changed);
      if ((changed & CHANGED_LOCATION) != 0) {
        output.writeS64((node.getLon() - baseLongitude) - (lastLongitude - baseLongitude));
        lastLongitude = node.getLon();
        output.writeS64((node.getLat() - baseLatitude) - (lastLatitude - baseLatitude));
        lastLatitude = node.getLat();

        minLon = Math.min(minLon, lastLongitude);
        maxLon = Math.max(maxLon, lastLongitude);

        minLat = Math.min(minLat, lastLatitude);
        maxLat = Math.max(maxLat, lastLatitude);
      }
    } // for versions

    byte header = 0;
    if (versions.size() > 1) {
      header |= HEADER_MULTIVERSION;
    }
    if (builder.getTimestampsNotInOrder()) {
      header |= HEADER_TIMESTAMPS_NOT_IN_ORDER;
    }
    if (builder.getKeySet().size() > 0) {
      header |= HEADER_HAS_TAGS;
    }

    if ((minLon != maxLon) || (minLat != maxLat)) {
      header |= HEADER_HAS_BOUNDINGBOX;
    }

    ByteArrayOutputWrapper record = new ByteArrayOutputWrapper();
    record.writeByte(header);
    if ((header & HEADER_HAS_BOUNDINGBOX) != 0) {
      record.writeS64(minLon - baseLongitude);
      record.writeU64(maxLon - minLon);
      record.writeS64(minLat - baseLatitude);
      record.writeU64(maxLat - minLat);
    }

    if ((header & HEADER_HAS_TAGS) != 0) {
      record.writeU32(builder.getKeySet().size());
      for (Integer key : builder.getKeySet()) {
        record.writeU32(key.intValue());
      }
    }

    long id = versions.get(0).getId();
    record.writeU64(id - baseId);
    record.writeByteArray(output.array(), 0, output.length());

    return ByteBuffer.wrap(record.array(), 0, record.length());
  }

  public boolean hasTags() {
    return (header & HEADER_HAS_TAGS) != 0;
  }

  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  private static class SerializationProxy implements Externalizable {

    private final OSHNodeImpl node;
    private byte[] data;

    public SerializationProxy(OSHNodeImpl node) {
      this.node = node;
    }

    public SerializationProxy() {
      this.node = null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeInt(node.getLength());
      node.writeTo(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      int length = in.readInt();
      data = new byte[length];
      in.readFully(data);
    }

    private Object readResolve() {
      try {
        return OSHNodeImpl.instance(data, 0, data.length);
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }
  }

  @Override
  public String toString() {
    return String.format("OSHNode %s", super.toString());
  }
}
