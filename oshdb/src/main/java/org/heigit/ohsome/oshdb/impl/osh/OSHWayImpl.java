package org.heigit.ohsome.oshdb.impl.osh;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.heigit.ohsome.oshdb.osh.OSHEntities;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osh.OSHWay;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.util.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.util.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayOutputWrapper;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayWrapper;

public class OSHWayImpl extends OSHEntityImpl implements OSHWay, Iterable<OSMWay>, Serializable {

  private static final long serialVersionUID = 1L;

  private static final int CHANGED_USER_ID = 1 << 0;
  private static final int CHANGED_TAGS = 1 << 1;
  private static final int CHANGED_REFS = 1 << 2;

  private static final int HEADER_MULTIVERSION = 1 << 0;
  private static final int HEADER_TIMESTAMPS_NOT_IN_ORDER = 1 << 1;
  private static final int HEADER_HAS_TAGS = 1 << 2;
  private static final byte HEADER_HAS_NO_NODES = 1 << 3;

  private final int[] nodeIndex;
  private final int nodeDataOffset;
  private final int nodeDataLength;

  public static OSHWayImpl instance(final byte[] data, final int offset, final int length)
      throws IOException {
    return instance(data, offset, length, 0, 0, 0, 0);
  }

  public static OSHWayImpl instance(final byte[] data, final int offset, final int length,
      final long baseId, final long baseTimestamp, final long baseLongitude,
      final long baseLatitude) throws IOException {

    ByteArrayWrapper wrapper = ByteArrayWrapper.newInstance(data, offset, length);
    final byte header = wrapper.readRawByte();

    final long minLon = baseLongitude + wrapper.readSInt64();
    final long maxLon = minLon + wrapper.readUInt64();
    final long minLat = baseLatitude + wrapper.readSInt64();
    final long maxLat = minLat + wrapper.readUInt64();

    final OSHDBBoundingBox bbox = new OSHDBBoundingBox(minLon, minLat, maxLon, maxLat);

    final int[] keys;
    if ((header & HEADER_HAS_TAGS) != 0) {
      final int size = wrapper.readUInt32();
      keys = new int[size];
      for (int i = 0; i < size; i++) {
        keys[i] = wrapper.readUInt32();
      }
    } else {
      keys = new int[0];
    }

    final long id = wrapper.readUInt64() + baseId;

    final int[] nodeIndex;
    final int nodeDataLength;
    if ((header & HEADER_HAS_NO_NODES) == 0) {
      final int nodeIndexLength = wrapper.readUInt32();
      nodeIndex = new int[nodeIndexLength];
      int index = 0;
      for (int i = 0; i < nodeIndexLength; i++) {
        index = wrapper.readUInt32() + index;
        nodeIndex[i] = index;
      }
      nodeDataLength = wrapper.readUInt32();
    } else {
      nodeIndex = new int[0];
      nodeDataLength = 0;
    }

    final int nodeDataOffset = wrapper.getPos();

    final int dataOffset = nodeDataOffset + nodeDataLength;
    final int dataLength = length - (dataOffset - offset);

    return new OSHWayImpl(data, offset, length, baseId, baseTimestamp, baseLongitude, baseLatitude,
        header, id, bbox, keys, dataOffset, dataLength, nodeIndex, nodeDataOffset, nodeDataLength);
  }

  private OSHWayImpl(final byte[] data, final int offset, final int length, final long baseId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude,
      final byte header, final long id, final OSHDBBoundingBox bbox, final int[] keys,
      final int dataOffset, final int dataLength, final int[] nodeIndex, final int nodeDataOffset,
      final int nodeDataLength) {
    super(data, offset, length, baseId, baseTimestamp, baseLongitude, baseLatitude, header, id,
        bbox, keys, dataOffset, dataLength);


    this.nodeIndex = nodeIndex;
    this.nodeDataOffset = nodeDataOffset;
    this.nodeDataLength = nodeDataLength;
  }

  @Override
  public Iterable<OSMWay> getVersions() {
    return this;
  }

  @Override
  public Iterator<OSMWay> iterator() {
    try {
      final List<OSHNode> nodes = this.getNodes();
      return new Iterator<OSMWay>() {
        ByteArrayWrapper wrapper = ByteArrayWrapper.newInstance(data, dataOffset, dataLength);

        int version = 0;
        long timestamp = 0;
        long changeset = 0;
        int userId = 0;
        int[] keyValues = new int[0];

        OSMMember[] members = new OSMMember[0];

        @Override
        public boolean hasNext() {
          return wrapper.hasLeft() > 0;
        }

        @Override
        public OSMWay next() {
          try {
            version = wrapper.readSInt32() + version;
            timestamp = wrapper.readSInt64() + timestamp;
            changeset = wrapper.readSInt64() + changeset;

            byte changed = wrapper.readRawByte();

            if ((changed & CHANGED_USER_ID) != 0) {
              userId = wrapper.readSInt32() + userId;
            }

            if ((changed & CHANGED_TAGS) != 0) {
              int size = wrapper.readUInt32();
              keyValues = new int[size];
              for (int i = 0; i < size; i++) {
                keyValues[i] = wrapper.readUInt32();
              }
            }

            if ((changed & CHANGED_REFS) != 0) {
              int size = wrapper.readUInt32();
              members = new OSMMember[size];
              long memberId = 0;
              int memberOffset = 0;
              OSHEntity member = null;
              for (int i = 0; i < size; i++) {
                memberOffset = wrapper.readUInt32();
                if (memberOffset > 0) {
                  member = nodes.get(memberOffset - 1);
                  memberId = member.getId();

                } else {
                  member = null;
                  memberId = wrapper.readSInt64() + memberId;
                }
                members[i] = new OSMMember(memberId, OSMType.NODE, -1, member);
              }
            }

            return new OSMWay(id, version, new OSHDBTimestamp(baseTimestamp + timestamp), changeset,
                userId, keyValues, members);

          } catch (IOException e) {
            e.printStackTrace();
          }
          return null;
        }
      };
    } catch (IOException e1) {
    }
    return Collections.emptyIterator();
  }

  public List<OSHNode> getNodes() throws IOException {
    List<OSHNode> nodes = new ArrayList<>(nodeIndex.length);
    long lastId = 0;
    for (int index = 0; index < nodeIndex.length; index++) {
      int offset = nodeIndex[index];
      int length =
          ((index < nodeIndex.length - 1) ? nodeIndex[index + 1] : nodeDataLength) - offset;
      OSHNode n = OSHNodeImpl.instance(data, nodeDataOffset + offset, length, lastId, 0, baseLongitude,
          baseLatitude);
      lastId = n.getId();
      nodes.add(n);
    }
    return nodes;
  }

  public static OSHWay build(List<OSMWay> versions, Collection<OSHNode> nodes) throws IOException {
    return build(versions, nodes, 0, 0, 0, 0);
  }
  
  public static OSHWay build(List<OSMWay> versions, Collection<OSHNode> nodes, final long baseId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude) throws IOException {
    ByteBuffer buffer = buildRecord(versions, nodes, baseId, baseTimestamp, baseLongitude, baseLatitude);
    return OSHWayImpl.instance(buffer.array(), 0, buffer.remaining(), baseId, baseTimestamp, baseLongitude, baseLatitude);
  }

  public static ByteBuffer buildRecord(List<OSMWay> versions, Collection<OSHNode> nodes, final long baseId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude)
      throws IOException {
    Collections.sort(versions, Collections.reverseOrder());
    ByteArrayOutputWrapper output = new ByteArrayOutputWrapper();

    OSMMember[] lastRefs = new OSMMember[0];

    long id = versions.get(0).getId();

    long minLon = Long.MAX_VALUE;
    long maxLon = Long.MIN_VALUE;
    long minLat = Long.MAX_VALUE;
    long maxLat = Long.MIN_VALUE;

    Map<Long, Integer> nodeOffsets = new HashMap<>();
    int[] nodeByteArrayIndex = new int[nodes.size()];
    ByteArrayOutputWrapper nodeData = new ByteArrayOutputWrapper();
    int idx = 0;
    int offset = 0;
    long lastId = 0;
    for (OSHNode node : nodes) {
      final long nodeId = node.getId();
      
      ByteBuffer buffer = OSHNodeImpl.buildRecord(OSHEntities.toList(node.getVersions()), lastId, 0, baseLongitude, baseLatitude);
      lastId = nodeId;
      nodeOffsets.put(node.getId(), idx);
      nodeByteArrayIndex[idx++] = offset;
      offset = buffer.remaining();
      nodeData.writeByteArray(buffer.array(), 0, buffer.remaining());

      Iterator<OSMNode> osmItr = node.getVersions().iterator();
      while (osmItr.hasNext()) {
        OSMNode osm = osmItr.next();
        if (osm.isVisible()) {
          minLon = Math.min(minLon, osm.getLon());
          maxLon = Math.max(maxLon, osm.getLon());

          minLat = Math.min(minLat, osm.getLat());
          maxLat = Math.max(maxLat, osm.getLat());
        }
      }
    }

    Builder builder = new Builder(output, baseTimestamp);

    for (OSMWay way : versions) {
      OSMEntity version = way;

      byte changed = 0;
      OSMMember[] refs = way.getRefs();
      if (version.isVisible() && !memberEquals(refs, lastRefs)) {
        changed |= CHANGED_REFS;
      }

      builder.build(version, changed);
      if ((changed & CHANGED_REFS) != 0) {
        long lastMemberId = 0;
        output.writeUInt32(refs.length);
        for (OSMMember ref : refs) {
          Integer refOffset = nodeOffsets.get(Long.valueOf(ref.getId()));
          if (refOffset == null) {
            output.writeUInt32(0);
            output.writeSInt64(ref.getId() - lastMemberId);
          } else {
            output.writeUInt32(refOffset.intValue() + 1);
          }
          lastMemberId = ref.getId();
        }
        lastRefs = refs;
      }
    }

    // store nodes
    ByteArrayOutputWrapper record = new ByteArrayOutputWrapper();

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
    if (nodes.isEmpty()) {
      header |= HEADER_HAS_NO_NODES;
    }

    record.writeByte(header);

    record.writeSInt64(minLon - baseLongitude);
    record.writeUInt64(maxLon - minLon);
    record.writeSInt64(minLat - baseLatitude);
    record.writeUInt64(maxLat - minLat);

    if ((header & HEADER_HAS_TAGS) != 0) {
      record.writeUInt32(builder.getKeySet().size());
      for (Integer key : builder.getKeySet()) {
        record.writeUInt32(key.intValue());
      }
    }

    record.writeUInt64(id - baseId);

    if ((header & HEADER_HAS_NO_NODES) == 0) {
      record.writeUInt32(nodeByteArrayIndex.length);
      for (int i = 0; i < nodeByteArrayIndex.length; i++) {
        record.writeUInt32(nodeByteArrayIndex[i]);
      }

      record.writeUInt32(nodeData.length());
      record.writeByteArray(nodeData.array(), 0, nodeData.length());
    }

    record.writeByteArray(output.array(), 0, output.length());
    return ByteBuffer.wrap(record.array(),0,record.length());
  }

  private static boolean memberEquals(OSMMember[] a, OSMMember[] b) {
    if (a.length != b.length) {
      return false;
    }
    for (int i = 0; i < a.length; i++) {
      if (a[i].getId() != b[i].getId()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return String.format("OSHWay %s", super.toString());
  }

  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  private static class SerializationProxy implements Externalizable {

    private final OSHWayImpl entity;
    private byte[] data;

    public SerializationProxy(OSHWayImpl entity) {
      this.entity = entity;
    }

    public SerializationProxy() {
      this.entity = null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeInt(entity.getLength());
      entity.writeTo(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      int length = in.readInt();
      data = new byte[length];
      in.readFully(data);
    }

    private Object readResolve() {
      try {
        return OSHWayImpl.instance(data, 0, data.length);
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }
  }
}
