package org.heigit.ohsome.oshdb.impl.osh;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.heigit.ohsome.oshdb.OSHDBBoundable;
import org.heigit.ohsome.oshdb.osh.OSHEntities;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osh.OSHRelation;
import org.heigit.ohsome.oshdb.osh.OSHWay;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayOutputWrapper;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayWrapper;

public class OSHRelationImpl extends OSHEntityImpl
    implements OSHRelation, Iterable<OSMRelation>, Serializable {

  private static final long serialVersionUID = 1L;

  private static final int CHANGED_USER_ID = 1 << 0;
  private static final int CHANGED_TAGS = 1 << 1;
  private static final int CHANGED_MEMBERS = 1 << 2;

  private static final int HEADER_MULTIVERSION = 1 << 0;
  private static final int HEADER_TIMESTAMPS_NOT_IN_ORDER = 1 << 1;
  private static final int HEADER_HAS_TAGS = 1 << 2;
  private static final int HEADER_HAS_NODES = 1 << 3;
  private static final int HEADER_HAS_WAYS = 1 << 4;

  private final int[] nodeIndex;
  private final int nodeDataOffset;
  private final int nodeDataLength;

  private final int[] wayIndex;
  private final int wayDataOffset;
  private final int wayDataLength;

  public static OSHRelationImpl instance(final byte[] data, final int offset, final int length)
      throws IOException {
    return instance(data, offset, length, 0, 0, 0, 0);
  }

  /**
   * Creates an instances of {@code OSHRelationImpl} from the given byte array.
   */
  public static OSHRelationImpl instance(final byte[] data, final int offset, final int length,
      final long baseId, final long baseTimestamp, final long baseLongitude,
      final long baseLatitude) throws IOException {

    final ByteArrayWrapper wrapper = ByteArrayWrapper.newInstance(data, offset, length);
    final byte header = wrapper.readRawByte();

    final long minLon = baseLongitude + wrapper.readS64();
    final long maxLon = minLon + wrapper.readU64();
    final long minLat = baseLatitude + wrapper.readS64();
    final long maxLat = minLat + wrapper.readU64();

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

    final long id = wrapper.readU64() + baseId;
    final int[] nodeIndex;
    final int nodeDataLength;
    if ((header & HEADER_HAS_NODES) != 0) {
      final int nodeIndexLength = wrapper.readU32();
      nodeIndex = new int[nodeIndexLength];
      int index = 0;
      for (int i = 0; i < nodeIndexLength; i++) {
        index = wrapper.readU32() + index;
        nodeIndex[i] = index;
      }
      nodeDataLength = wrapper.readU32();
    } else {
      nodeIndex = new int[0];
      nodeDataLength = 0;
    }

    final int nodeDataOffset = wrapper.getPos();

    wrapper.seek(nodeDataOffset + nodeDataLength);

    final int[] wayIndex;
    final int wayDataLength;
    if ((header & HEADER_HAS_WAYS) != 0) {
      final int wayIndexLength = wrapper.readU32();
      wayIndex = new int[wayIndexLength];
      int index = 0;
      for (int i = 0; i < wayIndexLength; i++) {
        index = wrapper.readU32() + index;
        wayIndex[i] = index;
      }
      wayDataLength = wrapper.readU32();

    } else {
      wayIndex = new int[0];
      wayDataLength = 0;

    }
    final int wayDataOffset = wrapper.getPos();

    wrapper.seek(wayDataOffset + wayDataLength);

    final int dataOffset = wayDataOffset + wayDataLength;
    final int dataLength = length - (dataOffset - offset);

    return new OSHRelationImpl(data, offset, length,
        baseTimestamp, baseLongitude, baseLatitude,
        header, id, minLon, minLat, maxLon, maxLat, keys,
        dataOffset, dataLength,
        nodeIndex, nodeDataOffset, nodeDataLength,
        wayIndex, wayDataOffset, wayDataLength);
  }

  private OSHRelationImpl(final byte[] data, final int offset, final int length,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude, byte header,
      long id, long minLon, long minLat, long maxLon, long maxLat, int[] keys,
      final int dataOffset, final int dataLength, final int[] nodeIndex, final int nodeDataOffset,
      final int nodeDataLength, final int[] wayIndex, final int wayDataOffset,
      final int wayDataLength) {
    super(data, offset, length, baseTimestamp, baseLongitude, baseLatitude, header, id,
        minLon, minLat, maxLon, maxLat, keys, dataOffset, dataLength);

    this.nodeIndex = nodeIndex;
    this.nodeDataOffset = nodeDataOffset;
    this.nodeDataLength = nodeDataLength;

    this.wayIndex = wayIndex;
    this.wayDataOffset = wayDataOffset;
    this.wayDataLength = wayDataLength;

  }

  @Override
  public OSMType getType() {
    return OSMType.RELATION;
  }

  @Override
  public Iterable<OSMRelation> getVersions() {
    return this;
  }

  @Override
  public Iterator<OSMRelation> iterator() {
    try {
      final List<OSHNode> nodes = getNodes();
      final List<OSHWay> ways = getWays();

      return new Iterator<>() {
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
        public OSMRelation next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
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
              keyValues = new int[size];
              for (int i = 0; i < size;) {
                keyValues[i++] = wrapper.readU32();
                keyValues[i++] = wrapper.readU32();
              }
            }

            if ((changed & CHANGED_MEMBERS) != 0) {
              int size = wrapper.readU32();
              members = new OSMMember[size];
              long memberId = 0;
              int memberOffset = 0;
              OSMType memberType;
              int memberRole = 0;
              OSHEntity member = null;
              for (int i = 0; i < size; i++) {
                memberType = OSMType.fromInt(wrapper.readU32());
                switch (memberType) {
                  case NODE: {
                    memberOffset = wrapper.readU32();
                    if (memberOffset > 0) {
                      member = nodes.get(memberOffset - 1);
                      memberId = member.getId();

                    } else {
                      member = null;
                      memberId = wrapper.readS64() + memberId;
                    }
                    break;
                  }
                  case WAY: {
                    memberOffset = wrapper.readU32();
                    if (memberOffset > 0) {
                      member = ways.get(memberOffset - 1);
                      memberId = member.getId();

                    } else {
                      member = null;
                      memberId = wrapper.readS64() + memberId;
                    }
                    break;
                  }
                  case RELATION: {
                    memberId = wrapper.readS64() + memberId;
                    break;
                  }
                  default: {
                    memberId = wrapper.readS64() + memberId;
                    break;
                  }
                }

                memberRole = wrapper.readU32();
                members[i] = new OSMMember(memberId, memberType, memberRole, member);
              }
            }
            return new OSMRelation(id, version, baseTimestamp + timestamp,
                changeset, userId, keyValues, members);
          } catch (IOException e) {
            e.printStackTrace();
            // TODO: handle exception(s)
          }
          return null;
        }
      };
    } catch (IOException e) {
      return Collections.emptyIterator();
    }
  }

  @Override
  public List<OSHNode> getNodes() throws IOException {
    List<OSHNode> nodes = new ArrayList<>(nodeIndex.length);
    for (int index = 0; index < nodeIndex.length; index++) {
      int offset = nodeIndex[index];
      int length =
          (index < nodeIndex.length - 1 ? nodeIndex[index + 1] : nodeDataLength) - offset;
      OSHNode n = OSHNodeImpl.instance(data, nodeDataOffset + offset, length, 0, 0, baseLongitude,
          baseLatitude);
      nodes.add(n);
    }
    return nodes;
  }

  @Override
  public List<OSHWay> getWays() throws IOException {
    List<OSHWay> ways = new ArrayList<>(wayIndex.length);
    for (int index = 0; index < wayIndex.length; index++) {
      int offset = wayIndex[index];
      int length = (index < wayIndex.length - 1 ? wayIndex[index + 1] : wayDataLength) - offset;
      OSHWay w = OSHWayImpl.instance(data, wayDataOffset + offset, length, 0, 0, baseLongitude,
          baseLatitude);
      ways.add(w);
    }
    return ways;
  }

  public static OSHRelationImpl build(final List<OSMRelation> versions,
      final Collection<OSHNode> nodes,
      final Collection<OSHWay> ways) throws IOException {
    return build(versions, nodes, ways, 0, 0, 0, 0);
  }

  /**
   * Creates a {@code OSHRelation} bases on the given list of relation versions.
   */
  public static OSHRelationImpl build(final List<OSMRelation> versions,
      final Collection<OSHNode> nodes,
      final Collection<OSHWay> ways, final long baseId, final long baseTimestamp,
      final long baseLongitude, final long baseLatitude) throws IOException {
    ByteBuffer buffer =
        buildRecord(versions, nodes, ways, baseId, baseTimestamp, baseLongitude, baseLatitude);
    return OSHRelationImpl.instance(buffer.array(), 0, buffer.remaining(), baseId, baseTimestamp,
        baseLongitude, baseLatitude);
  }

  /**
   * Creates a {@code OSHRelation} bases on the given list of relation versions,
   * but returns the underlying ByteBuffer instead of an instance of {@code OSHRelation}.
   */
  public static ByteBuffer buildRecord(final List<OSMRelation> versions,
      final Collection<OSHNode> nodes,
      final Collection<OSHWay> ways, final long baseId, final long baseTimestamp,
      final long baseLongitude, final long baseLatitude) throws IOException {
    Collections.sort(versions, Collections.reverseOrder());

    OSMMember[] lastMembers = new OSMMember[0];

    long minLon = Long.MAX_VALUE;
    long maxLon = Long.MIN_VALUE;
    long minLat = Long.MAX_VALUE;
    long maxLat = Long.MIN_VALUE;

    Map<Long, Integer> nodeOffsets = new HashMap<>();
    int[] nodeByteArrayIndex = new int[nodes.size()];
    ByteArrayOutputWrapper nodeData = new ByteArrayOutputWrapper();
    int idx = 0;
    int offset = 0;
    for (OSHNode node : nodes) {
      OSHDBBoundable bbox = node;
      if (bbox.isValid()) {
        minLon = Math.min(minLon, bbox.getMinLonLong());
        maxLon = Math.max(maxLon, bbox.getMaxLonLong());
        minLat = Math.min(minLat, bbox.getMinLatLong());
        maxLat = Math.max(maxLat, bbox.getMaxLatLong());
      } else {
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

      ByteBuffer buffer = OSHNodeImpl.buildRecord(OSHEntities.toList(node.getVersions()), 0, 0,
          baseLongitude, baseLatitude);
      nodeOffsets.put(node.getId(), idx);
      nodeByteArrayIndex[idx++] = offset;
      offset = buffer.remaining();
      nodeData.writeByteArray(buffer.array(), 0, buffer.remaining());
    }

    Map<Long, Integer> wayOffsets = new HashMap<>();

    int[] wayByteArrayIndex = new int[ways.size()];
    ByteArrayOutputWrapper wayData = new ByteArrayOutputWrapper();
    idx = 0;
    offset = 0;
    for (OSHWay way : ways) {
      OSHDBBoundable bbox = way;
      minLon = Math.min(minLon, bbox.getMinLonLong());
      maxLon = Math.max(maxLon, bbox.getMaxLonLong());
      minLat = Math.min(minLat, bbox.getMinLatLong());
      maxLat = Math.max(maxLat, bbox.getMaxLatLong());

      ByteBuffer buffer = OSHWayImpl.buildRecord(OSHEntities.toList(way.getVersions()),
          way.getNodes(), 0, 0, baseLongitude, baseLatitude);
      wayOffsets.put(way.getId(), idx);
      wayByteArrayIndex[idx++] = offset;
      offset = buffer.remaining();
      wayData.writeByteArray(buffer.array(), 0, buffer.remaining());
    }

    ByteArrayOutputWrapper output = new ByteArrayOutputWrapper();
    Builder builder = new Builder(output, baseTimestamp);
    for (int i = 0; i < versions.size(); i++) {
      OSMRelation relation = versions.get(i);
      OSMEntity version = relation;

      byte changed = 0;
      OSMMember[] members = relation.getMembers();
      if (version.isVisible() && !Arrays.equals(members, lastMembers)) {
        changed |= CHANGED_MEMBERS;
      }

      builder.build(version, changed);

      if ((changed & CHANGED_MEMBERS) != 0) {
        long lastMemberId = 0;
        output.writeU32(members.length);
        for (OSMMember member : members) {
          output.writeU32(member.getType().intValue());
          switch (member.getType()) {
            case RELATION: {
              output.writeS64(member.getId() - lastMemberId);
              break;
            }
            case NODE: {
              Integer refOffset = nodeOffsets.get(Long.valueOf(member.getId()));
              if (refOffset == null) {
                output.writeU32(0);
                output.writeS64(member.getId() - lastMemberId);
              } else {
                output.writeU32(refOffset.intValue() + 1);
              }
              break;
            }
            case WAY: {
              Integer refOffset = wayOffsets.get(Long.valueOf(member.getId()));
              if (refOffset == null) {
                output.writeU32(0);
                output.writeS64(member.getId() - lastMemberId);
              } else {
                output.writeU32(refOffset.intValue() + 1);
              }
              break;
            }
            default: {
              break;
            }
          }

          output.writeU32(member.getRawRoleId());
          lastMemberId = member.getId();
        }
      }

    }

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

    if (!nodes.isEmpty()) {
      header |= HEADER_HAS_NODES;
    }
    if (!ways.isEmpty()) {
      header |= HEADER_HAS_WAYS;
    }

    ByteArrayOutputWrapper record = new ByteArrayOutputWrapper();
    record.writeByte(header);

    record.writeS64(minLon - baseLongitude);
    record.writeU64(maxLon - minLon);
    record.writeS64(minLat - baseLatitude);
    record.writeU64(maxLat - minLat);

    if ((header & HEADER_HAS_TAGS) != 0) {
      record.writeU32(builder.getKeySet().size());
      for (Integer key : builder.getKeySet()) {
        record.writeU32(key.intValue());
      }
    }

    long id = versions.get(0).getId();
    record.writeU64(id - baseId);

    if (!nodes.isEmpty()) {
      record.writeU32(nodeByteArrayIndex.length);
      for (int i = 0; i < nodeByteArrayIndex.length; i++) {
        record.writeU32(nodeByteArrayIndex[i]);
      }
      record.writeU32(nodeData.length());
      record.writeByteArray(nodeData.array(), 0, nodeData.length());
    }

    if (!ways.isEmpty()) {

      record.writeU32(wayByteArrayIndex.length);
      for (int i = 0; i < wayByteArrayIndex.length; i++) {
        record.writeU32(wayByteArrayIndex[i]);
      }
      record.writeU32(wayData.length());
      record.writeByteArray(wayData.array(), 0, wayData.length());
    }

    record.writeByteArray(output.array(), 0, output.length());
    return ByteBuffer.wrap(record.array(), 0, record.length());
  }

  public void writeTo(ByteArrayOutputWrapper out) throws IOException {
    out.writeByteArray(data, offset, length);
  }

  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  @Override
  public String toString() {
    return String.format("OSHRelation %s", super.toString());
  }

  private static class SerializationProxy implements Externalizable {

    private final OSHRelationImpl entity;
    private byte[] data;

    public SerializationProxy(OSHRelationImpl entity) {
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
        return OSHRelationImpl.instance(data, 0, data.length);
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }
  }

}
