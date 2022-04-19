package org.heigit.ohsome.oshdb.impl.osh;

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
import org.heigit.ohsome.oshdb.OSHDBBoundable;
import org.heigit.ohsome.oshdb.osh.OSHEntities;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osh.OSHRelation;
import org.heigit.ohsome.oshdb.osh.OSHWay;
import org.heigit.ohsome.oshdb.osm.OSM;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayOutputWrapper;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayWrapper;

/**
 * An implementation of the {@link OSHRelation} interface.
 */
public class OSHRelationImpl extends OSHEntityImpl
    implements OSHRelation, Iterable<OSMRelation>, Serializable {

  private static final long serialVersionUID = 1L;

  private static final int CHANGED_MEMBERS = 1 << 2;

  private static final int HEADER_HAS_NODES = 1 << 3;
  private static final int HEADER_HAS_WAYS = 1 << 4;

  private final int[] nodeIndex;
  private final int nodeDataOffset;
  private final int nodeDataLength;

  private final int[] wayIndex;
  private final int wayDataOffset;
  private final int wayDataLength;

  public static OSHRelationImpl instance(final byte[] data, final int offset, final int length) {
    return instance(data, offset, length, 0, 0, 0, 0);
  }

  /**
   * Creates an instances of {@code OSHRelationImpl} from the given byte array.
   */
  public static OSHRelationImpl instance(final byte[] data, final int offset, final int length,
      final long baseId, final long baseTimestamp, final int baseLongitude,
      final int baseLatitude) {

    var wrapper = ByteArrayWrapper.newInstance(data, offset, length);
    var commonProps = new CommonEntityProps(data, offset, length);
    readCommon(wrapper, commonProps, baseId, baseTimestamp, baseLongitude, baseLatitude);
    final int[] nodeIndex;
    final int nodeDataLength;
    if ((commonProps.getHeader() & HEADER_HAS_NODES) != 0) {
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
    if ((commonProps.getHeader() & HEADER_HAS_WAYS) != 0) {
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
    commonProps.setDataOffset(wayDataOffset + wayDataLength);
    commonProps.setDataLength(
        commonProps.getLength() - (commonProps.getDataOffset() - commonProps.getOffset()));
    return new OSHRelationImpl(commonProps, nodeIndex, nodeDataOffset, nodeDataLength,
        wayIndex, wayDataOffset, wayDataLength);
  }

  private OSHRelationImpl(final CommonEntityProps p, final int[] nodeIndex,
      final int nodeDataOffset, final int nodeDataLength, final int[] wayIndex,
      final int wayDataOffset, final int wayDataLength) {
    super(p);
    this.nodeIndex = nodeIndex;
    this.nodeDataOffset = nodeDataOffset;
    this.nodeDataLength = nodeDataLength;

    this.wayIndex = wayIndex;
    this.wayDataOffset = wayDataOffset;
    this.wayDataLength = wayDataLength;
  }

  @Override
  public Iterable<OSMRelation> getVersions() {
    return this;
  }

  @Override
  public Iterator<OSMRelation> iterator() {
    var wrapper = ByteArrayWrapper.newInstance(data, dataOffset, dataLength);
    return new VersionIterator(wrapper, id, baseTimestamp, getNodes(), getWays());
  }

  private transient List<OSHNode> nodes = null;

  @Override
  public List<OSHNode> getNodes() {
    if (nodes == null) {
      nodes = new ArrayList<>(nodeIndex.length);
      for (int index = 0; index < nodeIndex.length; index++) {
        int offset = nodeIndex[index];
        int length =
            (index < nodeIndex.length - 1 ? nodeIndex[index + 1] : nodeDataLength) - offset;
        OSHNode n = OSHNodeImpl.instance(data, nodeDataOffset + offset, length, 0, 0, baseLongitude,
            baseLatitude);
        nodes.add(n);
      }
    }
    return nodes;
  }

  private transient List<OSHWay> ways = null;

  @Override
  public List<OSHWay> getWays() {
    if (ways == null) {
      ways = new ArrayList<>(wayIndex.length);
      for (int index = 0; index < wayIndex.length; index++) {
        int offset = wayIndex[index];
        int length = (index < wayIndex.length - 1 ? wayIndex[index + 1] : wayDataLength) - offset;
        OSHWay w = OSHWayImpl.instance(data, wayDataOffset + offset, length, 0, 0, baseLongitude,
            baseLatitude);
        ways.add(w);
      }
    }
    return ways;
  }

  public static OSHRelationImpl build(final List<OSMRelation> versions,
      final Collection<OSHNode> nodes,
      final Collection<OSHWay> ways) {
    return build(versions, nodes, ways, 0, 0, 0, 0);
  }

  /**
   * Creates a {@code OSHRelation} bases on the given list of relation versions.
   */
  public static OSHRelationImpl build(final List<OSMRelation> versions,
      final Collection<OSHNode> nodes,
      final Collection<OSHWay> ways, final long baseId, final long baseTimestamp,
      final int baseLongitude, final int baseLatitude) {
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
      final int baseLongitude, final int baseLatitude) {
    Collections.sort(versions, Collections.reverseOrder());

    var lastMembers = new OSMMember[0];

    int minLon = Integer.MAX_VALUE;
    int maxLon = Integer.MIN_VALUE;
    int minLat = Integer.MAX_VALUE;
    int maxLat = Integer.MIN_VALUE;

    Map<Long, Integer> nodeOffsets = new HashMap<>();
    int[] nodeByteArrayIndex = new int[nodes.size()];
    ByteArrayOutputWrapper nodeData = new ByteArrayOutputWrapper();
    int idx = 0;
    int offset = 0;
    for (OSHNode node : nodes) {
      OSHDBBoundable bbox = node;
      if (bbox.isValid()) {
        minLon = Math.min(minLon, bbox.getMinLongitude());
        maxLon = Math.max(maxLon, bbox.getMaxLongitude());
        minLat = Math.min(minLat, bbox.getMinLatitude());
        maxLat = Math.max(maxLat, bbox.getMaxLatitude());
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
      minLon = Math.min(minLon, bbox.getMinLongitude());
      maxLon = Math.max(maxLon, bbox.getMaxLongitude());
      minLat = Math.min(minLat, bbox.getMinLatitude());
      maxLat = Math.max(maxLat, bbox.getMaxLatitude());

      ByteBuffer buffer = OSHWayImpl.buildRecord(OSHEntities.toList(way.getVersions()),
          way.getNodes(), 0, 0, baseLongitude, baseLatitude);
      wayOffsets.put(way.getId(), idx);
      wayByteArrayIndex[idx++] = offset;
      offset = buffer.remaining();
      wayData.writeByteArray(buffer.array(), 0, buffer.remaining());
    }

    var output = new ByteArrayOutputWrapper();
    var builder = new Builder(output, baseTimestamp);
    for (int i = 0; i < versions.size(); i++) {
      OSMRelation relation = versions.get(i);
      OSMEntity version = relation;

      byte changed = 0;
      var members = relation.getMembers();
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

    byte header = builder.getHeader(versions.size() > 1);
    if (!nodes.isEmpty()) {
      header |= HEADER_HAS_NODES;
    }
    if (!ways.isEmpty()) {
      header |= HEADER_HAS_WAYS;
    }

    var buffer = builder.writeCommon(header, versions.get(0).getId() - baseId,
        true,
        minLon - baseLongitude,
        minLat  - baseLatitude,
        maxLon, maxLat);

    if (!nodes.isEmpty()) {
      buffer.writeU32(nodeByteArrayIndex.length);
      for (int i = 0; i < nodeByteArrayIndex.length; i++) {
        buffer.writeU32(nodeByteArrayIndex[i]);
      }
      buffer.writeU32(nodeData.length());
      buffer.writeByteArray(nodeData.array(), 0, nodeData.length());
    }

    if (!ways.isEmpty()) {
      buffer.writeU32(wayByteArrayIndex.length);
      for (int i = 0; i < wayByteArrayIndex.length; i++) {
        buffer.writeU32(wayByteArrayIndex[i]);
      }
      buffer.writeU32(wayData.length());
      buffer.writeByteArray(wayData.array(), 0, wayData.length());
    }

    buffer.writeByteArray(output.array(), 0, output.length());
    return ByteBuffer.wrap(buffer.array(), 0, buffer.length());
  }

  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  @Override
  public String toString() {
    return String.format("OSHRelation %s", super.toString());
  }

  private static class VersionIterator extends EntityVersionIterator<OSMRelation> {
    private final List<OSHNode> nodes;
    private final List<OSHWay> ways;

    private OSMMember[] members = new OSMMember[0];

    private VersionIterator(ByteArrayWrapper wrapper, long id, long baseTimestamp,
        List<OSHNode> nodes, List<OSHWay> ways) {
      super(wrapper, id, baseTimestamp);
      this.nodes = nodes;
      this.ways = ways;
    }

    @Override
    protected OSMRelation extension(byte changed) {
      if ((changed & CHANGED_MEMBERS) != 0) {
        int size = wrapper.readU32();
        members = new OSMMember[size];
        var member = new OSMMember(0, null, 0, null);
        for (int i = 0; i < size; i++) {
          member = readMembers(member);
          members[i] = member;
        }
      }
      return OSM.relation(id, version, baseTimestamp + timestamp, changeset, userId,
          keyValues, members);
    }

    private OSMMember readMembers(OSMMember m) {
      var memberType = OSMType.fromInt(wrapper.readU32());
      switch (memberType) {
        case NODE: {
          m = readMember(memberType, m, nodes);
          break;
        }
        case WAY: {
          m = readMember(memberType, m, ways);
          break;
        }
        case RELATION: {
          var memberId = wrapper.readS64() + m.getId();
          var memberRole = wrapper.readU32();
          m = new OSMMember(memberId, memberType, memberRole, null);
          break;
        }
        default:
          throw new IllegalStateException();
      }
      return m;
    }

    private <T extends OSHEntity> OSMMember readMember(OSMType memberType, OSMMember prev,
        List<T> list) {
      long memberId;
      var memberOffset = wrapper.readU32();
      OSHEntity member = null;
      if (memberOffset > 0) {
        member = list.get(memberOffset - 1);
        memberId = member.getId();

      } else {
        member = null;
        memberId = wrapper.readS64() + prev.getId();
      }
      var memberRole = wrapper.readU32();
      return new OSMMember(memberId, memberType, memberRole, member);
    }
  }

  private static class SerializationProxy extends OSHEntitySerializationProxy {
    public SerializationProxy(OSHRelationImpl osh) {
      super(osh);
    }

    public SerializationProxy() {
      super(null);
    }

    @Override
    protected Object newInstance(byte[] data, long id, long baseTimestamp, int baseLongitude,
        int baseLatitude) {
      return OSHRelationImpl.instance(data, 0, data.length, id, baseTimestamp, baseLongitude,
          baseLatitude);
    }
  }
}
