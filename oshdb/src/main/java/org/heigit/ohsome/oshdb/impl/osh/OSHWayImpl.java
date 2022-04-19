package org.heigit.ohsome.oshdb.impl.osh;

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
import org.heigit.ohsome.oshdb.osm.OSM;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayOutputWrapper;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayWrapper;

/**
 * An implementation of the {@link OSHWay} interface.
 */
public class OSHWayImpl extends OSHEntityImpl implements OSHWay, Iterable<OSMWay>, Serializable {

  private static final long serialVersionUID = 1L;

  private static final int CHANGED_REFS = 1 << 2;
  private static final byte HEADER_HAS_NO_NODES = 1 << 3;

  private final int[] nodeIndex;
  private final int nodeDataOffset;
  private final int nodeDataLength;

  public static OSHWayImpl instance(final byte[] data, final int offset, final int length) {
    return instance(data, offset, length, 0, 0, 0, 0);
  }

  /**
   * Creates an instances of {@code OSHWayImpl} from the given byte array.
   */
  public static OSHWayImpl instance(final byte[] data, final int offset, final int length,
      final long baseId, final long baseTimestamp, final int baseLongitude,
      final int baseLatitude) {

    var wrapper = ByteArrayWrapper.newInstance(data, offset, length);
    var commonProps = new CommonEntityProps(data, offset, length);
    readCommon(wrapper, commonProps, baseId, baseTimestamp, baseLongitude, baseLatitude);

    final int[] nodeIndex;
    final int nodeDataLength;
    if ((commonProps.getHeader() & HEADER_HAS_NO_NODES) == 0) {
      final int nodeIndexLength = wrapper.readU32();
      nodeIndex = new int[nodeIndexLength];
      var index = 0;
      for (var i = 0; i < nodeIndexLength; i++) {
        index = wrapper.readU32() + index;
        nodeIndex[i] = index;
      }
      nodeDataLength = wrapper.readU32();
    } else {
      nodeIndex = new int[0];
      nodeDataLength = 0;
    }
    final var nodeDataOffset = wrapper.getPos();
    commonProps.setDataOffset(nodeDataOffset + nodeDataLength);
    commonProps.setDataLength(
        commonProps.getLength() - (commonProps.getDataOffset() - commonProps.getOffset()));
    return new OSHWayImpl(commonProps, nodeIndex, nodeDataOffset, nodeDataLength);
  }

  private OSHWayImpl(final CommonEntityProps p, final int[] nodeIndex, final int nodeDataOffset,
      final int nodeDataLength) {
    super(p);
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
    var wrapper = ByteArrayWrapper.newInstance(data, dataOffset, dataLength);
    return new VersionIterator(wrapper, id, baseTimestamp, getNodes());
  }

  private transient List<OSHNode> nodes = null;

  @Override
  public List<OSHNode> getNodes() {
    if (nodes == null) {
      nodes = new ArrayList<>(nodeIndex.length);
      long lastId = 0;
      for (var index = 0; index < nodeIndex.length; index++) {
        int offset = nodeIndex[index];
        int length =
            (index < nodeIndex.length - 1 ? nodeIndex[index + 1] : nodeDataLength) - offset;
        OSHNode n = OSHNodeImpl.instance(data, nodeDataOffset + offset, length, lastId, 0,
            baseLongitude, baseLatitude);
        lastId = n.getId();
        nodes.add(n);
      }
    }
    return nodes;
  }

  public static OSHWay build(List<OSMWay> versions, Collection<OSHNode> nodes) {
    return build(versions, nodes, 0, 0, 0, 0);
  }

  /**
   * Creates a {@code OSHway} bases on the given list of way versions.
   */
  public static OSHWay build(List<OSMWay> versions, Collection<OSHNode> nodes, final long baseId,
      final long baseTimestamp, final int baseLongitude, final int baseLatitude) {
    ByteBuffer buffer =
        buildRecord(versions, nodes, baseId, baseTimestamp, baseLongitude, baseLatitude);
    return OSHWayImpl.instance(buffer.array(), 0, buffer.remaining(), baseId, baseTimestamp,
        baseLongitude, baseLatitude);
  }

  /**
   * Creates a {@code OSHway} bases on the given list of way versions,
   * but returns the underlying ByteBuffer instead of an instance of {@code OSHWay}.
   */
  public static ByteBuffer buildRecord(List<OSMWay> versions, Collection<OSHNode> nodes,
      final long baseId, final long baseTimestamp, final int baseLongitude,
      final int baseLatitude) {
    Collections.sort(versions, Collections.reverseOrder());
    ByteArrayOutputWrapper output = new ByteArrayOutputWrapper();

    OSMMember[] lastRefs = new OSMMember[0];

    int minLon = Integer.MAX_VALUE;
    int maxLon = Integer.MIN_VALUE;
    int minLat = Integer.MAX_VALUE;
    int maxLat = Integer.MIN_VALUE;

    Map<Long, Integer> nodeOffsets = new HashMap<>();
    int[] nodeByteArrayIndex = new int[nodes.size()];
    ByteArrayOutputWrapper nodeData = new ByteArrayOutputWrapper();
    int idx = 0;
    int offset = 0;
    long lastId = 0;
    for (OSHNode node : nodes) {
      final long nodeId = node.getId();

      nodeOffsets.put(node.getId(), idx);
      nodeByteArrayIndex[idx++] = offset;
      ByteBuffer buffer = OSHNodeImpl.buildRecord(OSHEntities.toList(node.getVersions()), lastId, 0,
          baseLongitude, baseLatitude);
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
      lastId = nodeId;
    }

    Builder builder = new Builder(output, baseTimestamp);

    for (OSMWay way : versions) {
      OSMEntity version = way;

      byte changed = 0;
      OSMMember[] refs = way.getMembers();
      if (version.isVisible() && !memberEquals(refs, lastRefs)) {
        changed |= CHANGED_REFS;
      }

      builder.build(version, changed);
      if ((changed & CHANGED_REFS) != 0) {
        long lastMemberId = 0;
        output.writeU32(refs.length);
        for (OSMMember ref : refs) {
          Integer refOffset = nodeOffsets.get(Long.valueOf(ref.getId()));
          if (refOffset == null) {
            output.writeU32(0);
            output.writeS64(ref.getId() - lastMemberId);
          } else {
            output.writeU32(refOffset.intValue() + 1);
          }
          lastMemberId = ref.getId();
        }
        lastRefs = refs;
      }
    }

    byte header = builder.getHeader(versions.size() > 1);
    if (nodes.isEmpty()) {
      header |= HEADER_HAS_NO_NODES;
    }

    var buffer = builder.writeCommon(header, versions.get(0).getId() - baseId,
        true,
        minLon - baseLongitude,
        minLat  - baseLatitude,
        maxLon, maxLat);

    if ((header & HEADER_HAS_NO_NODES) == 0) {
      buffer.writeU32(nodeByteArrayIndex.length);
      for (int i = 0; i < nodeByteArrayIndex.length; i++) {
        buffer.writeU32(nodeByteArrayIndex[i]);
      }
      buffer.writeU32(nodeData.length());
      buffer.writeByteArray(nodeData.array(), 0, nodeData.length());
    }

    buffer.writeByteArray(output.array(), 0, output.length());
    return ByteBuffer.wrap(buffer.array(), 0, buffer.length());
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

  private static class VersionIterator extends EntityVersionIterator<OSMWay> {
    private final List<OSHNode> nodes;
    private OSMMember[] members = new OSMMember[0];

    private VersionIterator(ByteArrayWrapper wrapper, long id, long baseTimestamp,
        List<OSHNode> nodes) {
      super(wrapper, id, baseTimestamp);
      this.nodes = nodes;
    }

    @Override
    protected OSMWay extension(byte changed) {
      if ((changed & CHANGED_REFS) != 0) {
        int size = wrapper.readU32();
        members = new OSMMember[size];
        var memberId = 0L;
        var memberOffset = 0;
        OSHEntity member = null;
        for (int i = 0; i < size; i++) {
          memberOffset = wrapper.readU32();
          if (memberOffset > 0) {
            member = nodes.get(memberOffset - 1);
            memberId = member.getId();

          } else {
            member = null;
            memberId = wrapper.readS64() + memberId;
          }
          members[i] = new OSMMember(memberId, OSMType.NODE, -1, member);
        }
      }

      return OSM.way(id, version, baseTimestamp + timestamp, changeset, userId, keyValues,
          members);
    }
  }

  private static class SerializationProxy extends OSHEntitySerializationProxy {
    public SerializationProxy(OSHWayImpl osh) {
      super(osh);
    }

    public SerializationProxy() {
      super(null);
    }

    @Override
    protected Object newInstance(byte[] data, long id, long baseTimestamp, int baseLongitude,
        int baseLatitude) {
      return OSHWayImpl.instance(data, 0, data.length, id, baseTimestamp, baseLongitude,
          baseLatitude);
    }
  }
}
