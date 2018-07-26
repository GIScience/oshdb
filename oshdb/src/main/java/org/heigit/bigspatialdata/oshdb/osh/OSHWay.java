package org.heigit.bigspatialdata.oshdb.osh;

import com.google.common.collect.Lists;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayWrapper;

public class OSHWay extends OSHEntity<OSMWay> implements Serializable {

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

  public static OSHWay instance(final byte[] data, final int offset, final int length) throws IOException {
    return instance(data, offset, length, 0, 0, 0, 0);
  }

  public static OSHWay instance(final byte[] data, final int offset, final int length, final long baseId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude) throws IOException {

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

    return new OSHWay(data, offset, length, baseId, baseTimestamp, baseLongitude, baseLatitude, header, id, bbox, keys,
        dataOffset, dataLength, nodeIndex, nodeDataOffset, nodeDataLength);
  }

  private OSHWay(final byte[] data, final int offset, final int length, final long baseId, final long baseTimestamp,
          final long baseLongitude, final long baseLatitude, final byte header, final long id, final OSHDBBoundingBox bbox,
          final int[] keys, final int dataOffset, final int dataLength, final int[] nodeIndex,
          final int nodeDataOffset, final int nodeDataLength) {
    super(data, offset, length, baseId, baseTimestamp, baseLongitude, baseLatitude, header, id, bbox, keys,
            dataOffset, dataLength);


    this.nodeIndex = nodeIndex;
    this.nodeDataOffset = nodeDataOffset;
    this.nodeDataLength = nodeDataLength;
  }

  @Override
  public OSMType getType() {
    return OSMType.WAY;
  }

  public List<OSMWay> getVersions() {
    List<OSMWay> versions = new ArrayList<>();
    this.forEach(versions::add);
    return versions;
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
              @SuppressWarnings("rawtypes")
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

            return new OSMWay(id, version, new OSHDBTimestamp(baseTimestamp + timestamp), changeset, userId, keyValues,
                    members);

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
      int length = ((index < nodeIndex.length - 1) ? nodeIndex[index + 1] : nodeDataLength) - offset;
      OSHNode n = OSHNode.instance(data, nodeDataOffset + offset, length, lastId, 0, baseLongitude, baseLatitude);
      lastId = n.getId();
      nodes.add(n);
    }
    return nodes;
  }

  @Override
  public OSHWay rebase(long baseId, long baseTimestamp, long baseLongitude, long baseLatitude) throws IOException {
    List<OSMWay> versions = getVersions();
    List<OSHNode> nodes = getNodes();
    return build(versions, nodes, baseId, baseTimestamp, baseLongitude, baseLatitude);
  }

  public static OSHWay build(List<OSMWay> versions, Collection<OSHNode> nodes) throws IOException {
    return build(versions, nodes, 0, 0, 0, 0);
  }

  public static OSHWay build(List<OSMWay> versions, Collection<OSHNode> nodes, final long baseId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude) throws IOException {
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
      node = node.rebase(lastId, 0, baseLongitude, baseLatitude);
      lastId = nodeId;
      nodeOffsets.put(node.getId(), idx);
      nodeByteArrayIndex[idx++] = offset;
      offset = node.getLength();
      node.writeTo(nodeData);

      Iterator<OSMNode> osmItr = node.iterator();
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

    return OSHWay.instance(record.array(), 0, record.length());
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

    private final OSHWay entity;
    private byte[] data;

    public SerializationProxy(OSHWay entity) {
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
        return OSHWay.instance(data, 0, data.length);
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }
  }

  @Override
  public List<OSHDBTimestamp> getModificationTimestamps(boolean recurse) {
    List<OSHDBTimestamp> wayTs = new ArrayList<>(this.iterator().next().getVersion());
    for (OSMWay osmWay : this) {
      wayTs.add(osmWay.getTimestamp());
    }
    if (!recurse) {
      return Lists.reverse(wayTs);
    }

    SortedSet<OSHDBTimestamp> result = new TreeSet<>(wayTs);
    int i = -1;
    for (OSMWay osmWay : this) {
      i++;
      if (!osmWay.isVisible()) continue;
      OSHDBTimestamp thisT = wayTs.get(i);
      OSHDBTimestamp nextT = i>0 ? wayTs.get(i-1) : new OSHDBTimestamp(Long.MAX_VALUE);
      OSMMember[] nds = osmWay.getRefs();
      for (OSMMember nd : nds) {
        OSHNode oshNode = (OSHNode)nd.getEntity();
        if (oshNode == null) continue;
        for (OSMNode osmNode : oshNode) {
          OSHDBTimestamp nodeTs = osmNode.getTimestamp();
          if (nodeTs.compareTo(nextT) >= 0) continue;
          if (nodeTs.compareTo(thisT) <= 0) break;
          result.add(nodeTs);
        }
      }
    }
    return new ArrayList<>(result);
  }

  @Override
  public Map<OSHDBTimestamp, Long> getChangesetTimestamps() {
    Map<OSHDBTimestamp, Long> result = new TreeMap<>();

    List<OSMWay> ways = this.getVersions();
    ways.forEach(osmWay -> {
      result.put(osmWay.getTimestamp(), osmWay.getChangeset());
    });

    // recurse way nodes
    try {
      this.getNodes().forEach(oshNode -> {
        if (oshNode != null)
          oshNode.getVersions().forEach(osmNode ->
              result.putIfAbsent(osmNode.getTimestamp(), osmNode.getChangeset())
          );
      });
    } catch (IOException e) {}

    return result;
  }
}
