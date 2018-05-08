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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayWrapper;

public class OSHRelation extends OSHEntity<OSMRelation> implements Serializable {

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

  public static OSHRelation instance(final byte[] data, final int offset, final int length) throws IOException {
    return instance(data, offset, length, 0, 0, 0, 0);
  }

  public static OSHRelation instance(final byte[] data, final int offset, final int length, final long baseId,
          final long baseTimestamp, final long baseLongitude, final long baseLatitude) throws IOException {

    final ByteArrayWrapper wrapper = ByteArrayWrapper.newInstance(data, offset, length);
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
    if ((header & HEADER_HAS_NODES) != 0) {
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

    wrapper.seek(nodeDataOffset + nodeDataLength);

    final int[] wayIndex;
    final int wayDataLength;
    if ((header & HEADER_HAS_WAYS) != 0) {
      final int wayIndexLength = wrapper.readUInt32();
      wayIndex = new int[wayIndexLength];
      int index = 0;
      for (int i = 0; i < wayIndexLength; i++) {
        index = wrapper.readUInt32() + index;
        wayIndex[i] = index;
      }
      wayDataLength = wrapper.readUInt32();

    } else {
      wayIndex = new int[0];
      wayDataLength = 0;

    }
    final int wayDataOffset = wrapper.getPos();

    wrapper.seek(wayDataOffset + wayDataLength);

    final int dataOffset = wayDataOffset + wayDataLength;
    final int dataLength = length - (dataOffset - offset);

    return new OSHRelation(data, offset, length, //
            baseId, baseTimestamp, baseLongitude, baseLatitude, //
            header, id, bbox, keys, //
            dataOffset, dataLength, //
            nodeIndex, nodeDataOffset, nodeDataLength, //
            wayIndex, wayDataOffset, wayDataLength);
  }

  private OSHRelation(final byte[] data, final int offset, final int length, final long baseId,
          final long baseTimestamp, final long baseLongitude, final long baseLatitude, final byte header,
          final long id, final OSHDBBoundingBox bbox, final int[] keys, final int dataOffset, final int dataLength,
          final int[] nodeIndex, final int nodeDataOffset, final int nodeDataLength, final int[] wayIndex,
          final int wayDataOffset, final int wayDataLength) {
    super(data, offset, length, baseId, baseTimestamp, baseLongitude, baseLatitude, header, id, bbox, keys,
            dataOffset, dataLength);

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

  public List<OSMRelation> getVersions() {
    List<OSMRelation> versions = new ArrayList<>();
    this.forEach(versions::add);
    return versions;
  }

  @Override
  public Iterator<OSMRelation> iterator() {
    try {
      final List<OSHNode> nodes = getNodes();
      final List<OSHWay> ways = getWays();

      return new Iterator<OSMRelation>() {
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
              for (int i = 0; i < size;) {
                keyValues[i++] = wrapper.readUInt32();
                keyValues[i++] = wrapper.readUInt32();
              }
            }

            if ((changed & CHANGED_MEMBERS) != 0) {
              int size = wrapper.readUInt32();
              members = new OSMMember[size];
              long memberId = 0;
              int memberOffset = 0;
              OSMType memberType;
              int memberRole = 0;
              @SuppressWarnings("rawtypes")
              OSHEntity member = null;
              for (int i = 0; i < size; i++) {
                memberType = OSMType.fromInt(wrapper.readUInt32());
                switch (memberType) {
                  case NODE: {
                    memberOffset = wrapper.readUInt32();
                    if (memberOffset > 0) {
                      member = nodes.get(memberOffset - 1);
                      memberId = member.getId();

                    } else {
                      member = null;
                      memberId = wrapper.readSInt64() + memberId;
                    }
                    break;
                  }
                  case WAY: {
                    memberOffset = wrapper.readUInt32();
                    if (memberOffset > 0) {
                      member = ways.get(memberOffset - 1);
                      memberId = member.getId();

                    } else {
                      member = null;
                      memberId = wrapper.readSInt64() + memberId;
                    }
                    break;
                  }
                  case RELATION: {
                    memberId = wrapper.readSInt64() + memberId;
                    break;
                  }
                  default: {
                    memberId = wrapper.readSInt64() + memberId;
                    break;
                  }
                }

                memberRole = wrapper.readUInt32();
                members[i] = new OSMMember(memberId, memberType, memberRole, member);
              }
            }
            return new OSMRelation(id, version, new OSHDBTimestamp(baseTimestamp + timestamp), changeset, userId, keyValues,
                    members);
          } catch (IOException e) {
            e.printStackTrace();
            // TODO: handle exception(s)
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
    for (int index = 0; index < nodeIndex.length; index++) {
      int offset = nodeIndex[index];
      int length = ((index < nodeIndex.length - 1) ? nodeIndex[index + 1] : nodeDataLength) - offset;
      OSHNode n = OSHNode.instance(data, nodeDataOffset + offset, length, 0, 0, baseLongitude, baseLatitude);
      nodes.add(n);
    }
    return nodes;
  }

  public List<OSHWay> getWays() throws IOException {
    List<OSHWay> ways = new ArrayList<>(wayIndex.length);
    for (int index = 0; index < wayIndex.length; index++) {
      int offset = wayIndex[index];
      int length = ((index < wayIndex.length - 1) ? wayIndex[index + 1] : wayDataLength) - offset;
      OSHWay w = OSHWay.instance(data, wayDataOffset + offset, length, 0, 0, baseLongitude, baseLatitude);
      ways.add(w);
    }
    return ways;
  }

  public static OSHRelation build(final List<OSMRelation> versions, final Collection<OSHNode> nodes,
          final Collection<OSHWay> ways) throws IOException {
    return build(versions, nodes, ways, 0, 0, 0, 0);
  }

  public static OSHRelation build(final List<OSMRelation> versions, final Collection<OSHNode> nodes,
          final Collection<OSHWay> ways, final long baseId, final long baseTimestamp, final long baseLongitude,
          final long baseLatitude) throws IOException {
    Collections.sort(versions, Collections.reverseOrder());
    ByteArrayOutputWrapper output = new ByteArrayOutputWrapper();

    OSMMember[] lastMembers = new OSMMember[0];

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
    for (OSHNode node : nodes) {
      OSHDBBoundingBox bbox = node.getBoundingBox();
      if (bbox != null) {
        minLon = Math.min(minLon, bbox.getMinLonLong());
        maxLon = Math.max(maxLon, bbox.getMaxLonLong());
        minLat = Math.min(minLat, bbox.getMinLatLong());
        maxLat = Math.max(maxLat, bbox.getMaxLatLong());
      } else {
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

      node = node.rebase(0, 0, baseLongitude, baseLatitude);
      nodeOffsets.put(node.getId(), idx);
      nodeByteArrayIndex[idx++] = offset;
      offset = node.getLength();
      node.writeTo(nodeData);
    }

    Map<Long, Integer> wayOffsets = new HashMap<>();

    int[] wayByteArrayIndex = new int[ways.size()];
    ByteArrayOutputWrapper wayData = new ByteArrayOutputWrapper();
    idx = 0;
    offset = 0;
    for (OSHWay way : ways) {
      OSHDBBoundingBox bbox = way.getBoundingBox();
      minLon = Math.min(minLon, bbox.getMinLonLong());
      maxLon = Math.max(maxLon, bbox.getMaxLonLong());
      minLat = Math.min(minLat, bbox.getMinLatLong());
      maxLat = Math.max(maxLat, bbox.getMaxLatLong());

      way = way.rebase(0, 0, baseLongitude, baseLatitude);
      wayOffsets.put(way.getId(), idx);
      wayByteArrayIndex[idx++] = offset;
      offset = way.getLength();
      way.writeTo(wayData);
    }

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
        output.writeUInt32(members.length);
        for (OSMMember member : members) {
          output.writeUInt32(member.getType().intValue());
          switch (member.getType()) {
            case RELATION: {
              output.writeSInt64(member.getId() - lastMemberId);
              break;
            }
            case NODE: {
              Integer refOffset = nodeOffsets.get(Long.valueOf(member.getId()));
              if (refOffset == null) {
                output.writeUInt32(0);
                output.writeSInt64(member.getId() - lastMemberId);
              } else {
                output.writeUInt32(refOffset.intValue() + 1);
              }
              break;
            }
            case WAY: {
              Integer refOffset = wayOffsets.get(Long.valueOf(member.getId()));
              if (refOffset == null) {
                output.writeUInt32(0);
                output.writeSInt64(member.getId() - lastMemberId);
              } else {
                output.writeUInt32(refOffset.intValue() + 1);
              }
              break;
            }
            default: {
              break;
            }
          }

          output.writeUInt32(member.getRawRoleId());
          lastMemberId = member.getId();
        }
      }

    }

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

    if (!nodes.isEmpty()) {
      header |= HEADER_HAS_NODES;
    }
    if (!ways.isEmpty()) {
      header |= HEADER_HAS_WAYS;
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

    if (!nodes.isEmpty()) {
      record.writeUInt32(nodeByteArrayIndex.length);
      for (int i = 0; i < nodeByteArrayIndex.length; i++) {
        record.writeUInt32(nodeByteArrayIndex[i]);
      }
      record.writeUInt32(nodeData.length());
      record.writeByteArray(nodeData.array(), 0, nodeData.length());
    }

    if (!ways.isEmpty()) {

      record.writeUInt32(wayByteArrayIndex.length);
      for (int i = 0; i < wayByteArrayIndex.length; i++) {
        record.writeUInt32(wayByteArrayIndex[i]);
      }
      record.writeUInt32(wayData.length());
      record.writeByteArray(wayData.array(), 0, wayData.length());
    }

    record.writeByteArray(output.array(), 0, output.length());
    return OSHRelation.instance(record.array(), 0, record.length(), baseId, baseTimestamp, baseLongitude, baseLatitude);
  }

  public void writeTo(ByteArrayOutputWrapper out) throws IOException {
    out.writeByteArray(data, offset, length);
  }

  @Override
  public OSHRelation rebase(long baseId, long baseTimestamp, long baseLongitude, long baseLatitude)
          throws IOException {

    List<OSMRelation> versions = getVersions();
    List<OSHNode> nodes = getNodes();
    List<OSHWay> ways = getWays();
    return build(versions, nodes, ways, baseId, baseTimestamp, baseLongitude, baseLatitude);
  }

  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  private static class SerializationProxy implements Externalizable {

    private final OSHRelation entity;
    private byte[] data;

    public SerializationProxy(OSHRelation entity) {
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
        return OSHRelation.instance(data, 0, data.length);
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }
  }

  @Override
  public List<OSHDBTimestamp> getModificationTimestamps(boolean recurse) {
    List<OSHDBTimestamp> relTs = new ArrayList<>(this.iterator().next().getVersion());
    for (OSMRelation osmRelation : this) {
      relTs.add(osmRelation.getTimestamp());
    }
    if (!recurse) {
      return Lists.reverse(relTs);
    }

    Map<Long, LinkedList<OSHDBTimestamp>> childNodes = new TreeMap<>();
    Map<Long, LinkedList<OSHDBTimestamp>> childWays = new TreeMap<>();
    int i = -1;
    for (OSMRelation osmRelation : this) {
      i++;
      OSHDBTimestamp thisT = relTs.get(i);
      OSHDBTimestamp nextT = i > 0 ? relTs.get(i - 1) : new OSHDBTimestamp(Long.MAX_VALUE);
      OSMMember[] members = osmRelation.getMembers();
      for (OSMMember member : members) {
        switch (member.getType()) {
          case NODE:
            childNodes.putIfAbsent(member.getId(), new LinkedList<>());
            LinkedList<OSHDBTimestamp> childNodeValidityTimestamps = childNodes.get(member.getId());
            if (childNodeValidityTimestamps.size() > 0 &&
                childNodeValidityTimestamps.getLast().equals(thisT)) {
              // merge consecutive time intervals
              childNodeValidityTimestamps.pop();
              childNodeValidityTimestamps.add(thisT);
            } else {
              childNodeValidityTimestamps.add(nextT);
              childNodeValidityTimestamps.add(thisT);
            }
            break;
          case WAY:
            childWays.putIfAbsent(member.getId(), new LinkedList<>());
            LinkedList<OSHDBTimestamp> childWayValidityTimestamps = childWays.get(member.getId());
            if (childWayValidityTimestamps.size() > 0 &&
                childWayValidityTimestamps.getLast().equals(thisT)) {
              // merge consecutive time intervals
              childWayValidityTimestamps.pop();
              childWayValidityTimestamps.add(thisT);
            } else {
              childWayValidityTimestamps.add(nextT);
              childWayValidityTimestamps.add(thisT);
            }
            break;
        }
      }
    }

    SortedSet<OSHDBTimestamp> result = new TreeSet<>(relTs);
    for (OSMRelation osmRelation : this) {
      OSMMember[] members = osmRelation.getMembers();
      for (OSMMember member : members) {
        Iterator<OSHDBTimestamp> validMemberTs;
        List<OSHDBTimestamp> modTs;
        OSHEntity entity = member.getEntity();
        if (entity == null) continue;
        switch (member.getType()) {
          case NODE:
            OSHNode oshNode = (OSHNode)entity;
            modTs = oshNode.getModificationTimestamps();
            validMemberTs = childNodes.get(member.getId()).iterator();
            break;
          case WAY:
            OSHWay oshWay = (OSHWay)entity;
            modTs = oshWay.getModificationTimestamps(true);
            validMemberTs = childWays.get(member.getId()).iterator();
            break;
          default:
            continue;
        }
        while (validMemberTs.hasNext()) {
          OSHDBTimestamp toTs = validMemberTs.next();
          OSHDBTimestamp fromTs = validMemberTs.next();
          for (OSHDBTimestamp ts : modTs) {
            if (ts.compareTo(fromTs) >= 0 && ts.compareTo(toTs) < 0) {
              result.add(ts);
            }
          }
        }
      }
    }

    return new ArrayList<>(result);
  }

  @Override
  public Map<OSHDBTimestamp, Long> getChangesetTimestamps() {
    Map<OSHDBTimestamp, Long> result = new TreeMap<>();

    List<OSMRelation> rels = this.getVersions();
    rels.forEach(osmRel -> {
      result.put(osmRel.getTimestamp(), osmRel.getChangeset());
    });

    // recurse rel members
    try {
      Stream.concat(
          this.getNodes().stream(),
          this.getWays().stream()
      ).forEach(oshEntity -> {
        if (oshEntity != null)
          oshEntity.getChangesetTimestamps().forEach(result::putIfAbsent);
      });
    } catch (IOException e) {}

    return result;
  }

  @Override
  public String toString() {
    return String.format("OSHRelation %s", super.toString());
  }

}
