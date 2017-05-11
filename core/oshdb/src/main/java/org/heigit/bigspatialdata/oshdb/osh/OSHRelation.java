package org.heigit.bigspatialdata.oshdb.osh;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.heigit.bigspatialdata.oshdb.osh.builder.Builder;
import org.heigit.bigspatialdata.oshdb.osm.*;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.ByteArrayOutputWrapper;
import org.heigit.bigspatialdata.oshdb.util.ByteArrayWrapper;

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

		final BoundingBox bbox = new BoundingBox(minLon * OSMNode.GEOM_PRECISION, maxLon * OSMNode.GEOM_PRECISION,
				minLat * OSMNode.GEOM_PRECISION, maxLat * OSMNode.GEOM_PRECISION);

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
			final long id, final BoundingBox bbox, final int[] keys, final int dataOffset, final int dataLength,
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
							int memberType = 0;
							int memberRole = 0;
							@SuppressWarnings("rawtypes")
							OSHEntity member = null;
							for (int i = 0; i < size; i++) {
								memberType = wrapper.readUInt32();
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
								}

								memberRole = wrapper.readUInt32();
								members[i] = new OSMMember(memberId, memberType, memberRole, member);
							}
						}
						return new OSMRelation(id, version, baseTimestamp + timestamp, changeset, userId, keyValues,
								members);
					} catch (IOException e) {
						e.printStackTrace();
						// TODO: handle exception
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
			BoundingBox bbox = node.getBoundingBox();
			if (bbox != null) {
				minLon = Math.min(minLon, (long) (bbox.minLon / OSMNode.GEOM_PRECISION));
				maxLon = Math.max(maxLon, (long) (bbox.maxLon / OSMNode.GEOM_PRECISION));
				minLat = Math.min(minLat, (long) (bbox.minLat / OSMNode.GEOM_PRECISION));
				maxLat = Math.max(maxLat, (long) (bbox.maxLat / OSMNode.GEOM_PRECISION));
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
			BoundingBox bbox = way.getBoundingBox();
			minLon = Math.min(minLon, (long) (bbox.minLon / OSMNode.GEOM_PRECISION));
			maxLon = Math.max(maxLon, (long) (bbox.maxLon / OSMNode.GEOM_PRECISION));
			minLat = Math.min(minLat, (long) (bbox.minLat / OSMNode.GEOM_PRECISION));
			maxLat = Math.max(maxLat, (long) (bbox.maxLat / OSMNode.GEOM_PRECISION));
			
			
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
					output.writeUInt32(member.getType());
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

					output.writeUInt32(member.getRoleId());
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
			byte[] nodesOutput = nodeData.toByteArray();
			record.writeUInt32(nodesOutput.length);
			record.writeByteArray(nodesOutput);
		}

		if (!ways.isEmpty()) {

			record.writeUInt32(wayByteArrayIndex.length);
			for (int i = 0; i < wayByteArrayIndex.length; i++) {
				record.writeUInt32(wayByteArrayIndex[i]);
			}
			byte[] waysOutput = wayData.toByteArray();
			record.writeUInt32(waysOutput.length);
			record.writeByteArray(waysOutput);
		}

		byte[] buffer = output.toByteArray();
		record.writeByteArray(buffer);

		byte[] data = record.toByteArray();
		return OSHRelation.instance(data, 0, data.length, baseId, baseTimestamp, baseLongitude, baseLatitude);
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
	public List<Long> getModificationTimestamps(boolean recurse) {
		List<OSMRelation> rels = this.getVersions();
		Set<Long> relTimestamps = rels.stream()
		.map(OSMEntity::getTimestamp)
		.collect(Collectors.toSet());

		if (!recurse) return new ArrayList<>(relTimestamps);

		Set<Long> memberTimestamps = IntStream.range(0, rels.size())
		.mapToObj(Integer::new)
		.flatMap(osmRelIndex -> {
			OSMRelation osmRel = rels.get(osmRelIndex);
			OSMRelation nextOsmRel = osmRelIndex > 0 ? rels.get(osmRelIndex - 1) : null;
			return Arrays.stream(osmRel.getMembers())
			.filter(member -> member.getType() == OSHEntity.NODE || member.getType() == OSHEntity.WAY)
			.map(OSMMember::getEntity)
			.flatMap(oshEntity ->
				(oshEntity instanceof OSHNode ? (OSHNode)oshEntity : (OSHWay)oshEntity)
				// gosh, ^--> this is needed because java apparently can't infer the proper stream type from the abstract OSHEntity class
				.getModificationTimestamps(true).stream()
				.filter(ts ->
					ts > osmRel.getTimestamp() && (nextOsmRel == null ||
					ts < nextOsmRel.getTimestamp())
				)
			);
		})
		.collect(Collectors.toSet());

		relTimestamps.addAll(memberTimestamps);

		List<Long> result = new ArrayList<>(relTimestamps);
		Collections.sort(result);
		return result;
	}

}
