package org.heigit.bigspatialdata.oshpbf.parser.pbf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.CommonEntityData;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Node;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Relation;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.RelationMember;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Tag;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.TagText;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Way;

public class OsmPrimitveBlockIterator implements Iterator<Object> {

	private final long blockStartPostion;
	private final Set<OSMType> types;
	private final crosby.binary.Osmformat.StringTable stringTable;
	private final String[] stringIndex;

	public final  List<crosby.binary.Osmformat.PrimitiveGroup> groups;
	private final  Iterator<crosby.binary.Osmformat.PrimitiveGroup> groupIterator;

	public final int granularityLocation;
	public final int granularityDate;
	public final long offsetLongitude;
	public final long offsetLatitude;

	private Iterator<Entity> entityIterator = Collections.emptyIterator();

	public OsmPrimitveBlockIterator(long blockStartPosition,crosby.binary.Osmformat.PrimitiveBlock block, Set<OSMType> types) {
		this.blockStartPostion = blockStartPosition;
		this.types = types;

		stringTable = block.getStringtable();
		stringIndex = new String[stringTable.getSCount()];

		granularityLocation = block.getGranularity();
		granularityDate = block.getDateGranularity();
		offsetLongitude = block.getLonOffset();
		offsetLatitude = block.getLatOffset();

		groups = block.getPrimitivegroupList();
		groupIterator = groups.iterator();

	}

	public long getBlockStartPosition() {
		return blockStartPostion;
	}
	
	
	public Set<OSMType> getTypes(){
	  return types;
	}

	@Override
	public boolean hasNext() {
		return groupIterator.hasNext() || entityIterator.hasNext();
	}

	@Override
	public Entity next() {
		if (!entityIterator.hasNext() && groupIterator.hasNext())
			entityIterator = parse(groupIterator.next());

		if (entityIterator.hasNext())
			return entityIterator.next();

		throw new NoSuchElementException("no entities within a group");
	}

	private Iterator<Entity> parse(crosby.binary.Osmformat.PrimitiveGroup group) {
		if (group.hasDense()) {
			return parseDense(group.getDense());
		} else if (hasElements(group.getNodesList())) {
			return parseNodes(group.getNodesList());
		} else if (hasElements(group.getWaysList())) {
			return parseWays(group.getWaysList());
		} else if (hasElements(group.getRelationsList())) {
			return parseRelations(group.getRelationsList());
		}

		return Collections.emptyIterator();
	}

	private Iterator<Entity> parseRelations(List<crosby.binary.Osmformat.Relation> entities) {
		return new Iterator<Entity>() {
			final Iterator<crosby.binary.Osmformat.Relation> entityIterator = entities.iterator();

			@Override
			public boolean hasNext() {
				return entityIterator.hasNext();
			}

			@Override
			public Entity next() {
				final crosby.binary.Osmformat.Relation entity = entityIterator.next();

				final CommonEntityData ced = getCommonEntityData(entity.getId(), entity.getInfo(), entity.getKeysList(),
						entity.getValsList());

				List<Integer> rolesSidList = entity.getRolesSidList();
				List<Long> memIdsList = entity.getMemidsList();
				List<crosby.binary.Osmformat.Relation.MemberType> typesList = entity.getTypesList();
				long memId = 0;
				RelationMember[] members = new RelationMember[memIdsList.size()];
				crosby.binary.Osmformat.Relation.MemberType type;
				for (int i = 0, iL = memIdsList.size(); i < iL; i++) {
					int roleSid = rolesSidList.get(i);
					memId += memIdsList.get(i);
					type = typesList.get(i);
					members[i] = new RelationMember(memId, getString(roleSid), type.getNumber());
				}

				return new Relation(ced, members);
			}

		};
	}

	private Iterator<Entity> parseWays(List<crosby.binary.Osmformat.Way> entities) {
		return new Iterator<Entity>() {
			final Iterator<crosby.binary.Osmformat.Way> entityIterator = entities.iterator();

			@Override
			public boolean hasNext() {
				return entityIterator.hasNext();
			}

			@Override
			public Entity next() {
				final crosby.binary.Osmformat.Way entity = entityIterator.next();

				final CommonEntityData ced = getCommonEntityData(entity.getId(), entity.getInfo(), entity.getKeysList(),
						entity.getValsList());

				final List<Long> refsList = entity.getRefsList();
				final long[] refs = new long[refsList.size()];

				long ref = 0;
				int i = 0;
				for (long refDelta : refsList) {
					ref += refDelta;
					refs[i++] = ref;
				}

				return new Way(ced, refs);
			}

		};
	}

	private Iterator<Entity> parseNodes(final List<crosby.binary.Osmformat.Node> entities) {
		return new Iterator<Entity>() {
			final Iterator<crosby.binary.Osmformat.Node> entityIterator = entities.iterator();

			@Override
			public boolean hasNext() {
				return entityIterator.hasNext();
			}

			@Override
			public Entity next() {
				final crosby.binary.Osmformat.Node entity = entityIterator.next();

				final CommonEntityData ced = getCommonEntityData(entity.getId(), entity.getInfo(), entity.getKeysList(),
						entity.getValsList());
				final long lon = entity.getLon();
				final long lat = entity.getLat();

				return new Node(ced, lon, lat);
			}
		};
	}

	private Iterator<Entity> parseDense(crosby.binary.Osmformat.DenseNodes dense) {
		return new Iterator<Entity>() {
			final List<Long> idList = dense.getIdList();
			final int idCount = idList.size();

			final boolean hasDenseInfo = dense.hasDenseinfo();
			final List<Integer> versionList = hasDenseInfo ? dense.getDenseinfo().getVersionList() : null;
			final List<Long> timestampList = hasDenseInfo ? dense.getDenseinfo().getTimestampList() : null;
			final List<Long> changesetList = hasDenseInfo ? dense.getDenseinfo().getChangesetList() : null;
			final List<Integer> uidList = hasDenseInfo ? dense.getDenseinfo().getUidList() : null;
			final List<Integer> userSidList = hasDenseInfo ? dense.getDenseinfo().getUserSidList() : null;
			final List<Boolean> visibleList = hasDenseInfo ? dense.getDenseinfo().getVisibleList() : null;

			final List<Integer> keyValsList = dense.getKeysValsList();
			final int keyVasCount = keyValsList.size();

			final List<Long> lonList = dense.getLonList();
			final List<Long> latList = dense.getLatList();

			int cursor = 0;
			int keyVal = 0;

			long id = 0;
			int version;
			long timestamp = 0;
			long changeset = 0;
			int uid = 0;
			int userSid = 0;
			boolean visible;

			long lon = 0;
			long lat = 0;

			@Override
			public boolean hasNext() {
				return cursor < idCount;
			}

			@Override
			public Entity next() {
				final int index = cursor++;

				id += idList.get(index);

				if (hasDenseInfo) {
					version = versionList.get(index);
					timestamp += timestampList.get(index);
					changeset += changesetList.get(index);
					uid += uidList.get(index);
					userSid += userSidList.get(index);
					visible = (!visibleList.isEmpty())?visibleList.get(index):true;
				}

				List<Tag> tags = new ArrayList<>();
				if (keyVal < keyVasCount) {
					for (int key = keyValsList.get(keyVal++); key > 0; key = keyValsList.get(keyVal++)) {
						int value = keyValsList.get(keyVal++);
						tags.add(new TagText(getString(key), getString(value)));
					}
				}
				
				final CommonEntityData ced = new CommonEntityData(id, version, changeset, timestamp, visible, uid,
						getString(userSid), tags.toArray(new TagText[tags.size()]));

				lon += lonList.get(index);
				lat += latList.get(index);

				return new Node(ced, lon, lat);
			}
		};
	}

	private CommonEntityData getCommonEntityData(final long id, final crosby.binary.Osmformat.Info info,
			List<Integer> keysList, List<Integer> valsList) {
		int version = -1;
		if (info.hasVersion()) {
			version = info.getVersion();
		}
		long timestamp = -1;
		if (info.hasTimestamp()) {
			timestamp = info.getTimestamp(); // // fix timestamp to
												// dateGranularity = 1, osm only
												// is second precision,
												// block.getDateGranularity();
		}
		long changeset = -1;
		if (info.hasChangeset()) {
			changeset = Long.valueOf(info.getChangeset());
		}

		boolean visible = false;
		if (info.hasVisible()) {
			visible = info.getVisible();
		}

		int uid = -1;
		if (info.hasUid()) {
			uid = info.getUid();
		}
		String user = "";
		if (info.hasUserSid()) {
			user = getString(info.getUserSid());
		}

		TagText[] tags = new TagText[keysList.size()];
		for (int i = 0; i < keysList.size(); i++) {
			tags[i] = new TagText(getString(keysList.get(i).intValue()), getString(valsList.get(i).intValue()));
		}

		return new CommonEntityData(id, version, changeset, timestamp, visible, uid, user, tags);
	}

	/**
	 * resolves a String value from its sid.
	 * 
	 * @param sid
	 * @return
	 */
	private String getString(int sid) {
		String s = stringIndex[sid];
		if (s == null) {
			s = stringTable.getS(sid).toStringUtf8();
			stringIndex[sid] = s;
		}
		return s;
	}

	@SuppressWarnings("rawtypes")
	private boolean hasElements(List l) {
		if (l == null || l.isEmpty())
			return false;
		return true;
	}

}
