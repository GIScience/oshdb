package org.heigit.bigspatialdata.oshpbf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfChangeset;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity.OSMCommonProperties;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfNode;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfRelation;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfRelation.OSMMember;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfTag;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfUser;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfWay;

import crosby.binary.Osmformat;
import crosby.binary.Osmformat.ChangeSet;
import crosby.binary.Osmformat.DenseNodes;
import crosby.binary.Osmformat.Info;
import crosby.binary.Osmformat.Node;
import crosby.binary.Osmformat.PrimitiveBlock;
import crosby.binary.Osmformat.PrimitiveGroup;
import crosby.binary.Osmformat.Relation;
import crosby.binary.Osmformat.StringTable;
import crosby.binary.Osmformat.Way;

public class OsmPbfIterator implements Iterator<OSMPbfEntity> {


	private final Iterator<Osmformat.PrimitiveBlock> source;

	private PrimitiveBlock block;
	private StringTable stringTable;

	private List<PrimitiveGroup> groups;
	private Iterator<PrimitiveGroup> groupIterator;
	private PrimitiveGroup group;

	private List<OSMPbfEntity> osmEntities;
	private Iterator<OSMPbfEntity> osmEntitiesIterator;

	public OsmPbfIterator(Iterator<Osmformat.PrimitiveBlock> source) {
		this.source = source;

		this.groups = new ArrayList<>();
		this.groupIterator = groups.iterator();

		this.osmEntities = new ArrayList<>(10_000);
		this.osmEntitiesIterator = osmEntities.iterator();
		
		seekNext();
	}

	private void seekNext() {
		while (true) {
			if (osmEntitiesIterator.hasNext())
				break;
			if (groupIterator.hasNext()) {
				group = groupIterator.next();
				parseGroup(group);
				if (osmEntitiesIterator.hasNext())
					break;
			}
			if (source.hasNext()) {
				block = source.next();
				stringTable = block.getStringtable();
				groupIterator = block.getPrimitivegroupList().iterator();
			} else {
				break;
			}
		}
	}

	@Override
	public boolean hasNext() {
		return osmEntitiesIterator.hasNext();
	}

	@Override
	public OSMPbfEntity next() {
		OSMPbfEntity entity = osmEntitiesIterator.next();
		seekNext();
		return entity;
	}

	private void parseGroup(PrimitiveGroup group) {
		osmEntities.clear();
		if (group.hasDense()) {
			parseDense(group.getDense());
		} else if (hasElements(group.getNodesList())) {
			parseNodes(group.getNodesList());
		} else if (hasElements(group.getWaysList())) {
			parseWays(group.getWaysList());
		} else if (hasElements(group.getRelationsList())) {
			parseRelations(group.getRelationsList());
		} else if (hasElements(group.getChangesetsList())) {
			parseChangesets(group.getChangesetsList());
		}
		osmEntitiesIterator = osmEntities.iterator();
	}

	private void parseDense(DenseNodes nodes) {
		List<Integer> versions = Collections.emptyList();
		List<Long> timestamps = Collections.emptyList();
		List<Long> changesets = Collections.emptyList();
		List<Integer> uids = Collections.emptyList();
		List<Integer> user_sids = Collections.emptyList();
		List<Boolean> visibles = Collections.emptyList();

		if (nodes.hasDenseinfo()) {
			Osmformat.DenseInfo info = nodes.getDenseinfo();
			versions = info.getVersionList();
			timestamps = info.getTimestampList();
			changesets = info.getChangesetList();
			uids = info.getUidList();
			user_sids = info.getUserSidList();
			visibles = info.getVisibleList();
		}

		List<Integer> keyvals = nodes.getKeysValsList();

		final int granularity = 1; // fix granularity to 1 but reduce precission to 7 decimal  digit instead to 9; block.getGranularity();
		final long lat_offset = block.getLatOffset();
		final long lon_offset = block.getLonOffset();
		final int date_granularity = 1; // fix timestamp to 1, osm only is second precision, block.getDateGranularity();

		long id = 0;
		long lat = 0;
		long lon = 0;
		long timestamp = 0;
		int keyVal = 0;

		long changeset = 0;

		int userid = 0;
		int usersid = 0;

		for (int i = 0; i < nodes.getIdCount(); i++) {
			OSMCommonProperties props = new OSMCommonProperties();

			id += nodes.getId(i);
			props.id = id;

			if (nodes.hasDenseinfo()) {
				int version = versions.get(i);
				timestamp += timestamps.get(i);
				changeset += changesets.get(i);
				userid += uids.get(i);
				usersid += user_sids.get(i);

				props.version = version;
				props.timestamp = timestamp * date_granularity;
				props.changeset = changeset;
				props.visible = visibles.get(i);

				OSMPbfUser user = new OSMPbfUser(userid, getString(usersid));
				props.user = user;
			}

			if (!keyvals.isEmpty()) {
				List<OSMPbfTag> tags = new ArrayList<OSMPbfTag>();
				while (keyvals.get(keyVal).intValue() > 0) {
					String key = getString(keyvals.get(keyVal++));
					String value = getString(keyvals.get(keyVal++));
					tags.add(new OSMPbfTag(key, value));
				}
				keyVal++; // skip the zero value tag
				props.tags = tags;
			}

			lat += nodes.getLat(i);
			lon += nodes.getLon(i);
			//double lonD = .000000001 * (granularity * lon + lon_offset);
			//double latD = .000000001 * (granularity * lat + lat_offset);
			osmEntities.add(new OSMPbfNode(props, (granularity * lon + lon_offset),(granularity * lat + lat_offset)));
		}

	}

	private void parseNodes(List<Node> nodes) {
		int granularity = block.getGranularity();
		long lat_offset = block.getLatOffset();
		long lon_offset = block.getLonOffset();

		for (Osmformat.Node e : nodes) {
			OSMCommonProperties props = new OSMCommonProperties();
			props.id = e.getId();
			populateInfo(props, e.getInfo());
			populateTags(props, e.getKeysList(), e.getValsList());

			//double lon = .000000001 * (granularity * e.getLon() + lon_offset);
			//double lat = .000000001 * (granularity * e.getLat() + lat_offset);
		
			osmEntities.add(new OSMPbfNode(props, (granularity * e.getLon() + lon_offset),(granularity * e.getLat() + lat_offset)));
		}
	}

	private void parseWays(List<Way> ways) {
		for (Osmformat.Way e : ways) {
			OSMCommonProperties props = new OSMCommonProperties();
			props.id = e.getId();
			populateInfo(props, e.getInfo());
			populateTags(props, e.getKeysList(), e.getValsList());

			List<Long> refsDelta = e.getRefsList();
			List<Long> refs = new ArrayList<>(refsDelta.size());
			long ref = 0;
			for (int i = 0, iL = refsDelta.size(); i < iL; i++) {
				ref += refsDelta.get(i);
				refs.add(ref);
			}
			osmEntities.add(new OSMPbfWay(props, refs));
		}
	}

	private void parseRelations(List<Relation> relations) {
		for (Osmformat.Relation e : relations) {
			OSMCommonProperties props = new OSMCommonProperties();
			props.id = e.getId();
			populateInfo(props, e.getInfo());
			populateTags(props, e.getKeysList(), e.getValsList());

			List<Integer> roles = e.getRolesSidList();
			List<Long> memIds = e.getMemidsList();
			List<Osmformat.Relation.MemberType> types = e.getTypesList();
			long memId = 0;
			List<OSMMember> members = new ArrayList<>(memIds.size());
			for (int i = 0, iL = memIds.size(); i < iL; i++) {
				int role = roles.get(i);
				memId += memIds.get(i);
				int type = -1;
				switch (types.get(i)) {
					case NODE:
						type = OSHEntity.NODE;
						break;
					case WAY:
						type = OSHEntity.WAY;
						break;
					case RELATION:
						type = OSHEntity.RELATION;
						break;
				}
				OSMMember m = new OSMMember(memId, getString(role), type);
				members.add(m);
			}
			osmEntities.add(new OSMPbfRelation(props, members));
		}
	}

	private void parseChangesets(List<ChangeSet> changesets) {
		for (Osmformat.ChangeSet e : changesets) {
			OSMCommonProperties props = new OSMCommonProperties();
			props.id = e.getId();
			/*
			 * populateInfo(props, e.getInfo()); populateTags(props,
			 * e.getKeysList(), e.getValsList());
			 */
			osmEntities.add(new OSMPbfChangeset(props));
		}
	}

	private void populateInfo(OSMCommonProperties props, Info info) {
		if (info.hasVersion()) {
			props.version = info.getVersion();
		}
		if (info.hasTimestamp()) {
			props.timestamp = info.getTimestamp() * 1; // // fix timestamp to dateGranularity = 1, osm only is second precision, block.getDateGranularity();
		}
		if (info.hasChangeset()) {
			props.changeset = Long.valueOf(info.getChangeset());
		}

		if (info.hasVisible()) {
			props.visible = info.getVisible();
		}

		int userId = -1;
		if (info.hasUid()) {
			userId = info.getUid();
		}
		String userName = "";
		if (info.hasUserSid()) {
			userName = getString(info.getUserSid());
		}
		props.user = new OSMPbfUser(userId, userName);
	}

	private void populateTags(OSMCommonProperties props, List<Integer> keys, List<Integer> vals) {
		List<OSMPbfTag> tags = new ArrayList<>();
		for (int i = 0, iL = keys.size(); i < iL; i++) {
			String key = getString(keys.get(i));
			String value = getString(vals.get(i));
			tags.add(new OSMPbfTag(key, value));
		}
		props.tags = tags;
	}

	private String getString(int sid) {
		return stringTable.getS(sid).toStringUtf8();
	}

	@SuppressWarnings("rawtypes")
	private boolean hasElements(List l) {
		if (l == null || l.isEmpty())
			return false;
		return true;
	}
}
