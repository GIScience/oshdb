package org.heigit.bigspatialdata.oshpbf.osm;

import java.util.Arrays;
import java.util.List;

public abstract class OSMPbfEntity implements Comparable<OSMPbfEntity> {
	public static class OSMCommonProperties {
		public long id;
		public int version;
		public long timestamp;
		public long changeset;
		public boolean visible = true;
		public OSMPbfUser user;
		public List<OSMPbfTag> tags;
	}

	public static enum Type {
		NODE, WAY, RELATION, CHANGESET, OTHER
	}

	private final long id;
	private final int version;
	private final long timestamp;
	private final long changeset;
	private final Boolean visible;
	private final OSMPbfUser user;
	private final List<OSMPbfTag> tags;

	public OSMPbfEntity(OSMCommonProperties props) {
		this.id = props.id;
		this.version = props.version;
		this.timestamp = props.timestamp;
		this.changeset = props.changeset;
		this.visible = props.visible;
		this.user = props.user;
		this.tags = props.tags;
	}

	public long getId() {
		return id;
	}

	public int getVersion() {
		return version;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public long getChangeset() {
		return changeset;
	}

	public boolean getVisible() {
		return visible;
	}

	public OSMPbfUser getUser() {
		return user;
	}

	public List<OSMPbfTag> getTags() {
		return tags;
	}

	@Override
	public String toString() {
		return String.format("ID:%10d [%3d]\n\tInfo: TS:%d CS:%d V:%s\n\tUser: %s\n\tTags: %s", id, version, timestamp,
				changeset, visible, user, Arrays.toString(tags.toArray()));
	}

	@Override
	public int compareTo(OSMPbfEntity o) {
		int c = Long.compare(id, o.id);
		if (c == 0) {
			c = Integer.compare(version, o.version);
		}
		if (c == 0) {
			c = Long.compare(timestamp, o.timestamp);
		}
		return c;
	}

	public abstract Type getType();

}
