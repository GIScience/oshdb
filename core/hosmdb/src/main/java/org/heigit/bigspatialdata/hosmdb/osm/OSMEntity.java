package org.heigit.bigspatialdata.hosmdb.osm;

import java.util.Arrays;
import java.util.stream.IntStream;

public abstract class OSMEntity {
	protected final long id;

	protected final int version;
	protected final long timestamp;
	protected final long changeset;
	protected final int userId;
	protected final int[] tags;

	public OSMEntity(final long id, final int version, final long timestamp, final long changeset, final int userId,
			final int[] tags) {
		this.id = id;
		this.version = version;
		this.timestamp = timestamp;
		this.changeset = changeset;
		this.userId = userId;
		this.tags = tags;
	}

	public long getId() {
		return id;
	}

	public int getVersion() {
		return Math.abs(version);
	}

	public long getTimestamp() {
		return timestamp;
	}

	public long getChangeset() {
		return changeset;
	}

	public int getUserId() {
		return userId;
	}

	public boolean isVisible() {
		return (version >= 0);
	}

	public int[] getTags() {
		return tags;
	}

	public boolean hasTagKey(int key) {
        // todo: replace this with binary search (keys are sorted)
		for (int i = 0; i < tags.length; i += 2) {
			if (tags[i] < key)
				continue;
			if (tags[i] == key)
				return true;
			break;
		}
		return false;
	}

  /** Tests if any a given key is present but ignores certain values. 
   *  Useful when looking for example "TagKey" != "no"
   *
   * @param key the key to search for
   * @param uninterestingValues list of values, that should return false although the key is actually present
   * @return true if the key is present and is NOT in a combination with the given values, false otherwise 
   */

	public boolean hasTagKey(int key, int[] uninterestingValues) {
        // todo: replace this with binary search (keys are sorted)
		for (int i = 0; i < tags.length; i += 2) {
			if (tags[i] < key)
				continue;
			if (tags[i] == key) {
				final int value = tags[i + 1];                                
				return !IntStream.of(uninterestingValues).anyMatch(x -> x == value);
			}
			break;
		}
		return false;
	}

	public boolean hasTagValue(int key, int value) {
        // todo: replace this with binary search (keys are sorted)
		for (int i = 0; i < tags.length; i += 2) {
			if (tags[i] > key)
				return false;
			if (tags[i] == key)
				return tags[i + 1] == value;
			break;
		}
		return false;
	}

	public boolean equalsTo(OSMEntity o) {
		return id == o.id && version == o.version && timestamp == o.timestamp && changeset == o.changeset
				&& userId == o.userId && Arrays.equals(tags, o.tags);
	}

	@Override
	public String toString() {
		return String.format("ID:%d V:+%d+ TS:%d CS:%d VIS:%s USER:%d TAGS:%S", id, getVersion(), getTimestamp(),
				getChangeset(), isVisible(), getUserId(), Arrays.toString(getTags()));
	}

}
