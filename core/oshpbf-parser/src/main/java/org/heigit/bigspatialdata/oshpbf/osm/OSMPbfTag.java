package org.heigit.bigspatialdata.oshpbf.osm;

public class OSMPbfTag implements Comparable<OSMPbfTag> {
	private final String key;
	private final String value;

	public OSMPbfTag(String key, String value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public String getValue() {
		return value;
	}

	@Override
	public String toString() {
		return String.format("(%s=%s)", key, value);
	}

	@Override
	public int compareTo(OSMPbfTag other) {
		int c = key.compareTo(other.key);
		if (c == 0)
			c = value.compareTo(other.value);

		return c;
	}
	
	@Override
	public int hashCode() {
		int result = 234;
		result = 37 * result + key.hashCode();
		result = 37 * result + value.hashCode();
		return result;
	}
}
