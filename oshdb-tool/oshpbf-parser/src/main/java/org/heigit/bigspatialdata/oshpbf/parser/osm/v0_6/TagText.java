package org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6;

public class TagText implements Tag {

	public final String key;
	public final String value;

	public TagText(String key, String value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (!(obj instanceof TagText))
			return false;
		TagText tag = (TagText) obj;

		return key.equals(tag.key) && value.equals(tag.value);
	}

	@Override
	public int hashCode() {
		int result = 37 * key.hashCode();
		result = 37 * value.hashCode();
		return result;
	}

	@Override
	public String toString() {
	  return key+"="+value;
	}
}
