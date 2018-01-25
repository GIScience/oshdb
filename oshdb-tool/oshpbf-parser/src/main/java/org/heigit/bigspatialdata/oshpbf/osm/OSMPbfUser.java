package org.heigit.bigspatialdata.oshpbf.osm;


public class OSMPbfUser implements Comparable<OSMPbfUser> {
	private int id;
	private String name;

	public OSMPbfUser(int id, String name) {
		this.id = id;
		this.name = name;
	}

	public void setId(int id) {
		this.id = id;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	@Override
	public String toString() {
		return String.format("%s [%d]", name, id);
	}

	@Override
	public int compareTo(OSMPbfUser o) {
		int c = Integer.compare(id, o.id);
		if (c == 0) {
			c = name.compareTo(o.name);
		}
		return c;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof OSMPbfUser) {
			OSMPbfUser other = (OSMPbfUser) obj;
			if (id == other.id)
				return true;
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		int result = 3;
		result = 37 * result+id;
		result = 37 * result + name.hashCode();
		
		return result;
	}
}