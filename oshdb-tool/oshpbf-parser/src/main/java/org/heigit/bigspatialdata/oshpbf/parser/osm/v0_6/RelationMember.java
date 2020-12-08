package org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6;

public class RelationMember {

	public final long memId;
	public final String role;
	public final int type;

	public RelationMember(long memId, String role, int type) {
		this.memId = memId;
		this.role = role;
		this.type = type;
	}

}
