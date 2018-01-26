package org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6;

import java.util.Arrays;

public class CommonEntityData {

	public final long id;
	public final int version;
	public final long timestamp;
	public final long changeset;
	public final boolean visible;
	public final int userId;
	public final String user;
	public final TagText[] tags;

	public CommonEntityData(long id, int version, long changeset, long timestamp, boolean visible, int userId,
			String user, TagText[] tags) {
		this.id = id;
		this.version = version;
		this.changeset = changeset;
		this.timestamp = timestamp;
		this.visible = visible;
		this.userId = userId;
		this.user = user;
		this.tags = tags;
	}
	
	@Override
	public String toString() {
	  return String.format("%d ver:%d,ch:%d,ts:%d,v:%s,uid:%d [%s]", id,version,changeset,timestamp,visible,userId,Arrays.toString(tags));
	 
	}

}
