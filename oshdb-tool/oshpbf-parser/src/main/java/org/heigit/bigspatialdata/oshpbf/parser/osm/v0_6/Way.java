package org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;

public class Way extends Entity {

	public final long[] refs;

	public Way(CommonEntityData entityData, long[] refs) {
		super(entityData);
		this.refs = refs;
	}

	@Override
	public OSMType getType() {
		return OSMType.WAY;
	}

  public long[] getRefs() {
    return refs;
  }

}
