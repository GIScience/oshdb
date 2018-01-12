package org.heigit.bigspatialdata.oshpbf.osm;


public class OSMPbfChangeset extends OSMPbfEntity{

	public OSMPbfChangeset(OSMCommonProperties props) {
		super(props);
	}

	@Override
	public Type getType() {
		return Type.CHANGESET;
	}
}
