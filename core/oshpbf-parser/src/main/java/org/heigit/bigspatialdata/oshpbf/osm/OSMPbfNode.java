package org.heigit.bigspatialdata.oshpbf.osm;


public class OSMPbfNode extends OSMPbfEntity {
	
	//public static final double presision = .000000001; 

	private final long longitude;
	private final long latitude;

	public OSMPbfNode(OSMCommonProperties props, long longitude, long latitude) {
		super(props);
		this.longitude = longitude;
		this.latitude = latitude;
	}

	public long getLongitude() {
		return longitude;
	}
	
	public long getLatitude() {
		return latitude;
	}

	@Override
	public String toString() {
		return String.format("N-> %s \n\tNode: %d %d", super.toString(), longitude, latitude);

	}

	@Override
	public Type getType() {
		return Type.NODE;
	}
}
