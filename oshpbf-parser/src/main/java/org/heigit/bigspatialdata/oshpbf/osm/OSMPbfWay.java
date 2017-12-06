package org.heigit.bigspatialdata.oshpbf.osm;

import java.util.Arrays;
import java.util.List;

public class OSMPbfWay extends OSMPbfEntity {
	private final List<Long> refs;

	public OSMPbfWay(OSMCommonProperties props, List<Long> refs) {
		super(props);
		this.refs = refs;
	}

	public List<Long> getRefs() {
		return refs;
	}

	@Override
	public String toString() {
		return String.format("W-> %s \n\tReferences: %s", super.toString(), Arrays.toString(refs.toArray()));
	}

	@Override
	public Type getType() {
		return Type.WAY;
	}

}
