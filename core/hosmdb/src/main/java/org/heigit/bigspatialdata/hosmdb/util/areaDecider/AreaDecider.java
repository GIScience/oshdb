package org.heigit.bigspatialdata.hosmdb.util.areaDecider;

import org.heigit.bigspatialdata.hosmdb.osm.OSMEntity;
import org.heigit.bigspatialdata.hosmdb.osm.OSMWay;

import java.util.Map;
import java.util.Set;

/**
 * instances of this class are used to determine wether a OSM way represents a polygon or linestring geometry.
 */
public class AreaDecider {
	int areaNoTagKey;
	int areaNoTagValue;
	Map<Integer, Set<Integer>> areaTags;

	public AreaDecider(int areaNoTagKey, int areaNoTagValue, Map<Integer, Set<Integer>> areaTags) {
		this.areaNoTagKey = areaNoTagKey;
		this.areaNoTagValue = areaNoTagValue;
		this.areaTags = areaTags;
	}

	public boolean evaluate(int[] tags) {
		// todo: replace with quicker binary search (tag keys are sorted)
		for (int i=0; i<tags.length; i+=2) {
			if (tags[i] == areaNoTagKey && tags[i+1] == areaNoTagValue)
				return false;
		}
		for (int i=0; i<tags.length; i+=2) {
			if (areaTags.containsKey(tags[i]) &&
				areaTags.get(tags[i]).contains(tags[i+1]))
				return true;
		}
		return false;
	}
}
