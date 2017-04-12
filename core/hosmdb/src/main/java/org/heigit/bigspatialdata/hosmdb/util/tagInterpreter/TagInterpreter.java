package org.heigit.bigspatialdata.hosmdb.util.tagInterpreter;

import org.heigit.bigspatialdata.hosmdb.osm.OSMEntity;
import org.heigit.bigspatialdata.hosmdb.osm.OSMWay;
import org.heigit.bigspatialdata.hosmdb.osm.OSMRelation;

import java.util.Map;
import java.util.Set;

/**
 * instances of this class are used to determine wether a OSM way represents a polygon or linestring geometry.
 */
public class TagInterpreter {
	int areaNoTagKey;
	int areaNoTagValue;
	Map<Integer, Set<Integer>> wayAreaTags;
	Map<Integer, Set<Integer>> relationAreaTags;

	Set<Integer> uninterestingTagKeys;

	public TagInterpreter(int areaNoTagKey, int areaNoTagValue, Map<Integer, Set<Integer>> wayAreaTags, Map<Integer, Set<Integer>> relationAreaTags, Set<Integer> uninterestingTagKeys) {
		this.areaNoTagKey = areaNoTagKey;
		this.areaNoTagValue = areaNoTagValue;
		this.wayAreaTags = wayAreaTags;
		this.relationAreaTags = relationAreaTags;
		this.uninterestingTagKeys = uninterestingTagKeys;
	}

	private boolean evaluateWayForArea(int[] tags) {
		// todo: replace with quicker binary search (tag keys are sorted)
		for (int i = 0; i < tags.length; i += 2) {
			if (tags[i] == areaNoTagKey)
				if (tags[i + 1] == areaNoTagValue)
					return false;
				else
					break;
		}
		for (int i = 0; i < tags.length; i += 2) {
			if (wayAreaTags.containsKey(tags[i]) &&
					wayAreaTags.get(tags[i]).contains(tags[i + 1]))
				return true;
		}
		return false;
	}

	private boolean evaluateRelationForArea(int[] tags) {
		// skip area=no check, since that doesn't make much sense for multipolygon relations (does it??)
		for (int i = 0; i < tags.length; i += 2) {
			if (relationAreaTags.containsKey(tags[i]) &&
					relationAreaTags.get(tags[i]).contains(tags[i + 1]))
				return true;
		}
		return false;
	}

	public boolean evaluateForArea(OSMEntity osm) {
		int[] tags = osm.getTags();
		// todo: implement for relation
		if (osm instanceof OSMWay) {
			return this.evaluateWayForArea(tags);
		} else if (osm instanceof OSMRelation) {
			return this.evaluateRelationForArea(tags);
		} else {
			return false;
		}
	}

	public boolean evaluateForLine(OSMEntity osm) {
		return !evaluateForArea(osm);
	}

	public boolean hasInterestingTagKey(int[] tags) {
		for (int i=0; i<tags.length; i+=2) {
			if (!uninterestingTagKeys.contains(tags[i]))
				return true;
		}
		return false;
	}
}
