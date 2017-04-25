package org.heigit.bigspatialdata.hosmdb.util.tagInterpreter;

import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.hosmdb.osm.OSMEntity;
import org.heigit.bigspatialdata.hosmdb.osm.OSMRelation;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Default TagInterpreter
 */
public class DefaultTagInterpreter extends TagInterpreter {

	private int typeKey = -1;
	private int typeMultipolygonValue = -1;
	private int typeBoundaryValue = -1;
	private int typeRouteValue = -1;

	public DefaultTagInterpreter(Map<String, Map<String, Pair<Integer, Integer>>> allKeyValues, Map<String, Integer> allRoles) throws IOException, ParseException {
		this(
			Thread.currentThread().getContextClassLoader().getResource("json/polygon-features.json").getPath(),
			Thread.currentThread().getContextClassLoader().getResource("json/uninterestingTags.json").getPath(),
			allKeyValues,
			allRoles
		);
	}

	public DefaultTagInterpreter(String areaTagsDefinitionFile, String uninterestingTagsDefinitionFile, Map<String, Map<String, Pair<Integer, Integer>>> allKeyValues, Map<String, Integer> allRoles) throws FileNotFoundException, IOException, ParseException {
		super(-1,-1, null, null, null, -1, -1, -1); // initialize with dummy parameters for now

		// construct list of area tags for ways
		Map<Integer, Set<Integer>> wayAreaTags = new HashMap<>();

		JSONParser parser = new JSONParser();
		JSONArray tagList = (JSONArray)parser.parse(new FileReader(areaTagsDefinitionFile));
		Iterator<JSONObject> tagIt = tagList.iterator();
		while (tagIt.hasNext()) {
			JSONObject tag = tagIt.next();
			String type = (String)tag.get("polygon");

			Set<Integer> valueIds;
			String key, value;
			Map<String, Pair<Integer, Integer>> keyValues;
			JSONArray values;
			Iterator<String> valuesIt;
			int keyId;
			switch (type) {
				case "whitelist":
					valueIds = new HashSet<>();
					key = (String)tag.get("key");
					if (!allKeyValues.containsKey(key)) {
						// no such tag key present in this db extract
						System.err.printf("DefaultTagInterpreter: key \"%s\" not found in this db extract\n", key);
						continue;
					}
					keyValues = allKeyValues.get(key);
					values = (JSONArray)tag.get("values");
					valuesIt = values.iterator();
					keyId = -1;
					while (valuesIt.hasNext()) {
						value = valuesIt.next();
						if (!keyValues.containsKey(value)) {
							// no such tag key/value in this db extract
							System.err.printf("DefaultTagInterpreter: key/value \"%s\"=\"%s\" not found in this db extract\n", key, value);
							continue;
						}
						valueIds.add(keyValues.get(value).getRight());
						keyId = keyValues.get(value).getLeft();
					}
					if (!keyValues.isEmpty())
						wayAreaTags.put(keyId, valueIds);
					break;
				case "all":
					valueIds = new InvertedHashSet<>();
					key = (String)tag.get("key");
					if (!allKeyValues.containsKey(key)) {
						// no such tag key present in this db extract
						System.err.printf("DefaultTagInterpreter: key \"%s\" not found in this db extract\n", key);
						continue;
					}
					keyValues = allKeyValues.get(key);
					Iterator<Pair<Integer,Integer>> keyValuesIt = keyValues.values().iterator();
					if (!keyValuesIt.hasNext()) {
						// no such tag value present int this db extract??
						System.err.printf("DefaultTagInterpreter: key \"%s\" not found in this db extract\n", key);
						continue;
					}
					keyId = keyValuesIt.next().getLeft();
					if (keyValues.containsKey("no"))
						valueIds.add(keyValues.get("no").getRight());
					wayAreaTags.put(keyId, valueIds);
					break;
				case "blacklist":
					valueIds = new InvertedHashSet<>();
					key = (String)tag.get("key");
					if (!allKeyValues.containsKey(key)) {
						// no such tag key present in this db extract
						System.err.printf("DefaultTagInterpreter: key \"%s\" not found in this db extract\n", key);
						continue;
					}
					keyValues = allKeyValues.get(key);
					values = (JSONArray)tag.get("values");
					valuesIt = values.iterator();
					keyId = -1;
					while (valuesIt.hasNext()) {
						value = valuesIt.next();
						if (!keyValues.containsKey(value)) {
							// no such tag key/value in this db extract
							System.err.printf("DefaultTagInterpreter: key/value \"%s\"=\"%s\" not found in this db extract\n", key, value);
							continue;
						}
						valueIds.add(keyValues.get(value).getRight());
						keyId = keyValues.get(value).getLeft();
					}
					if (keyValues.containsKey("no"))
						valueIds.add(keyValues.get("no").getRight());
					if (!keyValues.isEmpty())
						wayAreaTags.put(keyId, valueIds);
					break;
				default:
					throw new ParseException(-13);
			}
		}

		// hardcoded type=multipolygon for relations
		Iterator<Pair<Integer,Integer>> keyValuesIt = allKeyValues.get("type").values().iterator();
		if (keyValuesIt.hasNext()) {
			this.typeKey = keyValuesIt.next().getLeft();
			if (allKeyValues.get("type").containsKey("multipolygon"))
				this.typeMultipolygonValue = allKeyValues.get("type").get("multipolygon").getRight();
			else
				System.err.printf("DefaultTagInterpreter: key/value \"%s\"=\"%s\" not found in this db extract\n", "type", "multipolygon");
			if (allKeyValues.get("type").containsKey("boundary"))
				this.typeBoundaryValue = allKeyValues.get("type").get("boundary").getRight();
			else
				System.err.printf("DefaultTagInterpreter: key/value \"%s\"=\"%s\" not found in this db extract\n", "type", "boundary");
			if (allKeyValues.get("type").containsKey("route"))
				this.typeRouteValue = allKeyValues.get("type").get("route").getRight();
			else
				System.err.printf("DefaultTagInterpreter: key/value \"%s\"=\"%s\" not found in this db extract\n", "type", "route");
		} else {
			System.err.printf("DefaultTagInterpreter: key \"%s\" not found in this db extract\n", "type");
		}
		// we still need to also store relation area tags for isOldStyleMultipolygon() functionality!
		Map<Integer, Set<Integer>> relAreaTags = new TreeMap<>();
		Set<Integer> relAreaTagValues = new TreeSet<>();
		relAreaTagValues.add(this.typeMultipolygonValue);
		relAreaTagValues.add(this.typeBoundaryValue);
		relAreaTags.put(this.typeKey, relAreaTagValues);

		// list of uninteresting tags
		Set<Integer> uninterestingTagKeys = new HashSet<>();
		JSONArray uninterestingTagsList = (JSONArray)parser.parse(new FileReader(uninterestingTagsDefinitionFile));
		Iterator<String> uninterestingTagsIt = uninterestingTagsList.iterator();
		while (uninterestingTagsIt.hasNext()) {
			String tagKey = uninterestingTagsIt.next();
			if (!allKeyValues.containsKey(tagKey))
				continue; // silent here, as some of these tags are actually quite exotic
			Iterator<Pair<Integer, Integer>> keyIt = allKeyValues.get(tagKey).values().iterator();
			if (keyIt.hasNext()) {
				uninterestingTagKeys.add(keyIt.next().getLeft());
			}
		}

		this.wayAreaTags = wayAreaTags;
		this.relationAreaTags = relAreaTags;
		this.uninterestingTagKeys = uninterestingTagKeys;

		if (allKeyValues.containsKey("area") && allKeyValues.get("area").containsKey("no")) {
			this.areaNoTagKeyId = allKeyValues.get("area").get("no").getLeft();
			this.areaNoTagValueId = allKeyValues.get("area").get("no").getRight();
		}

		if (allRoles.containsKey("outer")) this.outerRoleId = allRoles.get("outer");
		if (allRoles.containsKey("inner")) this.innerRoleId = allRoles.get("inner");
		if (allRoles.containsKey(""))      this.emptyRoleId = allRoles.get("");
	}

	@Override
	public boolean evaluateForArea(OSMEntity osm) {
		if (osm instanceof OSMRelation) {
			return evaluateRelationForArea(osm.getTags());
		} else {
			return super.evaluateForArea(osm);
		}
	}

	@Override
	public boolean evaluateForLine(OSMEntity osm) {
		if (osm instanceof OSMRelation) {
			return evaluateRelationForLine(osm.getTags());
		} else {
			return super.evaluateForArea(osm);
		}
	}

	// checks if the relation has the tag "type=multipolygon"
	private boolean evaluateRelationForArea(int[] tags) {
		// skip area=no check, since that doesn't make much sense for multipolygon relations (does it??)
		// todo: replace with quicker binary search (tag keys are sorted)
		for (int i = 0; i < tags.length; i += 2) {
			if (tags[i] == typeKey)
				return tags[i + 1] == typeMultipolygonValue || tags[i + 1] == typeBoundaryValue;
		}
		return false;
	}

	// checks if the relation has the tag "type=route"
	private boolean evaluateRelationForLine(int[] tags) {
		// todo: replace with quicker binary search (tag keys are sorted)
		for (int i = 0; i < tags.length; i += 2) {
			if (tags[i] == typeKey)
				return tags[i + 1] == typeRouteValue;
		}
		return false;
	}
}
