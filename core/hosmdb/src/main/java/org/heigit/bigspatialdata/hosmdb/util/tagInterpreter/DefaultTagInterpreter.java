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
	private int typeRouteValue = -1;

	public DefaultTagInterpreter(Map<String, Map<String, Pair<Integer, Integer>>> allKeyValues) throws IOException, ParseException {
		this(Thread.currentThread().getContextClassLoader().getResource("json/polygon-features.json").getPath(), allKeyValues); // todo: does this work=??? path?
	}

	public DefaultTagInterpreter(String jsonDefinitionFile, Map<String, Map<String, Pair<Integer, Integer>>> allKeyValues) throws FileNotFoundException, IOException, ParseException {
		super(0,0, null, null); // initialize with dummy parameters for now

		Map<Integer, Set<Integer>> wayAreaTags = new HashMap<>();

		JSONParser parser = new JSONParser();
		JSONArray tagList = (JSONArray)parser.parse(new FileReader(jsonDefinitionFile));
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
			if (allKeyValues.get("type").containsKey("route"))
				this.typeRouteValue = allKeyValues.get("type").get("route").getRight();
		}

		this.areaNoTagKey   = allKeyValues.get("area").get("no").getLeft();
		this.areaNoTagValue = allKeyValues.get("area").get("no").getRight();
		this.wayAreaTags = wayAreaTags;
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
			if (tags[i] == typeKey && tags[i + 1] == typeMultipolygonValue)
				return true;
		}
		return false;
	}

	// checks if the relation has the tag "type=route"
	private boolean evaluateRelationForLine(int[] tags) {
		// todo: replace with quicker binary search (tag keys are sorted)
		for (int i = 0; i < tags.length; i += 2) {
			if (tags[i] == typeKey && tags[i + 1] == typeRouteValue)
				return true;
		}
		return false;
	}
}
