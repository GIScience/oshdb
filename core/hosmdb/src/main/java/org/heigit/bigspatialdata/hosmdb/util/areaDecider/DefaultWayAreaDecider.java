package org.heigit.bigspatialdata.hosmdb.util.areaDecider;

import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.hosmdb.util.areaDecider.AreaDecider;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Default AreaDecider
 */
public class DefaultWayAreaDecider extends AreaDecider {

	public DefaultWayAreaDecider(Map<String, Map<String, Pair<Integer, Integer>>> allKeyValues) throws IOException, ParseException {
		this(Thread.currentThread().getContextClassLoader().getResource("json/polygon-features.json").getPath(), allKeyValues); // todo: does this work=??? path?
	}

	public DefaultWayAreaDecider(String jsonDefinitionFile, Map<String, Map<String, Pair<Integer, Integer>>> allKeyValues) throws FileNotFoundException, IOException, ParseException {
		super(0,0, null); // initialize with dummy parameters for now

		Map<Integer, Set<Integer>> areaTags = new HashMap<>();

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
						System.err.printf("DefaultWayAreaDecider: key \"%s\" not found in this db extract\n", key);
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
							System.err.printf("DefaultWayAreaDecider: key/value \"%s\"=\"%s\" not found in this db extract\n", key, value);
							continue;
						}
						valueIds.add(keyValues.get(value).getRight());
						keyId = keyValues.get(value).getLeft();
					}
					if (!keyValues.isEmpty())
						areaTags.put(keyId, valueIds);
					break;
				case "all":
					valueIds = new InvertedHashSet<>();
					key = (String)tag.get("key");
					if (!allKeyValues.containsKey(key)) {
						// no such tag key present in this db extract
						System.err.printf("DefaultWayAreaDecider: key \"%s\" not found in this db extract\n", key);
						continue;
					}
					keyValues = allKeyValues.get(key);
					Iterator<Pair<Integer,Integer>> keyValuesIt = keyValues.values().iterator();
					if (!keyValuesIt.hasNext()) {
						// no such tag value present int his db extract??
						System.err.printf("DefaultWayAreaDecider: key \"%s\" not found in this db extract\n", key);
						continue;
					}
					keyId = keyValuesIt.next().getLeft();
					areaTags.put(keyId, valueIds);
					break;
				case "blacklist":
					valueIds = new InvertedHashSet<>();
					key = (String)tag.get("key");
					if (!allKeyValues.containsKey(key)) {
						// no such tag key present in this db extract
						System.err.printf("DefaultWayAreaDecider: key \"%s\" not found in this db extract\n", key);
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
							System.err.printf("DefaultWayAreaDecider: key/value \"%s\"=\"%s\" not found in this db extract\n", key, value);
							continue;
						}
						valueIds.add(keyValues.get(value).getRight());
						keyId = keyValues.get(value).getLeft();
					}
					if (!keyValues.isEmpty())
						areaTags.put(keyId, valueIds);
					break;
				default:
					throw new ParseException(-13);
			}
		}

		this.areaNoTagKey   = allKeyValues.get("area").get("no").getLeft();
		this.areaNoTagValue = allKeyValues.get("area").get("no").getRight();
		this.areaTags = areaTags;
	}
}
