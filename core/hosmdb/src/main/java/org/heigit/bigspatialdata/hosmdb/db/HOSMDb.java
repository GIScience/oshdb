package org.heigit.bigspatialdata.hosmdb.db;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.heigit.bigspatialdata.hosmdb.osh.HOSMNode;
import org.heigit.bigspatialdata.hosmdb.osh.HOSMWay;
import org.heigit.bigspatialdata.hosmdb.osm.OSMNode;
import org.heigit.bigspatialdata.hosmdb.osm.OSMWay;

public class HOSMDb {
	
	
	public static Map<Long, OSMNode> getByTimestamp(HOSMNode node, List<Long> byTimestamps){
		return null;
	}
	public static Map<Long, OSMWay> getByTimestamp(HOSMWay hosm, List<Long> byTimestamps){
		try {
			if (byTimestamps != null && byTimestamps.size() > 1)
				Collections.sort(byTimestamps, Collections.reverseOrder());
			
			Map<Long, OSMWay> result = new TreeMap<>();
			Iterator<OSMWay> itr = hosm.iterator();
			
			int currentTS = 0;
			while(itr.hasNext()){
				OSMWay osm = itr.next();
				if (byTimestamps != null && !byTimestamps.isEmpty()) {
					while (currentTS < byTimestamps.size()) {
						if (byTimestamps.get(currentTS).longValue() > osm.getTimestamp()) {
							result.put(byTimestamps.get(currentTS), osm);
							currentTS++;
						} else {
							break;
						}
					}
					if (currentTS >= byTimestamps.size())
						return result;
				} else {
					result.put(Long.valueOf(osm.getTimestamp()), osm);
				}
			}
			return result;
		}catch(Exception e){
			//TODO log the exception
		}
		return null;
	}
}
