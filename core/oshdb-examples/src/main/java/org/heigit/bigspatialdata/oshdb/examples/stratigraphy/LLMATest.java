package org.heigit.bigspatialdata.oshdb.examples.stratigraphy;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.Date;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
//import org.heigit.bigspatialdata.hosmdb.grid.HOSMCell;
//import org.heigit.bigspatialdata.hosmdb.osh.HOSMEntity;
//import org.heigit.bigspatialdata.hosmdb.osm.OSMEntity;
//import org.heigit.bigspatialdata.hosmdb.osm.OSMRelation;
//import org.heigit.bigspatialdata.hosmdb.osm.OSMWay;
//import org.heigit.bigspatialdata.hosmdb.util.BoundingBox;
//import org.heigit.bigspatialdata.hosmdb.util.Geo;
//import org.heigit.bigspatialdata.hosmdb.util.XYGrid;
//import org.heigit.bigspatialdata.hosmdb.util.tagInterpreter.DefaultTagInterpreter;
//import org.heigit.bigspatialdata.hosmdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.*;


import com.google.common.collect.Multiset.Entry;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.TopologyException;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class LLMATest {

	public static class ZoomId {
		public final int zoom;
		public final long id;

		public ZoomId(final int zoom, final long id) {
			this.zoom = zoom;
			this.id = id;
		}
	}

	public static class Result {
		public final int zoom;
		public final long id;
		public final long count;

		public Result(final int zoom, final long id, final long count) {
			this.zoom = zoom;
			this.id = id;
			this.count = count;
		}

		public Result reduce(Result other) {
			return new Result(zoom, id, count + other.count);
		}

		@Override
		public String toString() {
			return String.format("zoom:%d id:%d -> %d", zoom, id, count);
		}
	}

	private static final int MAXZOOM = 12;

	public static void main(String[] args)
			throws ClassNotFoundException, SQLException, IOException, org.json.simple.parser.ParseException {
		Class.forName("org.h2.Driver");

		// try (Connection conn =
		// DriverManager.getConnection("jdbc:h2:./hosmdb/src/main/resources/oshdb/kathmandu.oshdb",
		// "sa", "");
		try (Connection conn = DriverManager.getConnection(
				"jdbc:h2:tcp://localhost/~/Development/hosm_v2_workspace/OSH-BigDB/core/hosmdb/src/main/resources/oshdb/kathmandu.oshdb",
				"sa", ""); final Statement stmt = conn.createStatement()) {

			System.out.println("Select tag key/value ids from DB");
			ResultSet rstTags = stmt.executeQuery(
					"select k.ID as KEYID, kv.VALUEID as VALUEID, k.txt as KEY, kv.txt as VALUE from KEYVALUE kv inner join KEY k on k.ID = kv.KEYID;");
			Map<String, Map<String, Pair<Integer, Integer>>> allKeyValues = new HashMap<>();
			
			Map<Integer, Map<Integer, Pair<String, String>>> tagIntToString = new HashMap<>();
			
			while (rstTags.next()) {
						
				int keyId = rstTags.getInt(1);
				int valueId = rstTags.getInt(2);
				String keyStr = rstTags.getString(3);
				String valueStr = rstTags.getString(4);
				
				//build allKeyValues
				if (!allKeyValues.containsKey(keyStr))
					allKeyValues.put(keyStr, new HashMap<>());
				allKeyValues.get(keyStr).put(valueStr, new ImmutablePair<>(keyId, valueId));
				
				//build tagIntToString
				if (!tagIntToString.containsKey(keyId))
					tagIntToString.put(keyId, new HashMap<>());
				tagIntToString.get(keyId).put(valueId, new ImmutablePair<>(keyStr, valueStr));
			}
			rstTags.close();
			ResultSet rstRoles = stmt.executeQuery("select ID as ROLEID, txt as ROLE from ROLE;");
			Map<String, Integer> allRoles = new HashMap<>();
			while (rstRoles.next()) {
				int roleId = rstRoles.getInt(1);
				String roleStr = rstRoles.getString(2);
				allRoles.put(roleStr, roleId);
			}
			rstRoles.close();
			final TagInterpreter tagInterpreter = new DefaultTagInterpreter(allKeyValues, allRoles);

			List<ZoomId> zoomIds = new ArrayList<>();

			/*
			 * System.out.println("Select ids from DB"); ResultSet rst =
			 * stmt.executeQuery("select level,id from grid_way");
			 * while(rst.next()){
			 * //System.out.println("-- "+rst.getInt(1)+"/"+rst.getInt(2));
			 * zoomIds.add(new ZoomId(rst.getInt(1),rst.getLong(2))); }
			 * rst.close();
			 */

//			final BoundingBox bboxFilter = new BoundingBox(85, 86, 27.71, 27.75);
			final BoundingBox bboxFilter = new BoundingBox(84, 87, 26, 29);
			for (int zoom = 0; zoom <= MAXZOOM; zoom++) {
				XYGrid grid = new XYGrid(zoom);
				
				System.out.println("ZoomLevel: " + zoom + " CellWidth: " + grid.getCellWidth() + " linke untere Ecke: " + grid.getCellDimensions(grid.getId(85, 27)).getMaxValuesPerDimension()[1]);
				System.out.println(grid.bbox2CellIdRanges(bboxFilter, true));
				// Set<Pair<Long,Long>> cellIds =
				// grid.bbox2CellIdRanges(bboxFilter, true);
				Set<Pair<Long, Long>> cellIds = grid.bbox2CellIdRanges(bboxFilter, true);
				// Pair<Long,Long> llma = cellId.iterator().next();
				// System.out.println("oooo "+llma.toString());

				for (Pair<Long, Long> cellsInterval : cellIds) {
//					System.out.println("++" + cellsInterval.getLeft() + " " + cellsInterval.getRight());
					for (long cellId = cellsInterval.getLeft(); cellId <= cellsInterval.getRight(); cellId++) {
						// System.out.println("-- "+zoom+"/"+cellId);
						zoomIds.add(new ZoomId(zoom, cellId));
					}
				}
			}

			List<Long> timestamps;
			timestamps = new ArrayList<>();
			final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
			for (int year = 2017; year <= 2017; year++) {
				for (int month = 5; month <= 5; month++) {
					try {
						timestamps.add(formatter.parse(String.format("%d%02d01", year, month)).getTime());
					} catch (java.text.ParseException e) {
						System.err.println("basdoawrd");
					};
				}
			}
			
			System.out.println("Process in parallel");
			Optional<Map<Long, Integer>> totals = zoomIds.parallelStream().flatMap(zoomId -> {
				try (final PreparedStatement pstmt = conn.prepareStatement(
						"(select data from grid_node where level = ?1 and id = ?2)")) {
					pstmt.setInt(1, zoomId.zoom);
					pstmt.setLong(2, zoomId.id);
//					System.out.println("adsfasdf");
                    
					try (final ResultSet rst2 = pstmt.executeQuery()) {
//						System.out.println("exec query");

						List<> cells = new LinkedList<>();
						while (rst2.next()) {
							final ObjectInputStream ois = new ObjectInputStream(rst2.getBinaryStream(1));
							cells.add(() ois.readObject());
//							System.out.println("is da der Fehler");
//HOSMCell
						}
						return cells.stream();
					}
				} catch (IOException | SQLException | ClassNotFoundException e) {
					e.printStackTrace();
					return null;
				}
			}).map(hosmCell -> {
				final int zoom = hosmCell.getLevel();
				final long id = hosmCell.getId();

				Map<Long, Integer> counts = new HashMap<>(timestamps.size());
				Iterator<OSHEntity> oshEntitylIt = hosmCell.iterator();
				
				while (oshEntitylIt.hasNext()) {
					OSHEntity oshEntity = oshEntitylIt.next();
//					System.out.println("oder hier");

					Map<Long, OSMEntity> osmEntityByTimestamps = oshEntity.getByTimestamps(timestamps);
//					System.out.println("vielleicht das da");
					
					int zaehler = 0;
					for (Map.Entry<Long, OSMEntity> entity : osmEntityByTimestamps.entrySet()) {
						Long timestamp = entity.getKey();
						OSMEntity osmEntity = entity.getValue();
						
//						if (osmEntity.isVisible() && osmEntity.hasTagKey(0) && osmEntity.hasTagValue(0,0)) {
						
						Pair<Integer, Integer> myTag = allKeyValues.get("amenity").get("restaurant");
						int myKey = myTag.getLeft(); 
						
						int myValue = myTag.getRight();
						
//						System.out.println(myKey + " " + myValue + " " +  zaehler);
						++zaehler;
						if (osmEntity.isVisible() && osmEntity.hasTagValue(myKey,myValue)) {
						
						if(!counts.containsKey(timestamp)){
							counts.put(timestamp, 0);
						}
							counts.put(timestamp, counts.get(timestamp)+1);
							
							
							
							
//						int[] osmEntityTags = osmEntity.getTags(); 
//							for (int i = 0; i < osmEntityTags.length; i=i+2) {
//								int key = osmEntityTags[i];
//								int val = osmEntityTags[i+1];
//								
////								System.out.println("tagints: " + key + " " + val + " tag: " + tagIntToString.get(key).get(val).getLeft() + " "+  tagIntToString.get(key).get(val).getRight());
//								System.out.println("TimeStamp: " + timestamp);
//							}
							
						}
						else {
							// System.out.println(osmEntity.getTags()[0]);
						}
						
					}
					
				}

				String result = counts.entrySet().stream().map(entry -> entry.getKey() + " - " +  entry.getValue())
						.collect(Collectors.joining("\n"));
				
				Map<Long, Integer> sortedCounts = new LinkedHashMap<>();
				counts.entrySet().stream().sorted(Map.Entry.<Long, Integer>comparingByKey()).forEachOrdered(x -> sortedCounts.put(x.getKey(), x.getValue()));
				
	//			System.out.println(id +"\n\n"+ sortedCounts + "\n");
				
				System.out.println(counts);
				
				return counts;
				
			}).reduce((a, b) -> 
			{
				Map<Long, Integer> sum = new TreeMap<>();
//				Set<Long> ts = new HashSet<Long>();
//				ts.addAll(a.keySet());
//				ts.addAll(b.keySet());
//				for (Long t : ts) {
//					Integer aCnt = a.get(t);
//					Integer bCnt = b.get(t);
//					sum.put(t, (aCnt != null ? aCnt.Value() : 0.) + (bCnt != null ? bCnt.doubleValue() : 0.)
//					/*
//					 * Cnt == null ? bCnt.doubleValue() : ( (bCnt == null ?
//					 * aCnt.doubleValue() : 0.5*( aCnt.doubleValue() +
//					 * bCnt.doubleValue() )) )
//					 */
//					);
//				}
				return sum;
			}
			);

			for (Map.Entry<Long, Integer> total : totals.get().entrySet()) {
				System.out.printf("%s\t%f\n", formatter.format(new Date(total.getKey())), total.getValue());
			}

		}

	}

}
