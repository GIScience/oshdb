package org.heigit.bigspatialdata.oshdb.etl;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

import com.vividsolutions.jts.geom.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.osh.*;
import org.heigit.bigspatialdata.oshdb.osm.*;
import org.heigit.bigspatialdata.oshdb.util.Geo;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class TestMultithreadDBProcessingAndGeometryCalculations {

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
		
		public Result(final int zoom, final long id, final long count){
			this.zoom = zoom;
			this.id = id;
			this.count = count;
		}
		
		public Result reduce(Result other){
			return new Result(zoom,id,count+other.count);
		}
		
		@Override
		public String toString() {
			return String.format("zoom:%d id:%d -> %d", zoom,id,count);
		}
	}
	
	

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, org.json.simple.parser.ParseException {
		Class.forName("org.h2.Driver");

	    try (Connection conn = DriverManager.getConnection("jdbc:h2:./hosmdb", "sa", "");
	            final Statement stmt = conn.createStatement()) {

			System.out.println("Select tag key/value ids from DB");
			ResultSet rstTags = stmt.executeQuery("select k.ID as KEYID, kv.VALUEID as VALUEID, k.txt as KEY, kv.txt as VALUE from KEYVALUE kv inner join KEY k on k.ID = kv.KEYID;");
			Map<String, Map<String, Pair<Integer, Integer>>> allKeyValues = new HashMap<>();
			while(rstTags.next()){
				int keyId   = rstTags.getInt(1);
				int valueId = rstTags.getInt(2);
				String keyStr   = rstTags.getString(3);
				String valueStr = rstTags.getString(4);
				if (!allKeyValues.containsKey(keyStr)) allKeyValues.put(keyStr, new HashMap<>());
				allKeyValues.get(keyStr).put(valueStr, new ImmutablePair<>(keyId, valueId));
			}
			rstTags.close();
			ResultSet rstRoles = stmt.executeQuery("select ID as ROLEID, txt as ROLE from ROLE;");
			Map<String, Integer> allRoles = new HashMap<>();
			while(rstRoles.next()){
				int roleId = rstRoles.getInt(1);
				String roleStr = rstRoles.getString(2);
				allRoles.put(roleStr, roleId);
			}
			rstRoles.close();
			final TagInterpreter areaDecider = new DefaultTagInterpreter(allKeyValues, allRoles);


	    	List<ZoomId> zoomIds = new ArrayList<>();
	    	
	    	System.out.println("Select ids from DB");
	    	ResultSet rst = stmt.executeQuery("select level,id from grid_way");
	    	while(rst.next()){
	    		zoomIds.add(new ZoomId(rst.getInt(1),rst.getLong(2)));
	    	}
	    	rst.close();


			List<Long> timestamps;
			timestamps = new ArrayList<>();
			final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
			for (int year = 2004; year <= 2018; year++) {
				for(int month = 1; month <= 12; month++){
					try {
						timestamps.add(formatter.parse(String.format("%d%02d01", year, month)).getTime());
					} catch(java.text.ParseException e) {
						System.err.println("basdoawrd");
					};
				}
			}
	    	
	    	
	    	System.out.println("Process in parallel");
			Optional<Map<Long, Double>> totals = zoomIds.parallelStream()
			.map(zoomId -> {
				try(final PreparedStatement pstmt = conn.prepareStatement("select data from grid_way where level = ? and id = ?")){
				pstmt.setInt(1,zoomId.zoom);
				pstmt.setLong(2, zoomId.id);

					try(final ResultSet rst2 = pstmt.executeQuery()){
						if(rst2.next()){
							final ObjectInputStream ois = new ObjectInputStream(rst2.getBinaryStream(1));
							final GridOSHWays hosmCell = (GridOSHWays) ois.readObject();
							return hosmCell;
						} else {
							System.err.printf("ERROR: no result for level:%d and id:%d\n",zoomId.zoom,zoomId.id);
							return null;
						}
					}
				} catch (IOException | SQLException | ClassNotFoundException e) {
					e.printStackTrace();
					return null;
				}
			})
			.map(hosmCell -> {
				final int zoom = hosmCell.getLevel();
				final long id = hosmCell.getId();

				Map<Long, Double> counts = new HashMap<>(timestamps.size());
				Iterator<OSHWay> oshWayIt = hosmCell.iterator();
				while(oshWayIt.hasNext()) {
					OSHWay oshWay = (OSHWay)oshWayIt.next();

					Map<Long,OSMWay> osmWays = oshWay.getByTimestamps(timestamps);
					for (Map.Entry<Long,OSMWay> way : osmWays.entrySet()) {
						Long timestamp = way.getKey();
						OSMWay osmWay = way.getValue();
						if (osmWay.isVisible() && osmWay.hasTagKey(1) /*&& osmWay.hasTagValue(1,0)*/) {

							double dist = 0.;
							try {
								Geometry geom = osmWay.getGeometry(timestamp, areaDecider);
								if (geom == null) throw new NotImplementedException(); // hack!
								if (geom.getGeometryType() != "LineString") throw new NotImplementedException(); // hack!

								LineString line = (LineString) geom;

								dist += Geo.lengthOf(line);


							} catch(NotImplementedException err) {}

							//dist = dist/(nds.length-1);

							Double prevCnt = counts.get(timestamp);
							//counts.put(timestamp, prevCnt != null ? 0.5*(prevCnt.doubleValue() + dist) : dist);
							counts.put(timestamp, prevCnt != null ? prevCnt.doubleValue() + dist : dist);
						}
					}
				}
				return counts;
			})
			.reduce((a,b) -> {
				Map<Long, Double> sum = new TreeMap<>();
				Set<Long> ts = new HashSet<Long>();
				ts.addAll(a.keySet());
				ts.addAll(b.keySet());
				for (Long t : ts) {
					Double aCnt = a.get(t);
					Double bCnt = b.get(t);
					sum.put(t,
						(aCnt != null ? aCnt.doubleValue() : 0.) +
						(bCnt != null ? bCnt.doubleValue() : 0.)
						/*Cnt == null ? bCnt.doubleValue() : (
							(bCnt == null ? aCnt.doubleValue() : 0.5*(
								aCnt.doubleValue() + bCnt.doubleValue()
							))
						)*/
					);
				}
				return sum;
			});

			for (Map.Entry<Long, Double> total : totals.get().entrySet()) {
				System.out.printf("%s\t%f\n", formatter.format(new Date(total.getKey())), total.getValue());
			}
	    	
	    }

	}



}
