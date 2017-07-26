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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;

import com.vividsolutions.jts.geom.Geometry;

public class TestMultipolygonGeometry2 {

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



  private static final int MAXZOOM = OSHDB.MAXZOOM;



  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, org.json.simple.parser.ParseException {
    Class.forName("org.h2.Driver");

    try (Connection conn = DriverManager.getConnection("jdbc:h2:./heidelberg", "sa", "");
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
      final TagInterpreter tagInterpreter = new DefaultTagInterpreter(allKeyValues, allRoles);


      List<ZoomId> zoomIds = new ArrayList<>();

			/*System.out.println("Select ids from DB");
			ResultSet rst = stmt.executeQuery("select level,id from grid_way");
			while(rst.next()){
				//System.out.println("-- "+rst.getInt(1)+"/"+rst.getInt(2));
				zoomIds.add(new ZoomId(rst.getInt(1),rst.getLong(2)));
			}
			rst.close();*/

      final BoundingBox bboxFilter = new BoundingBox(8.65092, 8.65695, 49.38681, 49.39091);
      //final BoundingBox bboxFilter = new BoundingBox(8, 9, 49, 50);
      for (int zoom = 0; zoom<= MAXZOOM; zoom++) {
        XYGrid grid = new XYGrid(zoom);
        Set<Pair<Long,Long>> cellIds = grid.bbox2CellIdRanges(bboxFilter, true);
        for (Pair<Long,Long> cellsInterval : cellIds) {
          for (long cellId=cellsInterval.getLeft(); cellId<=cellsInterval.getRight(); cellId++) {
            //System.out.println("-- "+zoom+"/"+cellId);
            zoomIds.add(new ZoomId(zoom, cellId));
          }
        }
      }


      List<Long> timestamps;
      timestamps = new ArrayList<>();
      final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
      for (int year = 2004; year <= 2018; year++) {
        for(int month = 1; month <= 12; month++) {
          try {
            timestamps.add(formatter.parse(String.format("%d%02d01", year, month)).getTime() / 1000);
          } catch (java.text.ParseException e) {
            System.err.println("basdoawrd");
          }
        }
      }


      System.out.println("Process in parallel");
      Optional<Map<Long, Double>> totals = zoomIds.parallelStream()
          .flatMap(zoomId -> {
            try(final PreparedStatement pstmt = conn.prepareStatement("(select data from grid_relation where level = ?1 and id = ?2) union (select data from grid_way where level = ?1 and id = ?2)")){
            //try(final PreparedStatement pstmt = conn.prepareStatement("(select data from grid_relation where level = ?1 and id = ?2)")){
              pstmt.setInt(1,zoomId.zoom);
              pstmt.setLong(2, zoomId.id);

              try(final ResultSet rst2 = pstmt.executeQuery()){
                List<GridOSHEntity> cells = new LinkedList<>();
                while(rst2.next()){
                  final ObjectInputStream ois = new ObjectInputStream(rst2.getBinaryStream(1));
                  cells.add((GridOSHEntity) ois.readObject());
                }
                return cells.stream();
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

            int interestedKeyId = allKeyValues.get("building").get("yes").getLeft();
            int[] uninterestedValueIds = { allKeyValues.get("building").get("no").getRight() };
            CellIterator.iterateByTimestamps(hosmCell, bboxFilter, timestamps, tagInterpreter, osmEntity -> osmEntity.hasTagKey(interestedKeyId, uninterestedValueIds), false)
            .forEach(result -> {
              result.entrySet().forEach(entry -> {
                long timestamp = entry.getKey();
                OSMEntity osmEntity = entry.getValue().getLeft();
                Geometry geometry = entry.getValue().getRight();

                counts.put(timestamp, counts.getOrDefault(timestamp, 0.0) + 1);
              });
            });

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
        System.out.printf("%s\t%f\n", formatter.format(new Date(total.getKey()*1000)), total.getValue());
      }

    }

  }



}
