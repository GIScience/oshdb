package org.heigit.bigspatialdata.oshdb.examples.histocounts;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.OSHDb;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osm.*;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class HistocountActivityTypes {

  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, org.json.simple.parser.ParseException {

    class ResultActivityEntry {
      long countTotal;
      long countCreation;
      long countTagChange;
      long countMemberChange;
      long countGeometryChange;
      long countDeletion;

      ResultActivityEntry() {
        this.countTotal = 0;
        this.countCreation = 0;
        this.countTagChange = 0;
        this.countMemberChange = 0;
        this.countGeometryChange = 0;
        this.countDeletion = 0;
      }

      public void add(ResultActivityEntry other) {
        this.countTotal += other.countTotal;
        this.countCreation += other.countCreation;
        this.countTagChange += other.countTagChange;
        this.countMemberChange += other.countMemberChange;
        this.countGeometryChange += other.countGeometryChange;
        this.countDeletion += other.countDeletion;
      }
    }

    boolean handleOldStyleMultipolygons = false;

    List<Long> timestamps = new ArrayList<>();
    final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    for (int year = 2005; year <= 2017; year++) {
      for (int month = 1; month <= 12; month++) {
        try {
          timestamps.add(formatter.parse(String.format("%d%02d01", year, month)).getTime() / 1000);
        } catch (java.text.ParseException e) {
          System.err.println(e.toString());
        }
      }
    }

    //final BoundingBox bbox = new BoundingBox(8.61, 8.76, 49.40, 49.41);
    //final BoundingBox bbox = new BoundingBox(8.65092, 8.65695, 49.38681, 49.39091);
    final BoundingBox bbox = new BoundingBox(8, 9, 49, 50);
    //final BoundingBox bbox = new BoundingBox(75.98145, 99.53613, 14.71113, 38.73695);

    XYGridTree grid = new XYGridTree(OSHDb.MAXZOOM);

    final List<CellId> cellIds = new ArrayList<>();
    grid.bbox2CellIds(bbox, true).forEach(cellIds::add);

    // connect to the "Big"DB
    Connection conn = DriverManager.getConnection("jdbc:h2:./karlsruhe-regbez","sa", "");
    final Statement stmt = conn.createStatement();

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


    Map<Long, ResultActivityEntry> countByTimestamp = cellIds.parallelStream().flatMap(cell -> {
      try (final PreparedStatement pstmt = conn.prepareStatement("" +
          "(select data from grid_node where level = ?1 and id = ?2) union " +
          "(select data from grid_way where level = ?1 and id = ?2) union " +
          "(select data from grid_relation where level = ?1 and id = ?2)"
      )) {
        pstmt.setInt(1, cell.getZoomLevel());
        pstmt.setLong(2, cell.getId());

        try (final ResultSet rst2 = pstmt.executeQuery()) {
          List<GridOSHEntity> cells = new LinkedList<>();
          while (rst2.next()) {
            final ObjectInputStream ois = new ObjectInputStream(rst2.getBinaryStream(1));
            cells.add((GridOSHEntity) ois.readObject());
          }
          return cells.stream();
        }
      } catch (IOException | SQLException | ClassNotFoundException e) {
        e.printStackTrace();
        return null;
      }
    }).map(oshCell -> {
      Map<Long, ResultActivityEntry> activitiesOverTime = new HashMap<>(timestamps.size());
      for (long t : timestamps) {
        activitiesOverTime.put(t, new ResultActivityEntry());
      }

      //int interestedKeyId = allKeyValues.get("landuse").get("residential").getLeft();
      int interestedKeyId = allKeyValues.get("building").get("yes").getLeft();
      //int interestedValueId = allKeyValues.get("building").get("yes").getRight();
      //int[] uninterestedValueIds = { allKeyValues.get("building").get("no").getRight() };
      CellIterator.iterateAll(
          oshCell,
          bbox,
          tagInterpreter,
          //osmEntity -> true,
          osmEntity -> osmEntity.hasTagKey(interestedKeyId),
          //osmEntity -> osmEntity.hasTagKey(interestedKeyId, uninterestedValueIds),
          //osmEntity -> osmEntity.hasTagValue(interestedKeyId, interestedValueId),
          handleOldStyleMultipolygons
      )
      .forEach(result -> {
        long timestamp = result.validFrom;
        OSMEntity osmEntity = result.osmEntity;
        Geometry geometry = result.geometry;


        // todo: geometry intersection with actual non-bbox area of interest


        for (int i=timestamps.size()-1; i>=0; i--) {
          if (timestamp > timestamps.get(i)) {
            timestamp = timestamps.get(i);
            break;
          }
          if (i==0) return; // skip altogether if too old
        }

        ResultActivityEntry thisResult = activitiesOverTime.get(timestamp);


        thisResult.countTotal++;
        if (result.activities.contains(CellIterator.IterateAllEntry.ActivityType.CREATION))
          thisResult.countCreation++;
        if (result.activities.contains(CellIterator.IterateAllEntry.ActivityType.DELETION))
          thisResult.countDeletion++;
        if (result.activities.contains(CellIterator.IterateAllEntry.ActivityType.TAG_CHANGE))
          thisResult.countTagChange++;
        if (result.activities.contains(CellIterator.IterateAllEntry.ActivityType.MEMBERLIST_CHANGE))
          thisResult.countMemberChange++;
        if (result.activities.contains(CellIterator.IterateAllEntry.ActivityType.GEOMETRY_CHANGE))
          thisResult.countGeometryChange++;



      });

      return activitiesOverTime;
    }).reduce(new HashMap<>(), (a, b) -> {
      Set<Long> ts = new HashSet<>();
      ts.addAll(a.keySet());
      ts.addAll(b.keySet());
      // make a copy of one of the intermediate results to aggregate results in
      Map<Long,ResultActivityEntry> combined = new HashMap<>(b);
      for (Long t : ts) {
        if (!combined.containsKey(t))
          combined.put(t, a.get(t));
        else if (a.containsKey(t)) {
          combined.get(t).add(a.get(t));
        } // third case: b already contains key, a result doesn't -> no need to do anything :)
      }
      return combined;
    });

    for (Map.Entry<Long,ResultActivityEntry> total : new TreeMap<>(countByTimestamp).entrySet()) {
      System.out.printf("%s\t%d\t%d\t%d\t%d\t%d\t%d\n",
          formatter.format(new Date(total.getKey()*1000)),
          total.getValue().countTotal,
          total.getValue().countCreation,
          total.getValue().countDeletion,
          total.getValue().countTagChange,
          total.getValue().countMemberChange,
          total.getValue().countGeometryChange
      );
    }

  }
}
