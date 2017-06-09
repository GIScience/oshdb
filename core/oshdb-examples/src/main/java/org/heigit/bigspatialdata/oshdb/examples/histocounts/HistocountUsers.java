package org.heigit.bigspatialdata.oshdb.examples.histocounts;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.OSHDb;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.*;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.Geo;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class HistocountUsers {

  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, org.json.simple.parser.ParseException {

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
    //final BoundingBox bbox = new BoundingBox(8, 9, 49, 50);
    final BoundingBox bbox = new BoundingBox(75.98145, 99.53613, 14.71113, 38.73695);

    XYGridTree grid = new XYGridTree(OSHDb.MAXZOOM);

    final List<CellId> cellIds = new ArrayList<>();
    grid.bbox2CellIds(bbox, true).forEach(cellIds::add);

    // connect to the "Big"DB
    Connection conn = DriverManager.getConnection("jdbc:h2:./nepal","sa", "");
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


    Map<Long, Set<Integer>> countByTimestamp = cellIds.parallelStream().flatMap(cell -> {
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
      Map<Long, Set<Integer>> activeUsersOverTime = new HashMap<>(timestamps.size());
      for (long t : timestamps) {
        activeUsersOverTime.put(t, new HashSet<>());
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
        long validFromTimestamp = result.validFrom;
        OSMEntity osmEntity = result.osmEntity;
        Geometry geometry = result.geometry;

        // todo: geometry intersection with actual non-bbox area of interest

        Long timestamp = null;
        for (int i=timestamps.size()-1; i>=0; i--) {
          if (validFromTimestamp > timestamps.get(i)) {
            timestamp = timestamps.get(i);
            break;
          }
          if (i==0) return; // skip altogether if too old
        }

        Set<Integer> thisResult = activeUsersOverTime.get(timestamp);

        if (osmEntity.getTimestamp() == validFromTimestamp)
          thisResult.add(osmEntity.getUserId());

        if (!result.activities.contains(CellIterator.IterateAllEntry.ActivityType.DELETION) &&
            !result.activities.contains(CellIterator.IterateAllEntry.ActivityType.CREATION)) {// only do this if members were actually changed in this modification
          if (osmEntity instanceof OSMWay) {
            for (OSMMember m : ((OSMWay) osmEntity).getRefs()) {
              OSHNode oshEntity = (OSHNode) m.getEntity();

              for (OSMEntity n : oshEntity) {
                long ts = n.getTimestamp();
                if (ts == result.validFrom) {
                  thisResult.add(n.getUserId());
                }
              }
            }
          }
          if (osmEntity instanceof OSMRelation) {
            for (OSMMember m : ((OSMRelation) osmEntity).getMembers()) {
              OSHEntity oshEntity = m.getEntity();

              if (oshEntity instanceof OSHNode) {
                for (OSMNode node : (OSHNode) oshEntity) {
                  long ts = node.getTimestamp();
                  if (ts == result.validFrom) {
                    thisResult.add(node.getUserId());
                  }
                }
              } else if (oshEntity instanceof OSHWay) {
                for (OSMWay way : (OSHWay) oshEntity) {
                  long ts = way.getTimestamp();
                  if (ts == result.validFrom) {
                    thisResult.add(way.getUserId());
                    // recurse way nodes
                    for (OSMMember wm : way.getRefs()) {
                      OSHNode oshEntity2 = (OSHNode) wm.getEntity();

                      for (OSMEntity n : oshEntity2) {
                        long ts2 = n.getTimestamp();
                        if (ts2 == result.validFrom) {
                          thisResult.add(n.getUserId());
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }


        /*if (handleOldStyleMultipolygons &&
            osmEntity instanceof OSMRelation &&
            tagInterpreter.isOldStyleMultipolygon((OSMRelation)osmEntity)) {
          // special handling of old style multipolygons: don't count this as a separate entity, just subtract the
          // total hole(s) size of the polygon from the end result
          if (geometry instanceof Polygon)
            thisResult.area -= Geo.areaOf((Polygon) geometry);
          else
            thisResult.area -= Geo.areaOf((MultiPolygon) geometry);
        } else {
          // normal case: a regular point, line or (multi)polygon
          thisResult.countTotal++;
          switch (osmEntity.getType()) {
            case OSHEntity.NODE:
              thisResult.countTagChange++;
              break;
            case OSHEntity.WAY:
              thisResult.countDeletion++;
              break;
            case OSHEntity.RELATION:
              thisResult.countRelations++;
              break;
          }
          if (geometry.getGeometryType().startsWith("LineString")) {
            //System.err.printf("%s %s\n", formatter.format(new Date(timestamp*1000)), osmEntity.toString());
            //System.err.printf("%s\n", geometry.toString());
            thisResult.countCreation++;
            thisResult.length += Geo.distanceOf((LineString) geometry);
          } else if (geometry.getGeometryType().startsWith("Polygon")) {
            thisResult.countGeometryChange++;
            if (geometry instanceof Polygon)
              thisResult.area += Geo.areaOf((Polygon) geometry);
            else
              thisResult.area += Geo.areaOf((MultiPolygon) geometry);
          }
        }*/
      });

      return activeUsersOverTime;
    }).reduce(new HashMap<>(), (a, b) -> {
      Set<Long> ts = new HashSet<>();
      ts.addAll(a.keySet());
      ts.addAll(b.keySet());
      // make a copy of one of the intermediate results to aggregate results in
      Map<Long,Set<Integer>> combined = new HashMap<>(b);
      for (Long t : ts) {
        if (!combined.containsKey(t)) {
          combined.put(t, a.get(t));
        } else if (a.containsKey(t)) {
          Set<Integer> combinedData = new HashSet<>(combined.get(t));
          combinedData.addAll(a.get(t));
          combined.put(t,combinedData);
        } // third case: b already contains key, a result doesn't -> no need to do anything :)
      }
      return combined;
    });

    for (Map.Entry<Long,Set<Integer>> total : new TreeMap<>(countByTimestamp).entrySet()) {
      System.out.printf("%s\t%d\n",
          formatter.format(new Date(total.getKey()*1000)),
          total.getValue().size()
      );
      if (formatter.format(new Date(total.getKey()*1000)).equals("20161101")) {
        for (Integer uid : total.getValue()) {
          ;//System.err.println(uid);
        }
      }
    }

  }
}
