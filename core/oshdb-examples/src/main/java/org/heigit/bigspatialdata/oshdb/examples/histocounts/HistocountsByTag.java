package org.heigit.bigspatialdata.oshdb.examples.histocounts;

import com.vividsolutions.jts.geom.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
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

public class HistocountsByTag {

  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, org.json.simple.parser.ParseException {

    class ResultEntry {
      long countTotal;
      long countLinestrings;
      long countPolygons;
      long countNodes;
      long countWays;
      long countRelations;
      double length;
      double area;

      ResultEntry() {
        this.countTotal = 0;
        this.countLinestrings = 0;
        this.countPolygons = 0;
        this.countNodes = 0;
        this.countWays = 0;
        this.countRelations = 0;
        this.length = 0.0;
        this.area = 0.0;
      }

      public void add(ResultEntry other) {
        this.countTotal += other.countTotal;
        this.countLinestrings += other.countLinestrings;
        this.countPolygons += other.countPolygons;
        this.countNodes += other.countNodes;
        this.countWays += other.countWays;
        this.countRelations += other.countRelations;
        this.length += other.length;
        this.area += other.area;
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
    //final BoundingBox bbox = new BoundingBox(75.98145, 99.53613, 14.71113, 38.73695);
    final BoundingBox bbox = new BoundingBox(8, 9, 49, 50);
    //final BoundingBox bbox = new BoundingBox(86.8798, 86.96065, 27.95271, 28.03774);

    XYGridTree grid = new XYGridTree(OSHDB.MAXZOOM);

    final List<CellId> cellIds = new ArrayList<>();
    grid.bbox2CellIds(bbox, true).forEach(cellIds::add);

    // connect to the "Big"DB
    Connection conn = DriverManager.getConnection("jdbc:h2:./karlsruhe-regbez","sa", "");

    final TagInterpreter tagInterpreter = DefaultTagInterpreter.fromH2(conn);

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


    Map<Long,ResultEntry> countByTimestamp = cellIds.parallelStream().flatMap(cell -> {
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
      Map<Long, ResultEntry> counts = new HashMap<>(timestamps.size());

      //int interestedKeyId = allKeyValues.get("highway").get("residential").getLeft();
      //int[] uninterestedValueIds = { allKeyValues.get("highway").get("no").getRight() };
      /*int interestedKeyId = allKeyValues.get("wikidata").get("Q513").getLeft();
      int interestedValueId = allKeyValues.get("wikidata").get("Q513").getRight();
      int interestedKeyId2 = allKeyValues.get("natural").get("peak").getLeft();
      int interestedValueId2 = allKeyValues.get("natural").get("peak").getRight();
      int interestedKeyId3 = allKeyValues.get("name").get("Mount Everest").getLeft();
      int interestedValueId3 = allKeyValues.get("name").get("Mount Everest").getRight();
      int interestedKeyId4 = allKeyValues.get("name").get("Everest").getLeft();
      int interestedValueId4 = allKeyValues.get("name").get("Everest").getRight();*/
      CellIterator.iterateByTimestamps(
          oshCell,
          bbox,
          timestamps,
          tagInterpreter,
          osmEntity -> osmEntity.getId() == 254154168 && osmEntity instanceof OSMWay,
          /*osmEntity -> osmEntity.hasTagValue(interestedKeyId, interestedValueId) || (
              osmEntity.hasTagValue(interestedKeyId2, interestedValueId2) && (
                  osmEntity.hasTagValue(interestedKeyId3, interestedValueId3) ||
                  osmEntity.hasTagValue(interestedKeyId4, interestedValueId4)
              )
          ),*/
          handleOldStyleMultipolygons
      )
      .forEach(result -> {
        result.entrySet().forEach(entry -> {
          long timestamp = entry.getKey();
          OSMEntity osmEntity = entry.getValue().getLeft();
          Geometry geometry = entry.getValue().getRight();

          if (formatter.format(new Date(timestamp*1000)).equals("20130601")) {
            System.out.println("####");
            System.out.println(oshCell.getId());
            System.out.println(osmEntity.getId());
          }

          // todo: geometry intersection with actual non-bbox area of interest



          if (!counts.containsKey(timestamp)) {
            counts.put(timestamp, new ResultEntry());
          }
          ResultEntry thisResult = counts.get(timestamp);

          if (handleOldStyleMultipolygons &&
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
                thisResult.countNodes++;
                break;
              case OSHEntity.WAY:
                thisResult.countWays++;
                break;
              case OSHEntity.RELATION:
                thisResult.countRelations++;
                break;
            }
            if (geometry.getGeometryType().startsWith("LineString")) {
              thisResult.countLinestrings++;
              thisResult.length += Geo.lengthOf((LineString) geometry);
            } else if (geometry.getGeometryType().startsWith("Polygon")) {
              thisResult.countPolygons++;
              if (geometry instanceof Polygon)
                thisResult.area += Geo.areaOf((Polygon) geometry);
              else
                thisResult.area += Geo.areaOf((MultiPolygon) geometry);
            }
          }
        });
      });

      return counts;
    }).reduce(new HashMap<>(), (a, b) -> {
      Set<Long> ts = new HashSet<>();
      ts.addAll(a.keySet());
      ts.addAll(b.keySet());
      // make a copy of one of the intermediate results to aggregate results in
      Map<Long,ResultEntry> combined = new HashMap<>(b);
      for (Long t : ts) {
        if (!combined.containsKey(t))
          combined.put(t, a.get(t));
        else if (a.containsKey(t)) {
          combined.get(t).add(a.get(t));
        } // third case: b already contains key, a result doesn't -> no need to do anything :)
      }
      return combined;
    });

    for (Map.Entry<Long,ResultEntry> total : new TreeMap<>(countByTimestamp).entrySet()) {
      System.out.printf("%s\t%d\t%d\t%d\t%d\t%d\t%f\t%d\t%f\n",
          formatter.format(new Date(total.getKey()*1000)),
          total.getValue().countTotal,
          total.getValue().countNodes,
          total.getValue().countWays,
          total.getValue().countRelations,
          total.getValue().countLinestrings,
          total.getValue().length,
          total.getValue().countPolygons,
          total.getValue().area
      );
    }

  }
}
