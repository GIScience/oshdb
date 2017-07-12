package org.heigit.bigspatialdata.oshdb.examples.histocounts;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
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

public class HistocountActivityTypes {

  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, org.json.simple.parser.ParseException {

    class ResultActivityEntry {
      double countTotal;
      double countCreation;
      double countTagChange;
      double countMemberChange;
      double countGeometryChange;
      double countGeometryChangeDiff;
      double countDeletion;

      ResultActivityEntry() {
        this.countTotal = 0;
        this.countCreation = 0;
        this.countTagChange = 0;
        this.countMemberChange = 0;
        this.countGeometryChange = 0;
        this.countGeometryChangeDiff = 0;
        this.countDeletion = 0;
      }

      public void add(ResultActivityEntry other) {
        this.countTotal += other.countTotal;
        this.countCreation += other.countCreation;
        this.countTagChange += other.countTagChange;
        this.countMemberChange += other.countMemberChange;
        this.countGeometryChange += other.countGeometryChange;
        this.countGeometryChangeDiff += other.countGeometryChangeDiff;
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
    //final BoundingBox bbox = new BoundingBox(8, 9, 49, 50);
    //final BoundingBox bbox = new BoundingBox(75.98145, 99.53613, 14.71113, 38.73695);
    //final BoundingBox bbox = new BoundingBox(86.8798, 86.96065, 27.95271, 28.03774);
    //final BoundingBox bbox = new BoundingBox(86.92209, 86.92535, 27.9857, 27.98805);
    //final BoundingBox bbox = new BoundingBox(-1, 1, 51, 52);
    //final BoundingBox bbox = new BoundingBox(18.3592, 18.6349, -34.0009, -33.857); // cape town
    //final BoundingBox bbox = new BoundingBox(26.98, 29.56, -30.72, -28.33); // lesotho
    //final BoundingBox bbox = new BoundingBox(-26.7, 53.1, -37.4, 37.5); // africa
    final BoundingBox bbox = new BoundingBox(42.4, 51.2,  -25.8, -11.8); // madagascar

    XYGridTree grid = new XYGridTree(OSHDB.MAXZOOM);

    final List<CellId> cellIds = new ArrayList<>();
    grid.bbox2CellIds(bbox, true).forEach(cellIds::add);

    // connect to the "Big"DB
    final Connection connKT = DriverManager.getConnection("jdbc:h2:./africaKeytables","sa", "");

    final TagInterpreter tagInterpreter = DefaultTagInterpreter.fromH2(connKT);

    final Statement stmtKT = connKT.createStatement();

    System.out.println("Select tag key/value ids from DB");
    ResultSet rstTags = stmtKT.executeQuery("select k.ID as KEYID, kv.VALUEID as VALUEID, k.txt as KEY, kv.txt as VALUE from KEYVALUE kv inner join KEY k on k.ID = kv.KEYID;");
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


    final Connection conn = DriverManager.getConnection("jdbc:h2:./africa","sa", "");

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
      //int interestedValueId = allKeyValues.get("building").get("yes").getRight();
      int interestedKeyId = allKeyValues.get("highway").get("residential").getLeft();
      //int interestedValueId = allKeyValues.get("highway").get("residential").getRight();
      int[] uninterestedValueIds = {
          allKeyValues.get("highway").get("no").getRight(),
          allKeyValues.get("highway").get("path").getRight(),
          allKeyValues.get("highway").get("footway").getRight(),
          allKeyValues.get("highway").get("cycleway").getRight(),
          allKeyValues.get("highway").get("pedestrian").getRight(),
          //allKeyValues.get("highway").get("track").getRight(),
          //allKeyValues.get("highway").get("service").getRight()
      };
      int nameKeyId = allKeyValues.get("name").get("Madagascar").getLeft();
      int refKeyId = allKeyValues.get("ref").get("N 7").getLeft();
      int primaryValueId = allKeyValues.get("highway").get("primary").getRight();
      int trackValueId = allKeyValues.get("highway").get("track").getRight();
      /*int interestedKeyId = allKeyValues.get("wikidata").get("Q513").getLeft();
      int interestedValueId = allKeyValues.get("wikidata").get("Q513").getRight();
      int interestedKeyId2 = allKeyValues.get("natural").get("peak").getLeft();
      int interestedValueId2 = allKeyValues.get("natural").get("peak").getRight();
      int interestedKeyId3 = allKeyValues.get("name").get("Mount Everest").getLeft();
      int interestedValueId3 = allKeyValues.get("name").get("Mount Everest").getRight();
      int interestedKeyId4 = allKeyValues.get("name").get("Everest").getLeft();
      int interestedValueId4 = allKeyValues.get("name").get("Everest").getRight();*/
      CellIterator.iterateAll(
          oshCell,
          bbox,
          tagInterpreter,
          //osmEntity -> osmEntity.getId() == 88962805 && osmEntity instanceof OSMWay,
          //osmEntity -> true,
          //osmEntity -> osmEntity.hasTagKey(interestedKeyId),
          //osmEntity -> osmEntity.hasTagValue(interestedKeyId, interestedValueId),
          osmEntity -> osmEntity.hasTagKey(interestedKeyId, uninterestedValueIds),//&& (osmEntity.hasTagKey(nameKeyId) || osmEntity.hasTagKey(refKeyId)),//&& osmEntity.hasTagValue(interestedKeyId, primaryValueId),
          /*osmEntity -> osmEntity.hasTagValue(interestedKeyId, interestedValueId) || (
              osmEntity.hasTagValue(interestedKeyId2, interestedValueId2) && (
                  osmEntity.hasTagValue(interestedKeyId3, interestedValueId3) ||
                  osmEntity.hasTagValue(interestedKeyId4, interestedValueId4)
              )
          ),*/
          handleOldStyleMultipolygons
      )
      .forEach(result -> {
        //todo: replace this with grouping by changeset id?!!?! -> maybe in iterateAll()!
        long timestamp = result.validFrom;
        OSMEntity osmEntity = result.osmEntity;
        Geometry geometry = result.geometry;

        /*System.out.printf("---> [%s] %s - %s: %s\n",
            result.activities.toString(),
            formatter.format(new Date(result.validFrom*1000)),
            result.validTo != null ? formatter.format(new Date(result.validTo*1000)) : "/",
            osmEntity.toString()
        );*/

        double length = 0;
        if (result.activities.contains(CellIterator.IterateAllEntry.ActivityType.DELETION))
          geometry = result.previousGeometry;
        if (geometry instanceof MultiLineString)
          length = Geo.distanceOf((MultiLineString)geometry);
        else if (geometry instanceof LineString)
          length = Geo.distanceOf((LineString)geometry);
        //length = 1.0;

        // todo: geometry intersection with actual non-bbox area of interest


        for (int i=timestamps.size()-1; i>=0; i--) {
          if (timestamp > timestamps.get(i)) {
            timestamp = timestamps.get(i);
            break;
          }
          if (i==0) return; // skip altogether if too old
        }

        ResultActivityEntry thisResult = activitiesOverTime.get(timestamp);

        if (result.activities.equals(EnumSet.of(CellIterator.IterateAllEntry.ActivityType.GEOMETRY_CHANGE)))
          thisResult.countTotal += (result.validTo != null) ? length * Math.min(result.validTo-result.validFrom, 60*60*24) / (60*60*24) : length; //todo: replace this with grouping by changeset id?!!?!
        else
          thisResult.countTotal += length;

        if (result.activities.contains(CellIterator.IterateAllEntry.ActivityType.CREATION))
          thisResult.countCreation += length;
        if (result.activities.contains(CellIterator.IterateAllEntry.ActivityType.DELETION))
          thisResult.countDeletion += length;
        if (result.activities.contains(CellIterator.IterateAllEntry.ActivityType.TAG_CHANGE))
          thisResult.countTagChange += length;
        if (result.activities.contains(CellIterator.IterateAllEntry.ActivityType.MEMBERLIST_CHANGE))
          thisResult.countMemberChange += length;
        if (result.activities.contains(CellIterator.IterateAllEntry.ActivityType.GEOMETRY_CHANGE)) {
          thisResult.countGeometryChange += (result.validTo != null) ? length * Math.min(result.validTo - result.validFrom, 60 * 60 * 24) / (60 * 60 * 24) : length; //todo: replace this with grouping by changeset id?!!?!
          thisResult.countGeometryChangeDiff += length - ((result.previousGeometry instanceof MultiLineString) ?
              Geo.distanceOf((MultiLineString)result.previousGeometry) :
              (result.previousGeometry instanceof LineString) ? Geo.distanceOf((LineString)result.previousGeometry) : 0.0
          );
        }



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
      System.out.printf("%s\t%f\t%f\t%f\t%f\t%f\t%f\t%f\n",
          formatter.format(new Date(total.getKey()*1000)),
          total.getValue().countTotal,
          total.getValue().countCreation,
          total.getValue().countDeletion,
          total.getValue().countTagChange,
          total.getValue().countMemberChange,
          total.getValue().countGeometryChange,
          total.getValue().countGeometryChangeDiff
      );
    }

  }
}
