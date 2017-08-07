package org.heigit.bigspatialdata.oshdb.examples.workshop.workshop2;

import com.vividsolutions.jts.geom.Geometry;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.Geo;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamps;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

public class ResidentialRoadLengthAnalysis {

  private final static String databaseFile = "jdbc:h2:./karlsruhe-regbez";
  private final static BoundingBox bbox = new BoundingBox(8.6528,8.7294, 49.3683,49.4376); // ca Heidelberg

  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, ParseException {
    // load H2-support
    Class.forName("org.h2.Driver");

    //connect to the "Big"DB
    Connection conn = DriverManager.getConnection(databaseFile, "sa", "");

    PreparedStatement pstmt = conn.prepareStatement("select id from KEY where txt = ?");
    pstmt.setString(1, "highway");
    ResultSet resultSet = pstmt.executeQuery();
    if (!resultSet.next()) System.err.println("tag id not found");
    int keyId = resultSet.getInt(1);

    pstmt = conn.prepareStatement("select valueid from KEYVALUE where keyid = ? and txt = ?");
    pstmt.setInt(1, keyId);
    pstmt.setString(2, "residential");
    resultSet = pstmt.executeQuery();
    if (!resultSet.next()) System.err.println("tag id not found");
    int valueId = resultSet.getInt(1);

    //load tag interpreter helper which is later used for geometry building
    final TagInterpreter tagInterpreter = DefaultTagInterpreter.fromH2(conn);

    //get all needed cell-ids:
    XYGridTree grid = new XYGridTree(OSHDB.MAXZOOM);
    Iterable<CellId> cellIds = grid.bbox2CellIds(bbox, true);

    //determine timestamps to query features at
    List<Long> timestamps = (new OSHDBTimestamps(2008, 2017, 1, 12)).getTimestamps();
    SortedMap<Long,Double> countsByTimestamp = new TreeMap<>();

    //iterate over all cellIds
    for (CellId curr : cellIds) {
      //prepare a SQL-statement
      pstmt = conn.prepareStatement(
          "(select data from grid_node where level = ?1 and id = ?2)"
          + " union (select data from grid_way where level = ?1 and id = ?2)"
          + " union (select data from grid_relation where level = ?1 and id = ?2)"
      );
      pstmt.setInt(1, curr.getZoomLevel());
      pstmt.setLong(2, curr.getId());

      //execute statement
      ResultSet oshCellsRawData = pstmt.executeQuery();

      //iterate over result (this should be only one)
      while (oshCellsRawData.next()) {
        //get one object (cell) from the raw data stream
        GridOSHEntity oshCell = (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1))).readObject();

        //iterate over the history of all OSM objects in the current hosmCell
        CellIterator.iterateByTimestamps(
            oshCell,
            bbox,
            timestamps,
            tagInterpreter,
            osmEntity -> osmEntity.hasTagValue(keyId, valueId),
            false
        ).forEach(result -> {
          result.entrySet().forEach(entry -> {
            long timestamp = entry.getKey();
            OSMEntity osmEntity = entry.getValue().getLeft();
            Geometry geometry = entry.getValue().getRight();

            double length = Geo.lengthOf(geometry);

            countsByTimestamp.put(
                timestamp,
                countsByTimestamp.getOrDefault(timestamp, 0.0) + length
            );
          });
        });


      }

    }


    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    for (Map.Entry<Long,Double> entry : countsByTimestamp.entrySet()) {
      System.out.println(
          formatter.format(new Date(entry.getKey()*1000))
          + "\t"
          + entry.getValue()
      );
    }
  }

}



