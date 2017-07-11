package org.heigit.bigspatialdata.oshdb.examples.histocounts;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.OSHDb;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.Geo;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.wololo.geojson.GeoJSON;
import org.wololo.jts2geojson.GeoJSONWriter;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class ObjectGeometries {

  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, org.json.simple.parser.ParseException {

    boolean handleOldStyleMultipolygons = false;

    final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    //final BoundingBox bbox = new BoundingBox(8.61, 8.76, 49.40, 49.41);
    //final BoundingBox bbox = new BoundingBox(8.65092, 8.65695, 49.38681, 49.39091);
    final BoundingBox bbox = new BoundingBox(8, 9, 49, 50);
    //final BoundingBox bbox = new BoundingBox(75.98145, 99.53613, 14.71113, 38.73695);
    //final BoundingBox bbox = new BoundingBox(86.8798, 86.96065, 27.95271, 28.03774);
    //final BoundingBox bbox = new BoundingBox(86.92209, 86.92535, 27.9857, 27.98805);
    //final BoundingBox bbox = new BoundingBox(-1, 1, 51, 52);

    XYGridTree grid = new XYGridTree(OSHDb.MAXZOOM);

    final List<CellId> cellIds = new ArrayList<>();
    grid.bbox2CellIds(bbox, true).forEach(cellIds::add);

    // connect to the "Big"DB
    Connection conn = DriverManager.getConnection("jdbc:h2:./karlsruhe-regbez","sa", "");

    final TagInterpreter tagInterpreter = DefaultTagInterpreter.fromH2(conn);

    cellIds.parallelStream().flatMap(cell -> {
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
    }).forEach(oshCell -> {

      CellIterator.iterateAll(
          oshCell,
          bbox,
          tagInterpreter,
          osmEntity -> osmEntity.getId() == 254154168 && osmEntity instanceof OSMWay,
          //osmEntity -> osmEntity.getId() == 60105 && osmEntity instanceof OSMRelation,
          //osmEntity -> osmEntity.getId() == 150834648 && osmEntity instanceof OSMWay,
          //osmEntity -> osmEntity.getId() == 26946230 && osmEntity instanceof OSMWay,
          //osmEntity -> osmEntity.getId() == 5182648 && osmEntity instanceof OSMWay,
          //osmEntity -> osmEntity.getId() == 154937898 && osmEntity instanceof OSMWay,
          //osmEntity -> …
          handleOldStyleMultipolygons
      )
      .forEach(result -> {
        //todo: replace this with grouping by changeset id?!!?! -> maybe in iterateAll()!
        long timestamp = result.validFrom;
        OSMEntity osmEntity = result.osmEntity;
        Geometry geometry = result.geometry;

        String jsonstring;
        if (geometry == null) {
          jsonstring = null;
        } else {
          GeoJSONWriter writer = new GeoJSONWriter();
          GeoJSON json = writer.write(geometry);

          jsonstring = json.toString();
        }
        System.out.println();
        System.out.println(formatter.format(new Date(result.validFrom*1000))+","+(result.validTo != null ? formatter.format(new Date(result.validTo*1000)) : ""));
        System.out.println(jsonstring);

        //GeometryJSON asd = new GeometryJSON();
        //GeoJsonWriter writer = new GeoJsonWriter();



      });
    });


  }
}
