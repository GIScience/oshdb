package org.heigit.bigspatialdata.oshdb.examples.activity;

import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.geotools.geometry.jts.JTS;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osh.*;
import org.heigit.bigspatialdata.oshdb.osm.*;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;

public class ActivityIndicatorFromPolygonBuildings {

  /**
   * Computes an Activity Indicator for buildings inside a polygon. This
   * activity indicator counts all changes of building objects,which are
   * geometry changes + tag changes (way versions) + tag and relation_member
   * changes (relation versions)
   * 
   * @param conn
   *          connection object to OSHDB e.g h2
   * @param polygon
   *          to define analyis area
   * @param ti
   *          TagInterpreter to decide whether to build polygons or linestrings
   * @return Map<Long,Long> with timestamps and counts
   * @throws ClassNotFoundException
   * @throws ParseException
   * @throws IOException
   */
  public Map<Long, Long> execute(Connection conn, Geometry polygon, TagInterpreter ti)
      throws ClassNotFoundException, ParseException, IOException {
    Class.forName("org.h2.Driver");

    // FileWriter fw = new FileWriter("allbuildingversions.csv");

    // create list of timestamps to analyze
    List<Long> timestamps = new ArrayList<>();
    final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    for (int year = 2016; year <= 2016; year++) {
      for (int month = 1; month <= 12; month++) {
        try {
          timestamps.add(formatter.parse(String.format("%d%02d01", year, month)).getTime() / 1000);
        } catch (java.text.ParseException e) {
          System.err.println("basdoawrd");
        }
        ;
      }
    }
    Collections.sort(timestamps, Collections.reverseOrder());

    // create BBOX to query gridcells as CellId-Objects, which contain
    // (zoomlevel,id)
    MultiPolygon inputPolygon = (MultiPolygon) polygon;

    Double minLon = JTS.toEnvelope(inputPolygon).getMinX();
    Double maxLon = JTS.toEnvelope(inputPolygon).getMaxX();
    Double minLat = JTS.toEnvelope(inputPolygon).getMinY();
    Double maxLat = JTS.toEnvelope(inputPolygon).getMaxY();

    BoundingBox inputBbox = new BoundingBox(minLon, maxLon, minLat, maxLat);

    XYGridTree grid = new XYGridTree(12);

    final List<CellId> cellIds = new ArrayList<>();

    grid.bbox2CellIds(inputBbox, true).forEach(cell -> {
      cellIds.add(cell);
    });

    // start processing in parallel all grid cells that relate to the input
    // polygons BBOX
    Map<Long, Long> superresult = cellIds.parallelStream().flatMap(cellId -> {

      try (
          final PreparedStatement pstmt = conn.prepareStatement(
          // union (select data from grid_relation where level = ?1 and id = ?2)
          "(select data from grid_way where level = ?1 and id = ?2) union (select data from grid_relation where level = ?1 and id = ?2)")) {
        pstmt.setInt(1, cellId.getZoomLevel());
        pstmt.setLong(2, cellId.getId());

        // each time 2 gridCells (1 grid_way and 1 grid_relation) are queried
        // and put into a stream
        try (final ResultSet rst2 = pstmt.executeQuery()) {
          List<GridOSHEntity> cells = new LinkedList<>();
          while (rst2.next()) {
            final ObjectInputStream ois = new ObjectInputStream(rst2.getBinaryStream(1));
            cells.add((GridOSHEntity) ois.readObject());
            // final GridOSHEntity gridCell = (GridOSHEntity) ois.readObject();
            // return gridCell;
          }
          // else {
          // return null;
          // }
          return cells.stream();

        }
      } catch (IOException | SQLException | ClassNotFoundException e) {
        e.printStackTrace();
        return null;
      }
    }).map(gridCell -> {

      GridOSHEntity cell = (GridOSHEntity) gridCell;

      Map<Long, Long> timestampActivity = new TreeMap<>();

      Iterator<OSHEntity> itr = cell.iterator();

      while (itr.hasNext()) {
        OSHEntity<OSMEntity> osh = itr.next();
        if (!osh.hasTagKey(0)) continue;
        if (!osh.intersectsBbox(inputBbox)){ //getBoundingBox().getGeometry().intersects(inputPolygon)) {
          continue;
        }
        
        //check if all versions are inside cell to avoid geometry checks
        boolean allVersionsWithin = false;
        if (osh.getBoundingBox().getGeometry().within(inputPolygon)){
           allVersionsWithin = true;
        };
        
        List<OSMEntity> versions = new ArrayList<>();

        List<Long> modTs = osh.getModificationTimestamps(true);
        Collections.sort(modTs, Collections.reverseOrder());

        Iterator<OSMEntity> allVersions = osh.getVersions().iterator();
        allVersions.hasNext();

        OSMEntity osm = allVersions.next();
        for (Long t : modTs) {

          if (t < osm.getTimestamp()) {
            if (!allVersions.hasNext())
              break;
            osm = allVersions.next();
          }
          
          if (!osm.isVisible() || !osm.hasTagKey(0)) continue;
          
          if (! allVersionsWithin){
          try {
            
            
            Geometry osmGeom = osm.getGeometry(t, ti);
            
            if ( //osmGeom.isValid()
                //getCoordinate instead of getCentroid
                
                //&& osmGeom.isSimple() &&  
                //GeometryFactory.createPointFromInternalCoord(osmGeom.getCoordinate(), osmGeom).intersects(inputPolygon)
                osmGeom.getCentroid().intersects(inputPolygon)
                ) 
                
            {

              // try {
              // fw.append(osm.getGeometry(t, ti).toText().toString() + "\n");
              // } catch (IOException e) {
              // // TODO Auto-generated catch block
              // e.printStackTrace();
              // }

              versions.add(osm);
            }
          } catch (Exception e) {
            // TODO: handle exception
            System.err.println(e);
          }
          }
          else {
            versions.add(osm);
          }
        }

        int v = 0;
        for (int i = 0; i < timestamps.size(); i++) {
          long ts = timestamps.get(i);
          long count = 0;
          while (v < versions.size() && versions.get(v).getTimestamp() > ts) {
            count++;
            v++;
          }
          if (i == 0) {
            continue;
          }
          if (timestampActivity.containsKey(ts)) {

            timestampActivity.put(ts, timestampActivity.get(ts) + count);

          } else {
            timestampActivity.put(ts, count);
          }

          if (v >= versions.size())
            break;

        }

      }

      return timestampActivity;

    }).reduce(Collections.emptyMap(), (partial, b) -> {

      Map<Long, Long> sum = new TreeMap<>();
      sum.putAll(partial);
      for (Map.Entry<Long, Long> entry : b.entrySet()) {

        Long activity = partial.get(entry.getKey());
        if (activity == null) {

          activity = entry.getValue();

          if (activity == null) {
            activity = (long) 0;
          }

        } else {
          Long newActivity = entry.getValue().longValue();
          // if (newActivity == null){ newActivity = Long.valueOf(0); }
          activity = Long.valueOf((activity.longValue() + newActivity));

        }
        sum.put(entry.getKey(), activity);
      }

      // System.out.println(sum);

      return sum;
    }

    );

    // fill missing values with 0
    for (Long ts : timestamps.subList(1, timestamps.size())) {
      superresult.putIfAbsent(ts, (long) 0);
    }
//System.out.println("1 Polygon done.");
    return superresult;

  }

}
