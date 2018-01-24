package org.heigit.bigspatialdata.oshdb.tool.etl;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.TopologyException;
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
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.Geo;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDbGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class TestMultipolygonGeometry {

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

  private static final int MAXZOOM = OSHDB.MAXZOOM;

  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException,
      org.json.simple.parser.ParseException {
    Class.forName("org.h2.Driver");

    try (Connection conn = DriverManager.getConnection("jdbc:h2:./heidelberg-ccbysa", "sa", "");
        final Statement stmt = conn.createStatement()) {

      System.out.println("Select tag key/value ids from DB");
      ResultSet rstTags = stmt.executeQuery(
          "select k.ID as KEYID, kv.VALUEID as VALUEID, k.txt as KEY, kv.txt as VALUE from KEYVALUE kv inner join KEY k on k.ID = kv.KEYID;");
      Map<String, Map<String, Pair<Integer, Integer>>> allKeyValues = new HashMap<>();
      while (rstTags.next()) {
        int keyId = rstTags.getInt(1);
        int valueId = rstTags.getInt(2);
        String keyStr = rstTags.getString(3);
        String valueStr = rstTags.getString(4);
        if (!allKeyValues.containsKey(keyStr)) {
          allKeyValues.put(keyStr, new HashMap<>());
        }
        allKeyValues.get(keyStr).put(valueStr, new ImmutablePair<>(keyId, valueId));
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
       * stmt.executeQuery("select level,id from grid_way"); while(rst.next()){
       * //System.out.println("-- "+rst.getInt(1)+"/"+rst.getInt(2)); zoomIds.add(new
       * ZoomId(rst.getInt(1),rst.getLong(2))); } rst.close();
       */
      final OSHDBBoundingBox bboxFilter = new OSHDBBoundingBox(8.65092, 8.65695, 49.38681, 49.39091);
      for (int zoom = 0; zoom <= MAXZOOM; zoom++) {
        XYGrid grid = new XYGrid(zoom);
        Set<Pair<Long, Long>> cellIds = grid.bbox2CellIdRanges(bboxFilter, true);
        for (Pair<Long, Long> cellsInterval : cellIds) {
          for (long cellId = cellsInterval.getLeft(); cellId <= cellsInterval
              .getRight(); cellId++) {
            // System.out.println("-- "+zoom+"/"+cellId);
            zoomIds.add(new ZoomId(zoom, cellId));
          }
        }
      }

      List<Long> timestamps;
      timestamps = new ArrayList<>();
      final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
      for (int year = 2004; year <= 2018; year++) {
        for (int month = 1; month <= 12; month++) {
          try {
            timestamps.add(formatter.parse(String.format("%d%02d01", year, month)).getTime());
          } catch (java.text.ParseException e) {
            System.err.println("basdoawrd");
          } ;
        }
      }

      System.out.println("Process in parallel");
      Optional<Map<Long, Double>> totals = zoomIds.parallelStream().flatMap(zoomId -> {
        try (final PreparedStatement pstmt = conn.prepareStatement(
            "(select data from grid_relation where level = ?1 and id = ?2) union (select data from grid_way where level = ?1 and id = ?2)")) {
          pstmt.setInt(1, zoomId.zoom);
          pstmt.setLong(2, zoomId.id);

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
      }).map(hosmCell -> {
        final int zoom = hosmCell.getLevel();
        final long id = hosmCell.getId();

        Map<Long, Double> counts = new HashMap<>(timestamps.size());
        Iterator<OSHEntity> oshEntitylIt = hosmCell.iterator();
        while (oshEntitylIt.hasNext()) {
          OSHEntity oshEntity = oshEntitylIt.next();

          if (!oshEntity.intersectsBbox(bboxFilter)) {
            continue;
          }
          boolean fullyInside = oshEntity.insideBbox(bboxFilter);

          Map<OSHDBTimestamp, OSMEntity> osmEntityByTimestamps = oshEntity.getByTimestamps(timestamps);
          int outerId = allRoles.get("outer");
          for (Map.Entry<OSHDBTimestamp, OSMEntity> entity : osmEntityByTimestamps.entrySet()) {
            OSHDBTimestamp timestamp = entity.getKey();
            OSMEntity osmEntity = entity.getValue();
            // if (osmEntity.isVisible() && osmEntity.hasTagKey(403) &&
            // osmEntity.hasTagValue(403,4)) {
            // if (osmEntity.isVisible() && (osmEntity.getId()==3188431 ||
            // osmEntity.getId()==236978113)) {
            if (osmEntity.isVisible()
                && osmEntity.hasTagKey(allKeyValues.get("building").get("yes").getLeft())) {// , new
              // int[]{allKeyValues.get("building").get("no").getRight()}))
              // {
              /*
               * if (osmEntity.isVisible() && (
               * osmEntity.hasTagValue(allKeyValues.get("type").get("multipolygon").getLeft(),
               * allKeyValues.get("type").get("multipolygon").getRight()) ||
               * osmEntity.hasTagValue(allKeyValues.get("aeroway").get("runway").getLeft(),
               * allKeyValues.get("aeroway").get("runway").getRight()) )) {
               */
              boolean isOldstyleMultipolygon = false;
              OSMWay oldstyleMultipolygonOuterWay = null;
              if (false) {
                // System.err.println(osmEntity.getClass().toString());
                if (osmEntity instanceof OSMRelation
                    && tagInterpreter.isOldStyleMultipolygon((OSMRelation) osmEntity)) {
                  OSMRelation rel = (OSMRelation) osmEntity;
                  for (int i = 0; i < rel.getMembers().length; i++) {
                    if (rel.getMembers()[i].getType() == OSMType.WAY
                        && rel.getMembers()[i].getRoleId() == outerId) {
                      oldstyleMultipolygonOuterWay =
                          (OSMWay) rel.getMembers()[i].getEntity().getByTimestamp(timestamp);
                      break;
                    }
                  }
                  if (!oldstyleMultipolygonOuterWay
                      .hasTagKey(allKeyValues.get("building").get("yes").getLeft())) {
                    continue;
                  }
                  isOldstyleMultipolygon = true;
                } else {
                  if (!osmEntity.hasTagKey(allKeyValues.get("building").get("yes").getLeft())) {
                    continue;
                  }
                }
              }
              // for (int i=0; i<osmEntity.getTags().length; i+=2)
              // System.out.println(osmEntity.getTags()[i] + "=" + osmEntity.getTags()[i+1]);
              // OSMWay foo = (OSMWay)osmEntity;
              double dist = 0.;
              try {
                Geometry geom = fullyInside
                    ? OSHDbGeometryBuilder.getGeometry(osmEntity, timestamp, tagInterpreter)
                    : OSHDbGeometryBuilder.getGeometryClipped(osmEntity, timestamp, tagInterpreter,
                        bboxFilter);

                if (geom == null) {
                  throw new NotImplementedException(); // hack!
                }
                if (geom.isEmpty()) {
                  throw new NotImplementedException(); // hack!
                }
                if (!(geom.getGeometryType() == "Polygon"
                    || geom.getGeometryType() == "MultiPolygon")) {
                  throw new NotImplementedException(); // hack!
                }
                // if (formatter.format(new Date(timestamp)).compareTo("20170101") == 0)
                // System.out.println(geom.getGeometryType()+"--"+osmEntity.getId());
                switch (geom.getGeometryType()) {
                  case "Polygon":
                    dist += 1.0 + 0 * Geo.areaOf((Polygon) geom);
                    break;
                  case "MultiPolygon":
                    dist += 1.0 + 0 * Geo.areaOf((MultiPolygon) geom);
                    break;
                  default:
                    System.err.println("Unknown geometry type found: " + geom.getGeometryType());
                }
                if (isOldstyleMultipolygon) {
                  Geometry adjustGeom = OSHDbGeometryBuilder
                      .getGeometry(oldstyleMultipolygonOuterWay, timestamp, tagInterpreter);
                  // oldstyleMultipolygonOuterWay.getGeometry(timestamp, new TagInterpreter()); ///
                  // todo -> custom taginterpreter for this case?!
                  System.out.println(
                      "subtract: " + Geo.areaOf((Polygon) adjustGeom) + " (from " + dist + ")");
                  dist -= 1.0 + 0 * Geo.areaOf((Polygon) adjustGeom);
                }
              } catch (NotImplementedException err) {
              } catch (IllegalArgumentException err) {
                System.err.printf(
                    "Relation %d skipped because of invalid geometry at timestamp %d\n",
                    osmEntity.getId(), timestamp);
              } catch (TopologyException err) {
                System.err.printf("Topology error at object %d at timestamp %d: %s\n",
                    osmEntity.getId(), timestamp, err.toString());
              }

              Double prevCnt = counts.get(timestamp);
              // counts.put(timestamp, prevCnt != null ? 0.5*(prevCnt.doubleValue() + dist) : dist);
              counts.put(timestamp.getRawUnixTimestamp(), prevCnt != null ? prevCnt.doubleValue() + dist : dist);
            } else {
              // System.out.println(osmEntity.getTags()[0]);
            }
          }
        }
        return counts;
      }).reduce((a, b) -> {
        Map<Long, Double> sum = new TreeMap<>();
        Set<Long> ts = new HashSet<Long>();
        ts.addAll(a.keySet());
        ts.addAll(b.keySet());
        for (Long t : ts) {
          Double aCnt = a.get(t);
          Double bCnt = b.get(t);
          sum.put(t,
              (aCnt != null ? aCnt.doubleValue() : 0.) + (bCnt != null ? bCnt.doubleValue() : 0.)
          /*
           * Cnt == null ? bCnt.doubleValue() : ( (bCnt == null ? aCnt.doubleValue() : 0.5*(
           * aCnt.doubleValue() + bCnt.doubleValue() )) )
           */
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
