package org.heigit.bigspatialdata.updater.OSHUpdating;

import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKBWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.Producer;
import org.heigit.bigspatialdata.oshdb.TableNames;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.LoggerFactory;

public class OSHLoader {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(OSHLoader.class);

  /**
   * represents the Load-Step in an ETL-Pipeline of updates.
   */
  public static void load(Connection conn, ArrayList<OSHEntity> oshEntityList, Connection dbBit) throws SQLException, IOException, ClassNotFoundException {
    OSHLoader.load(conn, oshEntityList, dbBit, null);
  }

  /**
   * represents the Load-Step in an ETL-Pipeline of updates.
   *
   * @param updateDb
   * @param oshEntities
   * @param producer
   */
  public static void load(Connection updateDb, Iterable<OSHEntity> oshEntities, Connection dbBit, Producer<String, Stream<Byte[]>> producer) throws SQLException, IOException, ClassNotFoundException {
    LOG.info("loading");
    Map<OSMType, LongBitmapDataProvider> bitmapMap = OSHLoader.prepareDB(updateDb, dbBit);
    oshEntities.forEach((OSHEntity oshEntity) -> {
      try (PreparedStatement st = updateDb.prepareStatement("INSERT INTO " + TableNames.forOSMType(oshEntity.getType()).get() + " (id,bbx,data) "
          + "VALUES (?,ST_GeomFromWKB(?,4326),?) "
          + "ON CONFLICT (id) DO UPDATE SET "
          + "bbx = EXCLUDED.bbx, "
          + "data = EXCLUDED.data;");) {
        LOG.trace(oshEntity.getType() + " -> " + oshEntity.toString());
        st.setLong(1, oshEntity.getId());

        WKBWriter wkbWriter = new WKBWriter();
        OSHDBBoundingBox boundingBox = oshEntity.getBoundingBox();
        Polygon geometry = OSHDBGeometryBuilder.getGeometry(oshEntity.getBoundingBox());
        st.setObject(2, wkbWriter.write(geometry));
        st.setBytes(3, oshEntity.getData());
        st.executeUpdate();
      } catch (SQLException ex) {
        LOG.error("error in SQL", ex);
      }
      LongBitmapDataProvider get = bitmapMap.get(oshEntity.getType());
      get.addLong(oshEntity.getId());
      bitmapMap.put(oshEntity.getType(), get);
    });
    OSHLoader.writeBitMap(bitmapMap, dbBit);
    if (producer != null) {
      OSHLoader.promote(producer, oshEntities);
    }

  }

  public static void promote(Producer<String, Stream<Byte[]>> producer, Iterable<OSHEntity> oshEntities) {

  }

  private static Map<OSMType, LongBitmapDataProvider> prepareDB(Connection updateDb, Connection dbBit) throws SQLException, IOException, ClassNotFoundException {
    Statement stmt = updateDb.createStatement();
    Statement stmt2 = dbBit.createStatement();
    for (OSMType type : OSMType.values()) {
      if (type != OSMType.UNKNOWN) {
        String sqlCreate = "CREATE TABLE IF NOT EXISTS " + TableNames.forOSMType(type).get()
            + " (id bigint PRIMARY KEY,"
            + " bbx GEOMETRY(POLYGON,4326),"
            + " data bytea);";
        stmt.execute(sqlCreate);
        String sqlIndex = "CREATE INDEX IF NOT EXISTS " + TableNames.forOSMType(type).get() + "_gix ON " + TableNames.forOSMType(type).get() + " USING GIST (bbx);";
        stmt.execute(sqlIndex);

        //one might prefer three entries in the same table instead of three tables with one entry?
        LongBitmapDataProvider r = new Roaring64NavigableMap();
        String sqlBit = "CREATE TABLE IF NOT EXISTS " + TableNames.forOSMType(type).get() + "_bitmap (id int PRIMARY KEY, bitmap bytea);";
        stmt2.execute(sqlBit);
        PreparedStatement st = dbBit.prepareStatement("INSERT INTO " + TableNames.forOSMType(type).get() + "_bitmap (id,bitmap) "
            + "VALUES (?,?) "
            + "ON CONFLICT (id) DO NOTHING;");
        st.setInt(1, 1);
        ByteArrayOutputStream s = new ByteArrayOutputStream();
        ObjectOutputStream d = new ObjectOutputStream(s);
        d.writeObject(r);
        byte[] toByteArray = s.toByteArray();
        st.setBytes(2, toByteArray);
        st.executeUpdate();
      }
    }
    return OSHLoader.getBitMap(dbBit);
  }

  public static Map<OSMType, LongBitmapDataProvider> getBitMap(Connection dbBit) throws SQLException, IOException, ClassNotFoundException {
    Map<OSMType, LongBitmapDataProvider> map = new HashMap<>();
    for (OSMType type : OSMType.values()) {
      if (type != OSMType.UNKNOWN) {
        Statement retreave = dbBit.createStatement();
        String retrSQL = "SELECT bitmap FROM " + TableNames.forOSMType(type).get() + "_bitmap WHERE id=1;";
        ResultSet executeQuery = retreave.executeQuery(retrSQL);
        executeQuery.next();
        byte[] bytes = executeQuery.getBytes("bitmap");
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream input = new ObjectInputStream(bais);
        LongBitmapDataProvider r = new Roaring64NavigableMap();
        r = (LongBitmapDataProvider) input.readObject();
        map.put(type, r);
      }
    }
    return map;
  }

  private static void writeBitMap(Map<OSMType, LongBitmapDataProvider> bitmapMap, Connection dbBit) throws SQLException {
    bitmapMap.forEach((type, bitmap) -> {
      try {
        PreparedStatement write = dbBit.prepareStatement("UPDATE " + TableNames.forOSMType(type).get() + "_bitmap SET bitmap=? WHERE id=1;");
        ByteArrayOutputStream s = new ByteArrayOutputStream();
        ObjectOutputStream d = new ObjectOutputStream(s);
        d.writeObject(bitmap);
        byte[] toByteArray = s.toByteArray();
        write.setBytes(1, toByteArray);
        write.executeUpdate();
      } catch (SQLException ex) {
        LOG.error("error", ex);
      } catch (IOException ex) {
        LOG.error("error", ex);
      }
    });
  }

}
