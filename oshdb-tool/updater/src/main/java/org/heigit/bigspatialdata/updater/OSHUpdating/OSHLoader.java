package org.heigit.bigspatialdata.updater.OSHUpdating;

import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKBWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.Producer;
import org.heigit.bigspatialdata.oshdb.TableNames;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.slf4j.LoggerFactory;

public class OSHLoader {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(OSHLoader.class);

  /**
   * represents the Load-Step in an ETL-Pipeline of updates.
   */
  public static void load(Connection conn, ArrayList<OSHEntity> oshEntityList) throws SQLException {
    OSHLoader.load(conn, oshEntityList, null);
  }

  /**
   * represents the Load-Step in an ETL-Pipeline of updates.
   *
   * @param updateDb
   * @param oshEntities
   * @param producer
   */
  public static void load(Connection updateDb, Iterable<OSHEntity> oshEntities, Producer<String, Stream<Byte[]>> producer) throws SQLException {
    LOG.info("loading");
    OSHLoader.prepareDB(updateDb);
    oshEntities.forEach((OSHEntity oshEntity) -> {
      try (PreparedStatement st = updateDb.prepareStatement("INSERT INTO " + TableNames.forOSMType(oshEntity.getType()).get() + " (id,bbx,data) "
          + "VALUES (?,ST_GeomFromWKB(?,4326),?) "
          + "ON CONFLICT (id) DO UPDATE SET "
          + "bbx = EXCLUDED.bbx, "
          + "data = EXCLUDED.data;")) {
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
    });
    if (producer != null) {
      OSHLoader.promote(producer, oshEntities);
    }

  }

  public static void promote(Producer<String, Stream<Byte[]>> producer, Iterable<OSHEntity> oshEntities) {

  }

  private static void prepareDB(Connection updateDb) throws SQLException {
    Statement stmt = updateDb.createStatement();
    for (OSMType type : OSMType.values()) {
      if (type != OSMType.UNKNOWN) {
        String sqlCreate = "CREATE TABLE IF NOT EXISTS " + TableNames.forOSMType(type).get()
            + " (id bigint PRIMARY KEY,"
            + " bbx GEOMETRY(POLYGON,4326),"
            + " data bytea);";
        stmt.execute(sqlCreate);
        String sqlIndex = "CREATE INDEX IF NOT EXISTS " + TableNames.forOSMType(type).get() + "_gix ON " + TableNames.forOSMType(type).get() + " USING GIST (bbx);";
        stmt.execute(sqlIndex);
      }
    }
  }

}
