package org.heigit.bigspatialdata.updater.OSHUpdating;

import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKBWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.Producer;
import org.heigit.bigspatialdata.oshdb.TableNames;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.update.UpdateDatabaseHandler;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
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
    Map<OSMType, LongBitmapDataProvider> bitmapMap = UpdateDatabaseHandler.prepareDB(updateDb, dbBit);
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
    UpdateDatabaseHandler.writeBitMap(bitmapMap, dbBit);
    if (producer != null) {
      OSHLoader.promote(producer, oshEntities);
    }

  }

  public static void promote(Producer<String, Stream<Byte[]>> producer, Iterable<OSHEntity> oshEntities) {

  }
}
