package org.heigit.bigspatialdata.updater.OSHUpdating;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.UnknownServiceException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.heigit.bigspatialdata.oshdb.TableNames;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.dbhandler.update.UpdateDatabaseHandler;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKBWriter;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class OSHLoader {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(OSHLoader.class);

  private OSHLoader() {
  }

  /**
   * represents the Load-Step in an ETL-Pipeline of updates.
   *
   * @param conn
   * @param oshEntityList
   * @param dbBit
   * @throws java.sql.SQLException
   * @throws java.io.IOException
   * @throws java.lang.ClassNotFoundException
   */
  public static void load(Connection conn, ArrayList<OSHEntity> oshEntityList, Connection dbBit)
      throws SQLException, IOException, ClassNotFoundException {
    OSHLoader.load(conn, oshEntityList, dbBit, null);
  }

  /**
   * represents the Load-Step in an ETL-Pipeline of updates.
   *
   * @param updateDb
   * @param oshEntities
   * @param dbBit
   * @param producer
   * @throws java.sql.SQLException
   * @throws java.io.IOException
   * @throws java.lang.ClassNotFoundException
   */
  public static void load(Connection updateDb, Iterable<OSHEntity> oshEntities, Connection dbBit,
      Producer<Long, Byte[]> producer) throws SQLException, IOException, ClassNotFoundException {
    LOG.info("loading");
    Map<OSMType, LongBitmapDataProvider> bitmapMap = UpdateDatabaseHandler
        .prepareDB(updateDb, dbBit);
    oshEntities.forEach((OSHEntity oshEntity) -> {
      //cannot use because of missing documentation: ByteArrayOutputWrapper out = new ByteArrayOutputWrapper();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PreparedStatement st;

      try (ObjectOutputStream oos = new ObjectOutputStream(baos);) {
        WKBWriter wkbWriter = new WKBWriter();
        OSHDBBoundingBox boundingBox = oshEntity.getBoundingBox();
        oos.writeObject(oshEntity);
        byte[] data = baos.toByteArray();
        Polygon geometry = OSHDBGeometryBuilder.getGeometry(oshEntity.getBoundingBox());
        switch (updateDb.getClass().getName()) {
          case "org.apache.ignite.internal.jdbc.thin.JdbcThinConnection":
            st = OSHLoader.insertIgnite(updateDb, oshEntity);
            break;
          case "org.postgresql.jdbc.PgConnection":
            st = OSHLoader.insertPostgres(updateDb, oshEntity);
            break;
          case "org.h2.jdbc.JdbcConnection":
            st = OSHLoader.insertH2(updateDb, oshEntity);
            break;
          default:
            throw new UnknownServiceException(
                "The used driver --"
                + updateDb.getClass().getName()
                + "-- is not supportd yet. Please report to the developers");
        }

        LOG.trace(oshEntity.getType() + " -> " + oshEntity.toString());

        st.setLong(1, oshEntity.getId());
        st.setString(2, geometry.toText());
        st.setBytes(3, data);
        st.executeUpdate();
        st.close();
      } catch (SQLException ex) {
        LOG.error("error in SQL", ex);
      } catch (IOException ex) {
        LOG.error("error", ex);
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

  public static void promote(Producer<Long, Byte[]> producer, Iterable<OSHEntity> oshEntities) {
    oshEntities.forEach(oshentity -> {
      ProducerRecord<Long, Byte[]> pr = new ProducerRecord(oshentity.getType().toString(), 0,
          oshentity.getLatest().getTimestamp().toDate().getTime(), oshentity.getId(), oshentity
          .getData());
      producer.send(pr);
    });
    producer.close();
  }

  private static PreparedStatement insertH2(Connection updateDb, OSHEntity oshEntity) throws
      SQLException {
    return updateDb.prepareStatement("MERGE INTO "
        + TableNames.forOSMType(oshEntity.getType()).get()
        + " (id,bbx,data) "
        + "VALUES (?,ST_GeomFromText(?, 4326),?);");
  }

  private static PreparedStatement insertIgnite(Connection updateDb, OSHEntity oshEntity) throws
      SQLException {
    return updateDb.prepareStatement("MERGE INTO "
        + TableNames.forOSMType(oshEntity.getType()).get()
        + " (id,bbx,data) "
        + "VALUES (?,?,?);");
  }

  private static PreparedStatement insertPostgres(Connection updateDb, OSHEntity oshEntity) throws
      SQLException {
    return updateDb.prepareStatement("INSERT INTO "
        + TableNames.forOSMType(oshEntity.getType()).get()
        + " (id,bbx,data) "
        + "VALUES (?,ST_GeomFromText(?,4326),?) "
        + "ON CONFLICT (id) DO UPDATE SET "
        + "bbx = EXCLUDED.bbx, "
        + "data = EXCLUDED.data;");
  }

}
