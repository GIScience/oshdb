package org.heigit.bigspatialdata.updater.OSHUpdating;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.UnknownServiceException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.heigit.bigspatialdata.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.bigspatialdata.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.bigspatialdata.oshdb.impl.osh.OSHWayImpl;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.TableNames;
import org.heigit.bigspatialdata.oshdb.util.dbhandler.update.UpdateDatabaseHandler;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.locationtech.jts.geom.Polygon;
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
  public static void load(
      Connection updateDb,
      Iterable<OSHEntity> oshEntities,
      Connection dbBit,
      Producer<Long, Byte[]> producer)
      throws SQLException, IOException, ClassNotFoundException {

    LOG.info("loading");
    Map<OSMType, LongBitmapDataProvider> bitmapMap
        = UpdateDatabaseHandler.prepareDB(updateDb, dbBit);

    for (OSHEntity oshEntity : oshEntities) {
      PreparedStatement st;
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
      ByteBuffer buildRecord;
      switch (oshEntity.getType()) {
        case NODE:
          buildRecord = OSHNodeImpl.buildRecord(
              Lists.newArrayList(((OSHNode) oshEntity).getVersions()),
              0,
              0,
              0,
              0);
          break;

        case WAY:
          buildRecord = OSHWayImpl.buildRecord(
              Lists.newArrayList(((OSHWay) oshEntity).getVersions()),
              oshEntity.getNodes(),
              0,
              0,
              0,
              0);
          break;

        case RELATION:
          buildRecord = OSHRelationImpl.buildRecord(
              Lists.newArrayList(((OSHRelation) oshEntity).getVersions()),
              oshEntity.getNodes(),
              oshEntity.getWays(),
              0,
              0,
              0,
              0);
          break;
        default:
          throw new AssertionError(oshEntity.getType().name());
      }
      st.setLong(1, oshEntity.getId());
      st.setString(2, geometry.toText());
      st.setBytes(3, Arrays.copyOfRange(
          buildRecord.array(),
          buildRecord.position(),
          buildRecord.remaining()));
      st.executeUpdate();
      st.close();

      LongBitmapDataProvider get = bitmapMap.get(oshEntity.getType());
      get.addLong(oshEntity.getId());
      bitmapMap.put(oshEntity.getType(), get);

      if (producer != null) {
        OSHLoader.promote(
            producer,
            oshEntity.getType(),
            oshEntity.getVersions().iterator().next().getTimestamp(),
            oshEntity.getId(),
            buildRecord);
      }
    }
    if (producer != null) {
      producer.close();
    }
    UpdateDatabaseHandler.writeBitMap(bitmapMap, dbBit);

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

  private static void promote(
      Producer<Long, Byte[]> producer,
      OSMType type,
      OSHDBTimestamp timestamp,
      long id,
      ByteBuffer buildRecord) {
    ProducerRecord<Long, Byte[]> pr = new ProducerRecord(
        type.toString(),
        0,
        timestamp.getRawUnixTimestamp(),
        id,
        Arrays.copyOfRange(
            buildRecord.array(),
            buildRecord.position(),
            buildRecord.remaining())
    );
    producer.send(pr);

  }

}
