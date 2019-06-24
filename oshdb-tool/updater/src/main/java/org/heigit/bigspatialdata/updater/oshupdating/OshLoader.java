package org.heigit.bigspatialdata.updater.oshupdating;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.UnknownServiceException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
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
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.updater.util.dbhandler.DatabaseHandler;
import org.locationtech.jts.geom.Polygon;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.slf4j.LoggerFactory;

/**
 * Static method provider to load updates into the update-db.
 */
public class OshLoader {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(OshLoader.class);

  private OshLoader() {
  }

  /**
   * represents the Load-Step in an ETL-Pipeline of updates.
   *
   * @param updateDb The database to write updates to
   * @param oshEntities The entities to be written
   * @param dbBit the bitmap-db to be updated with newly changed OSM-Ids
   * @param producer An optional Kafka producer to promote new OSH-Ojects to a
   *     Kafka-Messaging-Cluster.
   * @throws java.sql.SQLException On error while handling databases
   * @throws java.io.IOException On etl-error
   * @throws java.lang.ClassNotFoundException on etl-error
   */
  public static void load(
      Connection updateDb,
      Iterable<Map<OSMType, Map<Long, OSHEntity>>> oshEntities,
      Connection dbBit,
      Producer<Long, Byte[]> producer)
      throws SQLException, IOException, ClassNotFoundException {

    LOG.info("loading");
    Map<OSMType, LongBitmapDataProvider> bitmapMap
        = DatabaseHandler.prepareDB(updateDb, dbBit);

    for (Map<OSMType, Map<Long, OSHEntity>> currMapOuter : oshEntities) {
      for (Entry<OSMType, Map<Long, OSHEntity>> currMap : currMapOuter.entrySet()) {
        for (Entry<Long, OSHEntity> currEntry : currMap.getValue().entrySet()) {
          OSHEntity currEntity = currEntry.getValue();
          PreparedStatement st;
          switch (updateDb.getClass().getName()) {
            case "org.apache.ignite.internal.jdbc.thin.JdbcThinConnection":
              st = OshLoader.insertIgnite(updateDb, currEntity);
              break;
            case "org.postgresql.jdbc.PgConnection":
              st = OshLoader.insertPostgres(updateDb, currEntity);
              break;
            case "org.h2.jdbc.JdbcConnection":
              st = OshLoader.insertH2(updateDb, currEntity);
              break;
            default:
              throw new UnknownServiceException(
                  "The used driver --"
                  + updateDb.getClass().getName()
                  + "-- is not supportd yet. Please report to the developers");
          }

          LOG.trace(currEntity.getType() + " -> " + currEntity.toString());
          ByteBuffer buildRecord;
          switch (currEntity.getType()) {
            case NODE:
              buildRecord = OSHNodeImpl.buildRecord(
                  Lists.newArrayList(((OSHNode) currEntity).getVersions()),
                  0,
                  0,
                  0,
                  0);
              break;

            case WAY:
              buildRecord = OSHWayImpl.buildRecord(
                  Lists.newArrayList(((OSHWay) currEntity).getVersions()),
                  currEntity.getNodes(),
                  0,
                  0,
                  0,
                  0);
              break;

            case RELATION:
              buildRecord = OSHRelationImpl.buildRecord(
                  Lists.newArrayList(((OSHRelation) currEntity).getVersions()),
                  currEntity.getNodes(),
                  currEntity.getWays(),
                  0,
                  0,
                  0,
                  0);
              break;
            default:
              throw new AssertionError(currEntity.getType().name());
          }
          Polygon geometry = OSHDBGeometryBuilder.getGeometry(currEntity.getBoundingBox());
          st.setLong(1, currEntity.getId());
          st.setString(2, geometry.toText());
          st.setBytes(3, Arrays.copyOfRange(
              buildRecord.array(),
              buildRecord.position(),
              buildRecord.remaining()));
          st.executeUpdate();
          st.close();

          bitmapMap.compute(currEntity.getType(), (k, v) -> {
            //we know v is present so no checking here
            v.addLong(currEntity.getId());
            return v;
          });

          if (producer != null) {
            OshLoader.promote(
                producer,
                currEntity.getType(),
                currEntity.getVersions().iterator().next().getTimestamp(),
                currEntity.getId(),
                buildRecord);
          }
        }
      }
    }
    if (producer != null) {
      producer.close();
    }
    DatabaseHandler.writeBitMap(bitmapMap, dbBit);

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
