package org.heigit.bigspatialdata.updater.util.dbhandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.UnknownServiceException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.TableNames;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.util.update.UpdateDbHelper;
import org.openstreetmap.osmosis.replication.common.ReplicationState;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.LoggerFactory;

public class DatabaseHandler {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(DatabaseHandler.class);

  private DatabaseHandler() {
  }

  public static void ereaseDb(Connection updateDb, Connection dbBit) throws SQLException,
      UnknownServiceException {
    Map<OSMType, LongBitmapDataProvider> bitmapMap = new HashMap<>();
    Statement createStatement = updateDb.createStatement();
    for (OSMType type : OSMType.values()) {
      if (type != OSMType.UNKNOWN) {
        //update bitmap first
        LongBitmapDataProvider bit = new Roaring64NavigableMap();
        bitmapMap.put(type, bit);
        DatabaseHandler.writeBitMap(bitmapMap, dbBit);

        //delete associated data after
        switch (updateDb.getClass().getName()) {
          case "org.postgresql.jdbc.PgConnection":
            createStatement.execute(
                "TRUNCATE "
                + TableNames.forOSMType(type).get());
            break;
          case "org.h2.jdbc.JdbcConnection":
            createStatement.execute(
                "TRUNCATE TABLE "
                + TableNames.forOSMType(type).get());
            break;
          case "org.apache.ignite.internal.jdbc.thin.JdbcThinConnection":
            createStatement.execute(
                "DELETE FROM "
                + TableNames.forOSMType(type).get());
            break;
          default:
            throw new UnknownServiceException(
                "The used driver --"
                + updateDb.getClass().getName()
                + "-- is not supportd yet. Please report to the developers");
        }
      }
    }

  }

  public static Map<OSMType, LongBitmapDataProvider> prepareDB(Connection updateDb, Connection dbBit)
      throws SQLException, IOException, ClassNotFoundException {
    for (OSMType type : OSMType.values()) {
      if (type != OSMType.UNKNOWN) {

        ByteArrayOutputStream s = new ByteArrayOutputStream();
        ObjectOutputStream d = new ObjectOutputStream(s);
        LongBitmapDataProvider r = new Roaring64NavigableMap();
        d.writeObject(r);
        byte[] toByteArray = s.toByteArray();

        switch (updateDb.getClass().getName()) {
          case "org.postgresql.jdbc.PgConnection":
            DatabaseHandler.preparePostgresUpdateDb(type, updateDb);
            DatabaseHandler.preparePostgresBitmapDb(type, dbBit, toByteArray);
            break;
          case "org.h2.jdbc.JdbcConnection":
            DatabaseHandler.prepareH2UpdateDb(type, updateDb);
            DatabaseHandler.prepareH2BitmapDb(type, dbBit, toByteArray);
            break;
          case "org.apache.ignite.internal.jdbc.thin.JdbcThinConnection":
            DatabaseHandler.prepareIgniteUpdateDb(type, updateDb);
            DatabaseHandler.prepareIgniteBitmapDb(type, dbBit, toByteArray);
            break;
          default:
            throw new UnknownServiceException(
                "The used driver --"
                + updateDb.getClass().getName()
                + "-- is not supportd yet. Please report to the developers");
        }
      }
    }
    return UpdateDbHelper.getBitMap(dbBit);
  }

  public static void writeBitMap(Map<OSMType, LongBitmapDataProvider> bitmapMap, Connection dbBit)
      throws SQLException {
    bitmapMap.forEach(new BiConsumer<OSMType, LongBitmapDataProvider>() {
      @Override
      public void accept(OSMType type, LongBitmapDataProvider bitmap) {
        try {
          PreparedStatement write = dbBit.prepareStatement("UPDATE " + TableNames.forOSMType(type)
              .get() + "_bitmap SET bitmap=? WHERE id=1;");
          ByteArrayOutputStream s = new ByteArrayOutputStream();
          ObjectOutputStream d = new ObjectOutputStream(s);
          d.writeObject(bitmap);
          byte[] toByteArray = s.toByteArray();
          write.setBytes(1, toByteArray);
          write.executeUpdate();
        } catch (SQLException | IOException ex) {
          LOG.error("error", ex);
        }
      }

    });
  }

  private static void prepareH2BitmapDb(OSMType type, Connection dbBit, byte[] bytea) throws
      SQLException {
    //one might prefer three entries in the same table instead of three tables with one entry?
    String sqlBit = "CREATE TABLE IF NOT EXISTS " + TableNames.forOSMType(type).get()
        + "_bitmap (id int PRIMARY KEY, bitmap bytea);";
    Statement bitmapTableCreateStatement = dbBit.createStatement();
    bitmapTableCreateStatement.execute(sqlBit);

    PreparedStatement bitmapTableInsertStatement = dbBit.prepareStatement(
        "INSERT IGNORE INTO "
        + TableNames.forOSMType(type).get()
        + "_bitmap (id,bitmap) VALUES (?,?);");
    bitmapTableInsertStatement.setInt(1, 1);
    bitmapTableInsertStatement.setBytes(2, bytea);
    bitmapTableInsertStatement.executeUpdate();
  }

  private static void prepareH2UpdateDb(OSMType type, Connection updateDb) throws SQLException {
    Statement dbStatement = updateDb.createStatement();
    String setMode = "SET MODE MySQL; "
        + "CREATE ALIAS IF NOT EXISTS H2GIS_SPATIAL FOR \"org.h2gis.functions.factory.H2GISFunctions.load\"; "
        + "CALL H2GIS_SPATIAL();";
    dbStatement.execute(setMode);

    String sqlCreate = "CREATE TABLE IF NOT EXISTS "
        + TableNames.forOSMType(type).get()
        + " (id BIGINT PRIMARY KEY, bbx GEOMETRY, data BYTEA);";
    dbStatement.execute(sqlCreate);

    String sqlIndex = "CREATE SPATIAL INDEX IF NOT EXISTS "
        + TableNames.forOSMType(type).get() + "_oshdbspatialindex ON "
        + TableNames.forOSMType(type).get() + "(bbx);";
    dbStatement.execute(sqlIndex);

  }

  private static void prepareIgniteBitmapDb(OSMType type, Connection dbBit, byte[] bytea)
      throws SQLException {
    //one might prefer three entries in the same table instead of three tables with one entry?
    String sqlBit = "CREATE TABLE IF NOT EXISTS " + TableNames.forOSMType(type).get()
        + "_bitmap (id int PRIMARY KEY, bitmap bytea);";
    Statement bitmapTableCreateStatement = dbBit.createStatement();
    bitmapTableCreateStatement.execute(sqlBit);

    PreparedStatement bitmapTableInsertStatement = dbBit.prepareStatement(
        "INSERT INTO "
        + TableNames.forOSMType(type).get()
        + "_bitmap (id,bitmap) VALUES (?,?);");
    bitmapTableInsertStatement.setInt(1, 1);
    bitmapTableInsertStatement.setBytes(2, bytea);
    try {
      //ignite seems to automatically ignore if the row is present (put if abset): https://apacheignite-sql.readme.io/docs/insert#section-description
      bitmapTableInsertStatement.executeUpdate();
    } catch (SQLException e) {
      LOG.warn("Is bitmap cache already present in ignite?", e);
    }
  }

  private static void prepareIgniteUpdateDb(OSMType type, Connection updateDb) throws SQLException {
    Statement dbStatement = updateDb.createStatement();
    String sqlCreate = "CREATE TABLE IF NOT EXISTS "
        + TableNames.forOSMType(type).get()
        + " (id BIGINT PRIMARY KEY, bbx GEOMETRY, data BYTEA);";
    dbStatement.execute(sqlCreate);

    String sqlIndex = "CREATE SPATIAL INDEX IF NOT EXISTS "
        + TableNames.forOSMType(type).get() + "_oshdbspatialindex ON "
        + TableNames.forOSMType(type).get() + "(bbx);";
    dbStatement.execute(sqlIndex);
  }

  private static void preparePostgresBitmapDb(OSMType type, Connection dbBit, byte[] bytea) throws
      SQLException {
    //one might prefer three entries in the same table instead of three tables with one entry?
    String sqlBit = "CREATE TABLE IF NOT EXISTS " + TableNames.forOSMType(type).get()
        + "_bitmap (id int PRIMARY KEY, bitmap bytea);";
    Statement bitmapTableCreateStatement = dbBit.createStatement();
    bitmapTableCreateStatement.execute(sqlBit);

    PreparedStatement bitmapTableInsertStatement = dbBit.prepareStatement("INSERT INTO "
        + TableNames.forOSMType(type).get()
        + "_bitmap (id,bitmap) VALUES (?,?) ON CONFLICT (id) DO NOTHING;");
    bitmapTableInsertStatement.setInt(1, 1);
    bitmapTableInsertStatement.setBytes(2, bytea);
    bitmapTableInsertStatement.executeUpdate();
  }

  private static void preparePostgresUpdateDb(OSMType type, Connection updateDb) throws
      SQLException {
    Statement dbStatement = updateDb.createStatement();

    String sqlCreate = "CREATE TABLE IF NOT EXISTS "
        + TableNames.forOSMType(type).get()
        + " (id BIGINT PRIMARY KEY, bbx GEOMETRY(POlYGON,4326), data BYTEA);";
    dbStatement.execute(sqlCreate);

    String sqlIndex = "CREATE INDEX IF NOT EXISTS "
        + TableNames.forOSMType(type).get() + "_gix ON "
        + TableNames.forOSMType(type).get() + " USING GIST (bbx);";
    dbStatement.execute(sqlIndex);
  }

  public static void updateOSHDBMetadata(OSHDBDatabase oshdb, ReplicationState state) throws
      SQLException {
    if (oshdb instanceof OSHDBJdbc) {
      try (
          PreparedStatement stmt = ((OSHDBJdbc) oshdb).getConnection().prepareStatement(
              "UPDATE " + TableNames.T_METADATA.toString(oshdb.prefix()) + " SET value=? where key=?;"
          )) {
            //timerange
            OSHDBTimestamp startOSHDB = (new OSHDBTimestamps(
                oshdb.metadata("data.timerange").split(",")[0])).get().first();
            stmt.setString(1,
                startOSHDB.toString() + "," + (new OSHDBTimestamp(state.getTimestamp())).toString());
            stmt.setString(2, "data.timerange");
            stmt.executeUpdate();

            //replication sequence
            stmt.setString(1, "" + state.getSequenceNumber());
            stmt.setString(2, "header.osmosis_replication_sequence_number");
            stmt.executeUpdate();
          }
    } else if (oshdb instanceof OSHDBIgnite) {
      throw new UnsupportedOperationException("Ignite backend does not provide metadata yet?");
    } else {
      throw new AssertionError(
          "Backend of type " + oshdb.getClass().getName() + " not supported yet.");
    }
  }

}
