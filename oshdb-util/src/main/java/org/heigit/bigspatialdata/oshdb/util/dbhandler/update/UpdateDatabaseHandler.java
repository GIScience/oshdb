package org.heigit.bigspatialdata.oshdb.util.dbhandler.update;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.UnknownServiceException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.TableNames;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.LoggerFactory;

public class UpdateDatabaseHandler {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(UpdateDatabaseHandler.class);

  private UpdateDatabaseHandler() {
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
        UpdateDatabaseHandler.writeBitMap(bitmapMap, dbBit);

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

  public static Map<OSMType, LongBitmapDataProvider> getBitMap(Connection dbBit) throws SQLException,
      IOException, ClassNotFoundException {
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
        LongBitmapDataProvider r = (LongBitmapDataProvider) input.readObject();
        map.put(type, r);
      }
    }
    return map;
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
            UpdateDatabaseHandler.preparePostgresUpdateDb(type, updateDb);
            UpdateDatabaseHandler.preparePostgresBitmapDb(type, dbBit, toByteArray);
            break;
          case "org.h2.jdbc.JdbcConnection":
            UpdateDatabaseHandler.prepareH2UpdateDb(type, updateDb);
            UpdateDatabaseHandler.prepareH2BitmapDb(type, dbBit, toByteArray);
            break;
          case "org.apache.ignite.internal.jdbc.thin.JdbcThinConnection":
            UpdateDatabaseHandler.prepareIgniteUpdateDb(type, updateDb);
            UpdateDatabaseHandler.prepareIgniteBitmapDb(type, dbBit, toByteArray);
            break;
          default:
            throw new UnknownServiceException(
                "The used driver --"
                + updateDb.getClass().getName()
                + "-- is not supportd yet. Please report to the developers");
        }
      }
    }
    return UpdateDatabaseHandler.getBitMap(dbBit);
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

}
