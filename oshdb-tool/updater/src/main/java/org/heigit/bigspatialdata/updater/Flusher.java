package org.heigit.bigspatialdata.updater;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.heigit.bigspatialdata.updater.util.cmd.FlushArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Flusher {

  private static final Logger LOG = LoggerFactory.getLogger(Updater.class);

  /**
   * Flush updates form JDBC to real Ignite (best done once in a while, when database-usage is low).
   *
   * @param oshdb
   * @param updatedb
   * @param dbBit
   */
  public static void flush(Connection oshdb, Connection updatedb, Connection dbBit) {
    //do i need to block the cluster somehow, to prevent false operations?
    //ignite.cluster().active(false);
    //do flush here: can I use something from the etl?
    //ignite.cluster().active(true);

  }

  public static void main(String[] args) throws SQLException {
    FlushArgs config = new FlushArgs();
    JCommander jcom = JCommander.newBuilder().addObject(config).build();
    try {
      jcom.parse(args);
    } catch (ParameterException e) {
      LOG.error("There were errors with the given arguments! See below for more information!", e);
      jcom.usage();
      return;
    }
    if (config.baseArgs.help) {
      jcom.usage();
      return;
    }
    if (config.baseArgs.dbbit == null) {
      config.baseArgs.dbbit = config.baseArgs.jdbc;
    }

    try (Connection updateDb = DriverManager.getConnection(config.baseArgs.jdbc);
        Connection dbBit = DriverManager.getConnection(config.baseArgs.dbbit);
        Connection oshdb = DriverManager.getConnection(config.dbconfig);) {
      Flusher.flush(oshdb, updateDb, dbBit);
    }
  }

}
