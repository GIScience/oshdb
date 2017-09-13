package org.heigit.bigspatialdata.oshdb.etl.load;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.heigit.bigspatialdata.oshdb.etl.CacheNames;
import org.heigit.bigspatialdata.oshdb.etl.cmdarg.LoadArgs;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.TableNames;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

/**
 * @author Rafael Troilo <rafael.troilo@uni-heidelberg.de>
 */
public class OSHDB2Ignite {

  private static final Logger LOG = Logger.getLogger(OSHDB2Ignite.class.getName());

  /**
   * Load your extracted and transformed OSH-Data into Ignite Caches.
   *
   * @param igniteXML Path to the Ignite-XML
   * @param oshdb Connection to the OSHDB
   * @throws org.apache.ignite.IgniteCheckedException
   */
  public static void load(File igniteXML, Connection oshdb) throws IgniteCheckedException {
    Ignition.setClientMode(true);
    IgniteConfiguration cfg = IgnitionEx.loadConfiguration(igniteXML.toString()).get1();
    cfg.setIgniteInstanceName("IgniteImportClientInstance");
    try (Ignite ignite = Ignition.start(cfg)) {
      ignite.active(true);

      try (Statement stmt = oshdb.createStatement()) {

        OSHDB2Ignite.<GridOSHNodes>doGridImport(ignite, stmt, CacheNames.NODES);
        OSHDB2Ignite.<GridOSHWays>doGridImport(ignite, stmt, CacheNames.WAYS);
        OSHDB2Ignite.<GridOSHRelations>doGridImport(ignite, stmt, CacheNames.RELATIONS);

      } catch (SQLException ex) {
        LOG.log(Level.SEVERE, null, ex);
      }

    }

  }

  private static <T> void doGridImport(Ignite ignite, Statement stmt, CacheNames cacheName) {
    CacheConfiguration<Long, T> cacheCfg = new CacheConfiguration<>(cacheName.toString());
    cacheCfg.setBackups(0);
    cacheCfg.setCacheMode(CacheMode.PARTITIONED);

    IgniteCache<Long, T> cache = ignite.getOrCreateCache(cacheCfg);
    try (IgniteDataStreamer<Long, T> streamer = ignite.dataStreamer(cache.getName())) {
      streamer.allowOverwrite(true);
      String tableName = null;
      switch (cacheName) {
        case NODES:
          tableName = TableNames.T_NODES.toString();
          break;
        case WAYS:
          tableName = TableNames.T_WAYS.toString();
          break;
        case RELATIONS:
          tableName = TableNames.T_RELATIONS.toString();
      }
      try (final ResultSet rst = stmt.executeQuery("select level, id, data from " + tableName)) {
        int cnt = 0;
        while (rst.next()) {
          final int level = rst.getInt(1);
          final long id = rst.getLong(2);
          final long levelId = CellId.getLevelId(level, id);
          
          final ObjectInputStream ois = new ObjectInputStream(rst.getBinaryStream(3));
//          System.out.printf("level:%d, id:%d -> LevelId:%16s%n", level, id, Long.toHexString(levelId));
          final T grid = (T) ois.readObject();
          streamer.addData(id, grid);
        }
      } catch (IOException | ClassNotFoundException e) {
        e.printStackTrace();
      } catch (SQLException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
    }

  }

  public static void main(String[] args) throws SQLException, IgniteCheckedException {
    LoadArgs largs = new LoadArgs();
    JCommander jcom = JCommander.newBuilder().addObject(largs).build();
    try {
      jcom.parse(args);
    } catch (ParameterException e) {
      System.out.println("");
      LOG.log(Level.SEVERE, e.getLocalizedMessage());
      System.out.println("");
      jcom.usage();

      return;
    }

    if (largs.help.help) {
      jcom.usage();
      return;
    }
    try (Connection con = DriverManager.getConnection("jdbc:h2:" + largs.oshdbarg.oshdb, "sa", "")) {
      OSHDB2Ignite.load(largs.ignitexml, con);
    }

  }

}
