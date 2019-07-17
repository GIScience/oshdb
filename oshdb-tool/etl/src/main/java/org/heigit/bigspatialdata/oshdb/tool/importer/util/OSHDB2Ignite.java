package org.heigit.bigspatialdata.oshdb.tool.importer.util;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.TableNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Rafael Troilo &lt;rafael.troilo@heigit.org&gt;
 */
public class OSHDB2Ignite {

  private static final Logger LOG = LoggerFactory.getLogger(OSHDB2Ignite.class);

  /**
   * Load your extracted and transformed OSH-Data into Ignite Caches.
   *
   * @param igniteXML Path to the Ignite-XML
   * @param oshdb Connection to the OSHDB
   * @param prefix
   * @throws org.apache.ignite.IgniteCheckedException
   */
  public static void load(File igniteXML, Connection oshdb, String prefix) throws IgniteCheckedException {
    Ignition.setClientMode(true);
    IgniteConfiguration cfg = IgnitionEx.loadConfiguration(igniteXML.toString()).get1();
    cfg.setIgniteInstanceName("IgniteImportClientInstance");
    try (Ignite ignite = Ignition.start(cfg)) {
      ignite.cluster().active(true);

      try (Statement stmt = oshdb.createStatement()) {

        OSHDB2Ignite.<GridOSHNodes>doGridImport(ignite, stmt, TableNames.T_NODES, prefix);
        OSHDB2Ignite.<GridOSHWays>doGridImport(ignite, stmt, TableNames.T_WAYS, prefix);
        OSHDB2Ignite.<GridOSHRelations>doGridImport(ignite, stmt, TableNames.T_RELATIONS, prefix);

      } catch (SQLException ex) {
        LOG.error("", ex);
      }

      //deactive  cluster after import, so that all caches get persist
      ignite.cluster().active(false);
      ignite.cluster().active(true);
    }
  }

  private static <T> void doGridImport(Ignite ignite, Statement stmt, TableNames cacheName, String prefix) {
    final String cacheWithPrefix = cacheName.toString(prefix);

    ignite.destroyCache(cacheWithPrefix);

    CacheConfiguration<Long, T> cacheCfg = new CacheConfiguration<>(cacheWithPrefix);
    cacheCfg.setBackups(0);
    cacheCfg.setCacheMode(CacheMode.PARTITIONED);

    IgniteCache<Long, T> cache = ignite.getOrCreateCache(cacheCfg);
    boolean pers = false;
    if (ignite.cluster().isWalEnabled(cacheWithPrefix)) {
      ignite.cluster().disableWal(cacheWithPrefix);
      pers = true;
    }

    try (IgniteDataStreamer<Long, T> streamer = ignite.dataStreamer(cache.getName())) {
      streamer.allowOverwrite(true);
      final String tableName;
      switch (cacheName) {
        case T_NODES:
          tableName = TableNames.T_NODES.toString();
          break;
        case T_WAYS:
          tableName = TableNames.T_WAYS.toString();
          break;
        case T_RELATIONS:
          tableName = TableNames.T_RELATIONS.toString();
          break;
        default:
          throw new IllegalArgumentException("unknown cacheName " + cacheName);
      }
      try (final ResultSet rst = stmt.executeQuery("select level, id, data from " + tableName)) {
        int cnt = 0;
        System.out.println(LocalDateTime.now() + " START loading " + tableName + " into " + cache.getName() + " on Ignite");
        while (rst.next()) {
          final int level = rst.getInt(1);
          final long id = rst.getLong(2);
          final long levelId = CellId.getLevelId(level, id);

          final ObjectInputStream ois = new ObjectInputStream(rst.getBinaryStream(3));
//          System.out.printf("level:%d, id:%d -> LevelId:%16s%n", level, id, Long.toHexString(levelId));
          @SuppressWarnings("unchecked")
          final T grid = (T) ois.readObject();
          streamer.addData(levelId, grid);
          if (++cnt % 10 == 0) {
            streamer.flush();
          }
        }
        System.out.println(LocalDateTime.now() + " FINISHED loading " + tableName + " into " + cache.getName() + " on Ignite");
      } catch (IOException | ClassNotFoundException | SQLException e) {
        LOG.error("Could not import Grid!", e);
      }
    } finally {
      if (pers) {
        ignite.cluster().enableWal(cacheWithPrefix);
      }
    }
  }

  private static class Config {
    @Parameter(names = {"-ignite", "-igniteConfig", "-icfg"}, description = "Path ot ignite-config.xml", required = true, order = 1)
    public File ignitexml;

    @Parameter(names = {"--prefix"}, description = "cache table prefix", required = false)
    public String prefix;

    @Parameter(names = {"-db", "-oshdb", "-outputDb"}, description = "Path to output H2", required = true, order = 2)
    public File oshdb;

    @Parameter(names = {"-help", "--help", "-h", "--h"}, help = true, order = 0)
    public boolean help = false;

  }

  public static void main(String[] args) throws SQLException, IgniteCheckedException {
    Config largs = new Config();
    JCommander jcom = JCommander.newBuilder().addObject(largs).build();
    try {
      jcom.parse(args);
    } catch (ParameterException e) {
      System.out.println("");
      LOG.error(e.getLocalizedMessage());
      System.out.println("");
      jcom.usage();

      return;
    }

    if (largs.help) {
      jcom.usage();
      return;
    }
    try (Connection con = DriverManager.getConnection("jdbc:h2:" + largs.oshdb, "sa", "")) {
      OSHDB2Ignite.load(largs.ignitexml, con, largs.prefix);
    }
  }
}
