package org.heigit.ohsome.oshdb.tool.importer.util;

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
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.heigit.ohsome.oshdb.grid.GridOSHNodes;
import org.heigit.ohsome.oshdb.grid.GridOSHRelations;
import org.heigit.ohsome.oshdb.grid.GridOSHWays;
import org.heigit.ohsome.oshdb.util.CellId;
import org.heigit.ohsome.oshdb.util.TableNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OSHDBToIgnite {

  private static final Logger LOG = LoggerFactory.getLogger(OSHDBToIgnite.class);

  /**
   * Load your extracted and transformed OSH-Data into Ignite Caches.
   *
   * @param igniteXml Path to the Ignite-XML
   * @param oshdb     Connection to the OSHDB
   * @param prefix oshdb table prefix
   */
  public static void load(File igniteXml, Connection oshdb, String prefix)
      throws IgniteCheckedException {
    Ignition.setClientMode(true);
    IgniteConfiguration cfg = IgnitionEx.loadConfiguration(igniteXml.toString()).get1();
    cfg.setIgniteInstanceName("IgniteImportClientInstance");
    try (Ignite ignite = Ignition.start(cfg)) {
      ignite.cluster().state(ClusterState.ACTIVE);

      try (Statement stmt = oshdb.createStatement()) {

        OSHDBToIgnite.<GridOSHNodes>doGridImport(ignite, stmt, TableNames.T_NODES, prefix);
        OSHDBToIgnite.<GridOSHWays>doGridImport(ignite, stmt, TableNames.T_WAYS, prefix);
        OSHDBToIgnite.<GridOSHRelations>doGridImport(ignite, stmt, TableNames.T_RELATIONS, prefix);

      } catch (SQLException ex) {
        LOG.error("", ex);
      }
    }
  }

  private static <T> void doGridImport(Ignite ignite, Statement stmt, TableNames cacheName,
      String prefix) {
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
      final String sql;
      switch (cacheName) {
        case T_NODES:
          sql = "select level, id, data from grid_node";
          break;
        case T_WAYS:
          sql = "select level, id, data from grid_way";
          break;
        case T_RELATIONS:
          sql = "select level, id, data from grid_relation";
          break;
        default:
          throw new IllegalArgumentException("unknown cacheName " + cacheName);
      }
      try (ResultSet rst = stmt.executeQuery(sql)) {
        int cnt = 0;
        System.out.println(LocalDateTime.now() + " START loading into "
            + cache.getName() + " on Ignite");
        while (rst.next()) {
          final int level = rst.getInt(1);
          final long id = rst.getLong(2);
          final long levelId = CellId.getLevelId(level, id);

          final ObjectInputStream ois = new ObjectInputStream(rst.getBinaryStream(3));
          @SuppressWarnings("unchecked")
          final T grid = (T) ois.readObject();
          streamer.addData(levelId, grid);
          if (++cnt % 10 == 0) {
            streamer.flush();
          }
        }
        System.out.println(LocalDateTime.now() + " FINISHED loading into "
            + cache.getName() + " on Ignite");
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
    @Parameter(names = {"-ignite", "-igniteConfig", "-icfg"},
        description = "Path ot ignite-config.xml", required = true, order = 1)
    public File ignitexml;

    @Parameter(names = {"--prefix"}, description = "cache table prefix", required = false)
    public String prefix;

    @Parameter(names = {"-db", "-oshdb", "-outputDb"}, description = "Path to output H2",
        required = true, order = 2)
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
    try (Connection con = DriverManager.getConnection("jdbc:h2:" + largs.oshdb, "sa", null)) {
      OSHDBToIgnite.load(largs.ignitexml, con, largs.prefix);
    }
  }
}
