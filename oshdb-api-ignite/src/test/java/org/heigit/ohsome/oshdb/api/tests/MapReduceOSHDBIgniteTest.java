package org.heigit.ohsome.oshdb.api.tests;

import static org.heigit.ohsome.oshdb.api.db.H2Support.createJdbcPoolFromPath;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.h2.jdbcx.JdbcConnectionPool;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBIgnite;
import org.heigit.ohsome.oshdb.grid.GridOSHNodes;
import org.heigit.ohsome.oshdb.util.CellId;
import org.heigit.ohsome.oshdb.util.TableNames;

abstract class MapReduceOSHDBIgniteTest extends MapReduceTest {

  static final String PREFIX = "tests";
  static final String KEYTABLES = "../data/test-data";
  static final Ignite ignite;

  static {
    int rndPort = 47577 + (int) (Math.random() * 1000);
    IgniteConfiguration cfg = new IgniteConfiguration();
    cfg.setPeerClassLoadingEnabled(true);
    cfg.setIgniteInstanceName("OSHDB-Unit-Tests_" + rndPort);
    cfg.setBinaryConfiguration((new BinaryConfiguration()).setCompactFooter(false));
    cfg.setGridLogger(new Slf4jLogger());
    cfg.setWorkDirectory("/tmp");
    cfg.setDiscoverySpi((new TcpDiscoverySpi())
        .setLocalPort(rndPort)
        .setLocalPortRange(0)
        .setIpFinder((new TcpDiscoveryVmIpFinder()).setAddresses(List.of("127.0.0.1:" + rndPort)))
    );
    ignite = Ignition.start(cfg);
  }

  private static OSHDBDatabase initOshdb(String prefix, String keytables, Consumer<OSHDBIgnite> computeMode) {
    ignite.cluster().state(ClusterState.ACTIVE);

    CacheConfiguration<Long, GridOSHNodes> cacheCfg =
        new CacheConfiguration<>(TableNames.T_NODES.toString(PREFIX));
    cacheCfg.setStatisticsEnabled(true);
    cacheCfg.setBackups(0);
    cacheCfg.setCacheMode(CacheMode.PARTITIONED);
    IgniteCache<Long, GridOSHNodes> cache = ignite.getOrCreateCache(cacheCfg);
    cache.clear();
    // dummy caches for ways+relations (at the moment we don't use them in the actual TestMapReduce)
    ignite.getOrCreateCache(new CacheConfiguration<>(TableNames.T_WAYS.toString(PREFIX)));
    ignite.getOrCreateCache(new CacheConfiguration<>(TableNames.T_RELATIONS.toString(PREFIX)));

    loadTestdataIntoIgnite(ignite, cache.getName(), KEYTABLES);

    JdbcConnectionPool oshdbH2 = createJdbcPoolFromPath(keytables);

    ignite.cluster().state(ClusterState.ACTIVE_READ_ONLY);

    var oshdb = new OSHDBIgnite(ignite, prefix, oshdbH2);
    computeMode.accept(oshdb);
    return oshdb;
  }

  private static void loadTestdataIntoIgnite(Ignite ignite, String cache, String keytables) {
    JdbcConnectionPool oshdbH2 = createJdbcPoolFromPath(keytables);

    // load test data into ignite cache
    try (IgniteDataStreamer<Long, GridOSHNodes> streamer = ignite.dataStreamer(cache);
        Connection h2Conn = oshdbH2.getConnection();
        Statement h2Stmt = h2Conn.createStatement()) {
      streamer.allowOverwrite(true);
      try (final ResultSet rst =
          h2Stmt.executeQuery("select level, id, data from " + TableNames.T_NODES)) {
        while (rst.next()) {
          final int level = rst.getInt(1);
          final long id = rst.getLong(2);
          final ObjectInputStream ois = new ObjectInputStream(rst.getBinaryStream(3));
          final GridOSHNodes grid = (GridOSHNodes) ois.readObject();
          streamer.addData(CellId.getLevelId(level, id), grid);
        }
      }
    } catch (IOException | ClassNotFoundException | SQLException e) {
      e.printStackTrace();
      fail(e.toString());
    }
  }


  MapReduceOSHDBIgniteTest(Consumer<OSHDBIgnite> computeMode) throws Exception {
    super(initOshdb(PREFIX, KEYTABLES, computeMode));
  }

  MapReduceOSHDBIgniteTest(String prefix, String keytables, Consumer<OSHDBIgnite> computeMode) throws Exception {
    super(initOshdb(prefix, keytables, computeMode));
  }
}
