package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
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
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.db.OSHDBIgnite;
import org.heigit.ohsome.oshdb.grid.GridOSHNodes;
import org.heigit.ohsome.oshdb.util.CellId;
import org.heigit.ohsome.oshdb.util.TableNames;

abstract class TestMapReduceOSHDBIgnite extends TestMapReduce {
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

  public TestMapReduceOSHDBIgnite(OSHDBIgnite oshdb) throws Exception {
    super(oshdb);

    final String prefix = "tests";
    oshdb.prefix(prefix);

    OSHDBH2 oshdbH2 = new OSHDBH2("../oshdb-api/src/test/resources/test-data");
    this.keytables = oshdbH2;

    Ignite ignite = ((OSHDBIgnite) this.oshdb).getIgnite();
    ignite.cluster().state(ClusterState.ACTIVE);

    CacheConfiguration<Long, GridOSHNodes> cacheCfg =
        new CacheConfiguration<>(TableNames.T_NODES.toString(prefix));
    cacheCfg.setStatisticsEnabled(true);
    cacheCfg.setBackups(0);
    cacheCfg.setCacheMode(CacheMode.PARTITIONED);
    IgniteCache<Long, GridOSHNodes> cache = ignite.getOrCreateCache(cacheCfg);
    cache.clear();
    // dummy caches for ways+relations (at the moment we don't use them in the actual TestMapReduce)
    ignite.getOrCreateCache(new CacheConfiguration<>(TableNames.T_WAYS.toString(prefix)));
    ignite.getOrCreateCache(new CacheConfiguration<>(TableNames.T_RELATIONS.toString(prefix)));

    // load test data into ignite cache
    try (IgniteDataStreamer<Long, GridOSHNodes> streamer = ignite.dataStreamer(cache.getName())) {
      Connection h2Conn = oshdbH2.getConnection();
      Statement h2Stmt = h2Conn.createStatement();

      streamer.allowOverwrite(true);

      try (final ResultSet rst =
          h2Stmt.executeQuery("select level, id, data from " + TableNames.T_NODES.toString())) {
        while (rst.next()) {
          final int level = rst.getInt(1);
          final long id = rst.getLong(2);
          final ObjectInputStream ois = new ObjectInputStream(rst.getBinaryStream(3));
          final GridOSHNodes grid = (GridOSHNodes) ois.readObject();
          streamer.addData(CellId.getLevelId(level, id), grid);
        }
      } catch (IOException | ClassNotFoundException e) {
        e.printStackTrace();
        fail(e.toString());
      }
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      fail(e.toString());
    }

    ignite.cluster().state(ClusterState.ACTIVE_READ_ONLY);
  }
}
