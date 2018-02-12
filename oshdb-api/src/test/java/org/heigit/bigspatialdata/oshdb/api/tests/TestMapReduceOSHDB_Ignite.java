package org.heigit.bigspatialdata.oshdb.api.tests;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.TableNames;
import static org.junit.Assert.fail;

abstract class TestMapReduceOSHDB_Ignite extends TestMapReduce {
  final static Ignite ignite =
      Ignition.start(new File("./src/test/resources/ignite-config.xml").toString());

  public TestMapReduceOSHDB_Ignite(OSHDBIgnite oshdb) throws Exception {
    super(oshdb);

    final String prefix = "tests";
    oshdb.prefix(prefix);

    OSHDBH2 oshdb_h2 = new OSHDBH2("./src/test/resources/test-data");
    this.keytables = oshdb_h2;

    Ignite ignite = ((OSHDBIgnite) this.oshdb).getIgnite();
    ignite.active(true);

    // todo: also ways+relations? (at the moment we don't use them in the actual TestMapReduce
    // tests)
    CacheConfiguration<Long, GridOSHNodes> cacheCfg =
        new CacheConfiguration<>(TableNames.T_NODES.toString(prefix));
    cacheCfg.setStatisticsEnabled(true);
    cacheCfg.setBackups(0);
    cacheCfg.setCacheMode(CacheMode.PARTITIONED);
    IgniteCache<Long, GridOSHNodes> cache = ignite.getOrCreateCache(cacheCfg);
    cache.clear();

    // load test data into ignite cache
    try (IgniteDataStreamer<Long, GridOSHNodes> streamer = ignite.dataStreamer(cache.getName())) {
      Connection h2Conn = oshdb_h2.getConnection();
      Statement h2Stmt = h2Conn.createStatement();

      streamer.allowOverwrite(true);

      try (final ResultSet rst =
          h2Stmt.executeQuery("select level, id, data from " + TableNames.T_NODES.toString())) {
        while (rst.next()) {
          final int level = rst.getInt(1);
          final long id = rst.getLong(2);
          final ObjectInputStream ois = new ObjectInputStream(rst.getBinaryStream(3));
          final GridOSHNodes grid = (GridOSHNodes) ois.readObject();
          streamer.addData(ZGrid.addZoomToId(id, level), grid);
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
  }
}
