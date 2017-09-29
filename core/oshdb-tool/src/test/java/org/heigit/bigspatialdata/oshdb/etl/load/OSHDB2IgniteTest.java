package org.heigit.bigspatialdata.oshdb.etl.load;

import java.io.File;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

public class OSHDB2IgniteTest implements Runnable {

  private static Ignite ignite;
  Thread t;
  ArrayList<String> caches = new ArrayList<>();

  public OSHDB2IgniteTest() {
    caches.add("grid_node");
    caches.add("grid_way");
    caches.add("grid_relation");
  }

  @Before
  public void setUpClass() {
    if (!(new File("./oshdb.mv.db")).exists()) {
      fail("extract and transoform Data first!");
    }
    t = new Thread(new OSHDB2IgniteTest());
    t.start();

  }

  /*
  @Test
  public void testLoad() throws SQLException {
    File igniteXML = new File("../../ignite-config.xml");
    Connection oshdb = DriverManager.getConnection("jdbc:h2:./oshdb", "sa", "");
    OSHDB2Ignite.load(igniteXML, oshdb);
    assertTrue(ignite.active());
    assertArrayEquals(caches.toArray(), ignite.cacheNames().toArray());
  }
   */
  //@Test
  public void testMain() throws Exception {
    //At one point the config should point to a cluster that is seperate from already running clusters on the machine
    String[] args = new String[]{"-db", "./oshdb", "-ignite", "../../ignite-config.xml"};
    OSHDB2Ignite.main(args);
    assertTrue(ignite.active());
    assertArrayEquals(caches.toArray(), ignite.cacheNames().toArray());
  }

  @Override
  public void run() {
    try {
      IgniteConfiguration cfg = IgnitionEx.loadConfiguration("../../ignite-config.xml").get1();
      cfg.setIgniteInstanceName("IgniteTestDataNode");
      ignite = Ignition.start(cfg);
      ignite.destroyCaches(caches);
    } catch (IgniteCheckedException ex) {
      Logger.getLogger(OSHDB2IgniteTest.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

}
