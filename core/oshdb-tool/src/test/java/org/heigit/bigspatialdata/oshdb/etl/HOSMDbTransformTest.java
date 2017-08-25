/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.etl;

import java.io.File;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Moritz Schott <m.schott@stud.uni-heidelberg.de>
 */
public class HOSMDbTransformTest {

  private final File oshdb = new File("./oshdb.mv.db");
  private final File keytables = new File("./keytables.mv.db");
  private final File nodes = new File("./temp_nodesForWays.ser");
  private final File relation = new File("./temp_nodesForRelation.ser");
  private final File ways = new File("./temp_waysForRelation.ser");

  public HOSMDbTransformTest() {
  }

  @Before
  public void setUpClass() {

    if (nodes.exists()) {
      nodes.delete();
    }

    if (relation.exists()) {
      relation.delete();
    }
    if (ways.exists()) {
      ways.delete();
    }
    if (oshdb.exists()) {
      oshdb.delete();
    }
    if (!keytables.exists()) {
      fail("Extract pbf-File first!");
    }
  }

  /*//works also:
  @Test
  public void testExtract_File() throws Exception {
    File pbfFile = new File("./src/test/resources/maldives.osh.pbf");
    HOSMDbTransform.transform(pbfFile);
    assertTrue(oshdb.exists());
    assertTrue(nodes.exists());
    assertTrue(relation.exists());
    assertTrue(ways.exists());
  }*/
  @Test
  public void testMain() throws Exception {
    String[] args = new String[]{"-pbf", "./src/test/resources/maldives.osh.pbf", "-tmp", "./", "-key", "./keytables", "-db", "./oshdb"};
    HOSMDbTransform.main(args);
    assertTrue(oshdb.exists());
    assertTrue(nodes.exists());
    assertTrue(relation.exists());
    assertTrue(ways.exists());
  }

}
