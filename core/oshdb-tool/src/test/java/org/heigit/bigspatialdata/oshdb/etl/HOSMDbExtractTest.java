/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.etl;

import java.io.File;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Moritz Schott <m.schott@stud.uni-heidelberg.de>
 */
public class HOSMDbExtractTest {

  private final File oshdb = new File("./keytables.mv.db");
  private final File relation = new File("./temp_relations.mv.db");
  private final File meta = new File("./temp_meta.properties");

  public HOSMDbExtractTest() {
  }

  @Before
  public void setUpClass() {

    if (oshdb.exists()) {
      oshdb.delete();
    }

    if (relation.exists()) {
      relation.delete();
    }

    if (meta.exists()) {
      meta.delete();
    }
  }

  /*//works also:
  @Test
  public void testExtract_File() throws Exception {
    File pbfFile = new File("./src/test/resources/maldives.osh.pbf");
    HOSMDbExtract.extract(pbfFile);
    assertTrue(oshdb.exists());
    assertTrue(relation.exists());
    assertTrue(meta.exists());
  }*/
  @Test
  public void testMain() throws Exception {
    String[] args = new String[]{"-pbf", "./src/test/resources/maldives.osh.pbf", "-tmp", "./", "-keytables", "./keytables"};
    HOSMDbExtract.main(args);
    assertTrue(oshdb.exists());
    assertTrue(relation.exists());
    assertTrue(meta.exists());
  }

}
