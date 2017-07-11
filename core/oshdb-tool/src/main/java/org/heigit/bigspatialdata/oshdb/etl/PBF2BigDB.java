package org.heigit.bigspatialdata.oshdb.etl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Logger;

/** Prototype of an easy class to convert PBF 2 H2 for testing users
 *
 */
public class PBF2BigDB {
  
  private static final Logger LOG = Logger.getLogger(PBF2BigDB.class.getName());

  /**
   *
   */
  public PBF2BigDB() {
  }
  
  public static void main(String[] args) throws IOException, FileNotFoundException, ClassNotFoundException, SQLException {
    HOSMDbExtract.main(new String []{"-p",args[0]});
    HOSMDbTransform.main(new String []{"-p",args[0]});
  }
  
}
