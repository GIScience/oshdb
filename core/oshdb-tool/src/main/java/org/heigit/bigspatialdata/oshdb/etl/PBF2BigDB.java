package org.heigit.bigspatialdata.oshdb.etl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * Prototype of an easy class to convert PBF 2 H2 for testing users
 *
 */
public class PBF2BigDB {

  private static final Logger LOG = Logger.getLogger(PBF2BigDB.class.getName());

  private PBF2BigDB() {
  }

  public static void toJDBC(File pbfFile, Connection conn, Path tmpDir) throws IOException, FileNotFoundException, SQLException, ClassNotFoundException {
    HOSMDbExtract.extract(pbfFile, conn, tmpDir);
    //HOSMDbTransform.transform();

  }

  public static void main(String[] args) throws IOException, FileNotFoundException, ClassNotFoundException, SQLException {
    HOSMDbExtract.main(args);
    //HOSMDbTransform.main(new String[]{"-p", args[0]});
  }

}
