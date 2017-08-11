package org.heigit.bigspatialdata.oshdb.etl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;
import org.apache.ignite.Ignite;

/**
 * Prototype of an easy class to convert PBF 2 H2 for testing users
 *
 */
public class PBF2BigDB {

  private static final Logger LOG = Logger.getLogger(PBF2BigDB.class.getName());

  /**
   * Load a PBF-File eg from Geofabrik into your desired JDBC-Compatible BigDB.
   *
   * @param pbfFile pbf-File
   * @param conn Connection to a Database. H2 is well supported.
   * @param tmpDir temporary Directory
   * @throws IOException
   * @throws FileNotFoundException
   * @throws SQLException
   * @throws ClassNotFoundException
   * @see <a href="http://download.geofabrik.de/"> Geofabrik</a>
   */
  public static void toJDBC(File pbfFile, Connection conn, Path tmpDir) throws IOException, FileNotFoundException, SQLException, ClassNotFoundException {
    HOSMDbExtract.extract(pbfFile, conn, tmpDir);
    HOSMDbTransform.transform(pbfFile, conn, tmpDir);
  }

  /**
   * Load a PBF-File eg from Geofabrik into your desired JDBC-Compatible BigDB.
   *
   * @param pbfFile pbf-File
   * @param connOSHDb Connection to a Database. H2 is well supported.
   * @param tmpDir temporary Directory
   * @param keyTables Connection to a KeyTable Database. This is independed of
   * the BigDB as it is only used to translate Tags and roles between String and
   * BigDb-Integer.
   * @throws IOException
   * @throws FileNotFoundException
   * @throws SQLException
   * @throws ClassNotFoundException
   * @see <a href="http://download.geofabrik.de/"> Geofabrik</a>
   */
  public static void toJDBC(File pbfFile, Connection connOSHDb, Path tmpDir, Connection keyTables) throws IOException, FileNotFoundException, SQLException, ClassNotFoundException {
    HOSMDbExtract.extract(pbfFile, keyTables, tmpDir);
    HOSMDbTransform.transform(pbfFile, connOSHDb, tmpDir, keyTables);

  }

  /** Load the extracted Data into an Ignite Session. 
   *
   * @param connOSHDb to the BigDB
   * @param ignite 
   */
  public static void intoIgnite(Connection connOSHDb, Ignite ignite) {

  }

  public static void main(String[] args) throws IOException, FileNotFoundException, ClassNotFoundException, SQLException {
    HOSMDbExtract.main(args);
    HOSMDbTransform.main(args);
  }

  private PBF2BigDB() {
  }

}
