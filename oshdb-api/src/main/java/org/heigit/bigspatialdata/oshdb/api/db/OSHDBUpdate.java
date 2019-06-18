package org.heigit.bigspatialdata.oshdb.api.db;

import java.sql.Connection;

/**
 * Defines a JDBC-Database where updates are stored.
 */
public class OSHDBUpdate extends OSHDBJdbc {

  protected Connection bitArrayConnection;
  private int batchSize;

  /**
   * Define a new update Database. Defaults store the bitmap in the same database and set a
   * batchsize of 1000.
   *
   * @param conn set JDBC-Connection
   */
  public OSHDBUpdate(Connection conn) {
    super(conn);
    this.bitArrayConnection = conn;
    this.batchSize = 1000;
  }

  /**
   * Get the size of Datachunks that should be processed in a batch.
   *
   * @return the batchSize
   */
  public int getBatchSize() {
    return batchSize;
  }

  /**
   * Set the number of OSHEntities that will be combined to one pseudo-GridOSHEntity when applying
   * updates.
   *
   * @param batchSize Number of entities in batch
   */
  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  /**
   * Get the database where the flagged bitmap of updated entities is stored.
   *
   * @return the connection to the bitmap database
   */
  public Connection getBitArrayDb() {
    return this.bitArrayConnection;
  }

  /**
   * Set the database where bitmaps are stored.
   *
   * @param conn the new connection to use for bitmaps
   */
  public void setBitArrayDb(Connection conn) {
    this.bitArrayConnection = conn;
  }

  //might add some extra metadata like update timestamp etc.
}
