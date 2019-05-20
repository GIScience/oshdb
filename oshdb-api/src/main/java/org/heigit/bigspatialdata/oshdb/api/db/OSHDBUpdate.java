package org.heigit.bigspatialdata.oshdb.api.db;

import java.sql.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OSHDBUpdate extends OSHDBJdbc {

  private static final Logger LOG = LoggerFactory.getLogger(OSHDBUpdate.class);
  protected Connection bitArrayConnection;
  private int batchSize;

  public OSHDBUpdate(Connection conn) {
    super(conn);
    this.bitArrayConnection = conn;
    this.batchSize = 100;
  }

  /**
   * @return the batchSize
   */
  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public Connection getBitArrayDb() {
    return this.bitArrayConnection;
  }

  //might add some extra metadata like update timestamp etc.
  public void setBitArrayDb(Connection conn) {
    this.bitArrayConnection = conn;
  }

}
