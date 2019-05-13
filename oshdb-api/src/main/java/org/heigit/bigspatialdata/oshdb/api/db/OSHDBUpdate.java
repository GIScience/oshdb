package org.heigit.bigspatialdata.oshdb.api.db;

import java.sql.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OSHDBUpdate extends OSHDBJdbc {

  private static final Logger LOG = LoggerFactory.getLogger(OSHDBUpdate.class);
  protected Connection bitArrayConnection;

  public OSHDBUpdate(Connection conn) {
    super(conn);
    this.bitArrayConnection = conn;
  }

  public void setBitArrayDb(Connection conn) {
    this.bitArrayConnection = conn;
  }

  public Connection getBitArrayDb() {
    return this.bitArrayConnection;
  }

  //might add some extra metadata like update timestamp etc.
}
