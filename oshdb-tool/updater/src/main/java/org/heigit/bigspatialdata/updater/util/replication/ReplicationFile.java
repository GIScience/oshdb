package org.heigit.bigspatialdata.updater.util.replication;

import java.io.File;
import org.openstreetmap.osmosis.replication.common.ReplicationState;

/**
 * Represents a replication-file downloaded form the replication-server.
 */
public class ReplicationFile {

  public final File file;
  public final ReplicationState state;

  /**
   * Sets the tow parameters of this replication file.
   *
   * @param state the state of that file
   * @param file the path to the file
   */
  public ReplicationFile(ReplicationState state, File file) {
    this.state = state;
    this.file = file;
  }

}
