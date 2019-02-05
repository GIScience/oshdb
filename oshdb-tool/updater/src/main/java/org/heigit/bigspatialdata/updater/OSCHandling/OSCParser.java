package org.heigit.bigspatialdata.updater.OSCHandling;

import java.util.Iterator;
import org.heigit.bigspatialdata.updater.util.ChangeIterator;
import org.heigit.bigspatialdata.updater.util.ReplicationFile;
import org.openstreetmap.osmosis.core.container.v0_6.ChangeContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OSCParser {
  private static final Logger LOG = LoggerFactory.getLogger(OSCParser.class);

  /**
   * Represents the transform-step in an etl pipeline.
   *
   * @param replicationFiles
   */
  public static Iterable<ChangeContainer> parse(Iterable<ReplicationFile> replicationFiles) {
    Iterable<ChangeContainer> changes = new Iterable<ChangeContainer>() {
      @Override
      public Iterator<ChangeContainer> iterator() {
        return new ChangeIterator(replicationFiles);
      }
    };
    return changes;
  }
}
