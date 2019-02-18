package org.heigit.bigspatialdata.oshdb.tool.importer.util.etl;

import java.nio.file.Path;
import java.util.Set;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class EtlFileStore implements EtlStore {

  private static final Logger LOG = LoggerFactory.getLogger(EtlFileStore.class);
  private final Path path;

  /**
   *
   * @param path
   */
  public EtlFileStore(Path path) {
    this.path = path;
  }

  @Override
  public void appendEntity(OSHEntity entity, Set<Long> newMemberNodes, Set<Long> newMemberWays,
      Set<Long> newMemberRelations) {
  }

  @Override
  public OSHEntity getEntity(OSMType type, long id) {
    return null;
  }

}
