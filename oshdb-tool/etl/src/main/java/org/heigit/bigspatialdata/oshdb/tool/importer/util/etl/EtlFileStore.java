package org.heigit.bigspatialdata.oshdb.tool.importer.util.etl;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An ETL-Store that stores the list of OSH-Entites in a file.
 */
public class EtlFileStore implements EtlStore {

  private static final Logger LOG = LoggerFactory.getLogger(EtlFileStore.class);
  private final Path path;

  /**
   * Creates a new ETL-FileStore defining the path of the files and providing the methods to read
   * and write OSH-Objects.
   *
   * @param path Path ot the ETL-Files.
   */
  public EtlFileStore(Path path) {
    this.path = path;
  }

  @Override
  public void appendEntity(OSHEntity entity, Set<Long> newMemberNodes, Set<Long> newMemberWays) {
  }

  @Override
  public Map<OSMType, Map<Long, OSHEntity>> getDependent(OSMType type, long id) {
    return new HashMap<>(0);
  }

  @Override
  public OSHEntity getEntity(OSMType type, long id) {
    return null;
  }

}
