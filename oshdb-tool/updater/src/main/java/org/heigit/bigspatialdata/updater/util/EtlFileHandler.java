package org.heigit.bigspatialdata.updater.util;

import java.io.IOException;
import java.nio.file.Path;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;

public class EtlFileHandler {

  public static OSHEntity getEntity(Path etlFile, EntityType type, long id) throws EntityNotFoudException, IOException {
    return null;
  }

  public static void appendEntity(Path etlFile, OSHEntity entity) {

  }

}
