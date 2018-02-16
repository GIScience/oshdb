package org.heigit.bigspatialdata.oshdb.tool.importer.osh;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

public abstract class TransformOSHEntity<OSM extends OSMEntity> extends OSHEntity<OSM>{


  public TransformOSHEntity(byte[] data, int offset, int length, long baseId, long baseTimestamp, long baseLongitude,
      long baseLatitude, byte header, long id, OSHDBBoundingBox bbox, int[] keys, int dataOffset, int dataLength) {
    super(data, offset, length, baseId, baseTimestamp, baseLongitude, baseLatitude, header, id, bbox, keys, dataOffset,dataLength);
  }

  @Override
  public OSHEntity<OSM> rebase(long baseId, long baseTimestamp, long baseLongitude, long baseLatitude)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<OSHDBTimestamp> getModificationTimestamps(boolean recurse) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<OSHDBTimestamp, Long> getChangesetTimestamps() {
    throw new UnsupportedOperationException();
  }

}
