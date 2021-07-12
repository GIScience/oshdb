package org.heigit.ohsome.oshdb.tool.importer.transform.oshdb;

import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.impl.osh.OSHEntityImpl;

public abstract class TransformOSHEntity extends OSHEntityImpl {

  public TransformOSHEntity(byte[] data, int offset, int length, long baseTimestamp,
      long baseLongitude, long baseLatitude, byte header, long id, OSHDBBoundingBox bbox,
      int[] keys, int dataOffset, int dataLength) {
    super(data, offset, length, baseTimestamp, baseLongitude, baseLatitude, header, id,
        bbox.getMinLongitude(), bbox.getMinLatitude(), bbox.getMaxLongitude(), bbox.getMaxLatitude(),
        keys, dataOffset, dataLength);
  }
}
