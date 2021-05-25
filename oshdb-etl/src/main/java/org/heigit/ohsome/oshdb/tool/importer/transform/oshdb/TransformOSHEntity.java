package org.heigit.ohsome.oshdb.tool.importer.transform.oshdb;

import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.impl.osh.OSHEntityImpl;

public abstract class TransformOSHEntity extends OSHEntityImpl {

  protected TransformOSHEntity(byte[] data, int offset, int length, long baseTimestamp,
      long baseLongitude, long baseLatitude, byte header, long id, OSHDBBoundingBox bbox,
      int[] keys, int dataOffset, int dataLength) {
    super(props(data, offset, length, 0, baseTimestamp, Math.toIntExact(baseLongitude),
        Math.toIntExact(baseLatitude), header, id, bbox.getMinLongitude(), bbox.getMinLatitude(),
        bbox.getMaxLongitude(), bbox.getMaxLatitude(), keys, dataOffset, dataLength));
  }

  private static CommonEntityProps props(byte[] data, int offset, int length, long baseId,
      long baseTimestamp, int baseLongitude, int baseLatitude, byte header, long id, int minLon,
      int minLat, int maxLon, int maxLat, int[] keys, int dataOffset, int dataLength) {
    var p = new CommonEntityProps(data, offset, length);
    p.setHeader(header);

    p.setMinLon(minLon);
    p.setMaxLon(maxLon);
    p.setMinLat(minLat);
    p.setMaxLat(maxLat);

    p.setBaseId(baseId);
    p.setBaseTimestamp(baseTimestamp);
    p.setBaseLongitude(baseLongitude);
    p.setBaseLatitude(baseLatitude);
    p.setId(id);
    p.setKeys(keys);
    p.setDataOffset(dataOffset);
    p.setDataLength(dataLength);
    return p;
  }
}
