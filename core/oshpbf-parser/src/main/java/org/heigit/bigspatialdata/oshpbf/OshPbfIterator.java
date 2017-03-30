package org.heigit.bigspatialdata.oshpbf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity;

public class OshPbfIterator implements Iterator<List<OSMPbfEntity>> {


  private final Iterator<OSMPbfEntity> source;
  private List<OSMPbfEntity> versions = new ArrayList<>();
  private OSMPbfEntity last = null;

  public OshPbfIterator(Iterator<OSMPbfEntity> source) {
    this.source = source;
  }

  @Override
  public boolean hasNext() {
    return source.hasNext();
  }

  @Override
  public List<OSMPbfEntity> next() {
    List<OSMPbfEntity> ret = null;
    while (source.hasNext()) {
      OSMPbfEntity entity = source.next();
      if (last != null && (last.getId() != entity.getId() || last.getType() != entity.getType())) {
        ret = versions;
        versions = new ArrayList<>();
      }
      versions.add(entity);
      last = entity;
      if(ret != null)
        return ret;
    }
    return versions;
  }

}
