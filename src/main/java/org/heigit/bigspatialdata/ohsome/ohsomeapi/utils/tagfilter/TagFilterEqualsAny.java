package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTagKey;

/**
 * A tag filter which executes a "key=*" check.
 */
public class TagFilterEqualsAny implements TagFilter {
  OSHDBTagKey tag;

  TagFilterEqualsAny(OSHDBTagKey tag) {
    this.tag = tag;
  }

  @Override
  public boolean applyOSM(OSMEntity e) {
    return e.hasTagKey(tag.toInt());
  }

  @Override
  public boolean applyOSH(OSHEntity e) {
    return e.hasTagKey(tag);
  }

  @Override
  public String toString() {
    return "tag:" + tag.toInt() + "!=*";
  }
}
