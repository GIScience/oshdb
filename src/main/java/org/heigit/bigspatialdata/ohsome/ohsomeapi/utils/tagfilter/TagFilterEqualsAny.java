package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTagKey;

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
  public String toString() {
    return "tag:" + tag.toInt() + "!=*";
  }
}