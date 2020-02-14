package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;

public class TagFilterEquals implements TagFilter {
  OSHDBTag tag;

  TagFilterEquals(OSHDBTag tag) {
    this.tag = tag;
  }

  @Override
  public boolean applyOSM(OSMEntity e) {
    return e.hasTagValue(tag.getKey(), tag.getValue());
  }

  @Override
  public String toString() {
    return "tag:" + tag.getKey() + "=" + tag.getValue();
  }
}
