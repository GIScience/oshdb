package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;

/**
 * A tag filter which executes a "key=value" check.
 */
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
  public boolean applyOSH(OSHEntity e) {
    return e.hasTagKey(tag.getKey());
  }

  @Override
  public String toString() {
    return "tag:" + tag.getKey() + "=" + tag.getValue();
  }
}
