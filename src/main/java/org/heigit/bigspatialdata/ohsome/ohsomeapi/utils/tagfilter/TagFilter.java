package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTagKey;

class TagFilter implements Filter {
  final String selector;
  final int tagKeyId;
  final int tagValueId;

  TagFilter(String selector, OSHDBTag tag) {
    this.selector = selector;
    this.tagKeyId = tag.getKey();
    this.tagValueId = tag.getValue();
  }

  TagFilter(String selector, OSHDBTagKey tag) {
    this.selector = selector;
    this.tagKeyId = tag.toInt();
    this.tagValueId = -1;
  }

  @Override
  public boolean applyOSM(OSMEntity e) {
    switch (selector) {
      case "=":
        return e.hasTagValue(this.tagKeyId, this.tagValueId);
      case "!=":
        return !e.hasTagValue(this.tagKeyId, this.tagValueId);
      case "=*":
        return e.hasTagKey(this.tagKeyId);
      case "!=*":
        return !e.hasTagKey(this.tagKeyId);
      default:
        throw new RuntimeException("unknown tagfilter selector: " + selector);
    }
  }

  @Override
  public String toString() {
    return "tag:" + tagKeyId + selector + tagValueId;
  }
}
