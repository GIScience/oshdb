package org.heigit.ohsome.filter;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTagKey;

/**
 * A tag filter which executes a "key!=*" check.
 */
public class TagFilterNotEqualsAny implements TagFilter {
  private final OSHDBTagKey tag;

  TagFilterNotEqualsAny(OSHDBTagKey tag) {
    this.tag = tag;
  }

  @Override
  public boolean applyOSM(OSMEntity e) {
    return !e.hasTagKey(tag.toInt());
  }

  @Override
  public FilterExpression negate() {
    return new TagFilterEqualsAny(tag);
  }

  @Override
  public String toString() {
    return "tag:" + tag.toInt() + "!=*";
  }
}
