package org.heigit.ohsome.filter;

import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTagKey;

/**
 * A tag filter which executes a "key=*" check.
 */
public class TagFilterEqualsAny implements TagFilter {
  private final OSHDBTagKey tag;

  TagFilterEqualsAny(OSHDBTagKey tag) {
    this.tag = tag;
  }

  @Override
  public OSHDBTagKey getTag() {
    return this.tag;
  }

  @Override
  public boolean applyOSM(OSMEntity entity) {
    return entity.hasTagKey(tag.toInt());
  }

  @Override
  public boolean applyOSH(OSHEntity entity) {
    return entity.hasTagKey(tag);
  }

  @Override
  public FilterExpression negate() {
    return new TagFilterNotEqualsAny(tag);
  }

  @Override
  public String toString() {
    return "tag:" + tag.toInt() + "=*";
  }
}