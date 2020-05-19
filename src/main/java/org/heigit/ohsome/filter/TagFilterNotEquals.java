package org.heigit.ohsome.filter;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;

/**
 * A tag filter which executes a "key!=value" check.
 */
public class TagFilterNotEquals implements TagFilter {
  private final OSHDBTag tag;

  TagFilterNotEquals(OSHDBTag tag) {
    this.tag = tag;
  }

  /**
   * Returns the OSM tag of this filter.
   *
   * @return the OSM tag of this filter.
   */
  public OSHDBTag getTag() {
    return this.tag;
  }

  @Override
  public boolean applyOSM(OSMEntity entity) {
    return !entity.hasTagValue(tag.getKey(), tag.getValue());
  }

  @Override
  public FilterExpression negate() {
    return new TagFilterEquals(tag);
  }

  @Override
  public String toString() {
    return "tag:" + tag.getKey() + "!=" + tag.getValue();
  }
}
