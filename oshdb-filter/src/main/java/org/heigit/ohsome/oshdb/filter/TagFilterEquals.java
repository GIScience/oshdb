package org.heigit.ohsome.oshdb.filter;

import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;

/**
 * A tag filter which executes a "key=value" check.
 */
public class TagFilterEquals implements TagFilter {
  private final OSHDBTag tag;

  TagFilterEquals(OSHDBTag tag) {
    this.tag = tag;
  }

  @Override
  public OSHDBTag getTag() {
    return this.tag;
  }

  @Override
  public boolean applyOSM(OSMEntity entity) {
    return entity.hasTagValue(tag.getKey(), tag.getValue());
  }

  @Override
  public boolean applyOSH(OSHEntity entity) {
    return entity.hasTagKey(tag.getKey());
  }

  @Override
  public FilterExpression negate() {
    return new TagFilterNotEquals(tag);
  }

  @Override
  public String toString() {
    return "tag:" + tag.getKey() + "=" + tag.getValue();
  }
}
