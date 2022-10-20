package org.heigit.ohsome.oshdb.filter;

import com.google.common.collect.Streams;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;

/**
 * A tag filter which executes a "key!=value" check.
 */
public class TagFilterNotEquals implements TagFilter {
  private final OSHDBTag tag;

  TagFilterNotEquals(OSHDBTag tag) {
    this.tag = tag;
  }

  @Override
  public OSHDBTag getTag() {
    return this.tag;
  }

  @Override
  public boolean applyOSH(OSHEntity entity) {
    return Streams.stream(entity.getVersions()).anyMatch(this::applyOSM);
  }

  @Override
  public boolean applyOSM(OSMEntity entity) {
    return !entity.getTags().hasTagValue(tag.getKey(), tag.getValue());
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
