package org.heigit.ohsome.oshdb.filter;

import java.util.Collection;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.OSHDBTag;

/**
 * A tag filter which executes a "key in (value1, value2, …)" check.
 */
public class TagFilterEqualsAnyOf extends TagFilterAnyOf {
  TagFilterEqualsAnyOf(@Nonnull Collection<OSHDBTag> tags) {
    super(tags);
  }

  @Override
  public boolean applyOSM(OSMEntity e) {
    for (OSHDBTag entityTag : e.getTags()) {
      int entityTagKey = entityTag.getKey();
      if (entityTagKey == keyId) {
        return this.tags.contains(entityTag);
      } else if (entityTagKey > keyId) {
        return false;
      }
    }
    return false;
  }

  @Override
  public boolean applyOSH(OSHEntity e) {
    return e.hasTagKey(this.keyId);
  }

  @Override
  public FilterExpression negate() {
    return new TagFilterNotEqualsAnyOf(tags);
  }

  @Override
  public String toString() {
    return "tag:" + keyId + "in" + tags.stream().map(OSHDBTag::getValue).map(String::valueOf)
        .collect(Collectors.joining(","));
  }
}
