package org.heigit.ohsome.filter;

import java.util.Collection;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;

/**
 * A tag filter which executes a "key not in (value1, value2, …)" check.
 */
public class TagFilterNotEqualsAnyOf extends TagFilterAnyOf {
  TagFilterNotEqualsAnyOf(@Nonnull Collection<OSHDBTag> tags) {
    super(tags);
  }

  @Override
  public boolean applyOSM(OSMEntity e) {
    for (OSHDBTag entityTag : e.getTags()) {
      int entityTagKey = entityTag.getKey();
      if (entityTagKey == keyId) {
        return !this.tags.contains(entityTag);
      } else if (entityTagKey > keyId) {
        return true;
      }
    }
    return true;
  }

  @Override
  public FilterExpression negate() {
    return new TagFilterEqualsAnyOf(tags);
  }

  @Override
  public String toString() {
    return "tag:" + keyId + "not-in" + tags.stream().map(OSHDBTag::getValue).map(String::valueOf)
        .collect(Collectors.joining(","));
  }
}
