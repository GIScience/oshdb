package org.heigit.ohsome.filter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;

/**
 * A tag filter which executes a "key not in (value1, value2, …)" check.
 */
public class TagFilterNotEqualsAnyOf implements Filter {
  private final Set<OSHDBTag> tags;
  private final int keyId;

  TagFilterNotEqualsAnyOf(@Nonnull Collection<OSHDBTag> tags) {
    Optional<OSHDBTag> firstTag = tags.stream().findFirst();
    if (!firstTag.isPresent()) {
      throw new IllegalStateException("list of tags must not be empty in a key in (values) filter");
    } else {
      this.keyId = firstTag.get().getKey();
      this.tags = new HashSet<>(tags);
    }
    if (!tags.stream().allMatch(tag -> tag.getKey() == this.keyId)) {
      throw new IllegalStateException(
          "list of tags must all share the same tag key in a key in (values) filter");
    }
  }

  @Override
  public boolean applyOSM(OSMEntity e) {
    if (!e.hasTagKey(this.keyId)) {
      return true;
    }
    for (OSHDBTag entityTag : e.getTags()) {
      int entityTagKey = entityTag.getKey();
      if (entityTagKey == keyId && this.tags.contains(entityTag)) {
        return false;
      } else if (entityTagKey > keyId) {
        break;
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
