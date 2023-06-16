package org.heigit.ohsome.oshdb.filter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.OSHDBTag;

/**
 * A tag filter which executes a "key [not] in (value1, value2, …)" check.
 */
abstract class TagFilterAnyOf implements Filter {
  final int keyId;
  final HashSet<OSHDBTag> tags;

  TagFilterAnyOf(@Nonnull Collection<OSHDBTag> tags) {
    Optional<OSHDBTag> firstTag = tags.stream().findFirst();
    this.keyId = firstTag
            .orElseThrow(() -> new IllegalStateException("list of tags must not be empty in a key in (values) filter"))
            .getKey();
    this.tags = new HashSet<>(tags);

    if (!tags.stream().allMatch(tag -> tag.getKey() == this.keyId)) {
      throw new IllegalStateException(
          "list of tags must all share the same tag key in a key in (values) filter");
    }
  }

  public int getKeyId() {
    return keyId;
  }

  public Set<OSHDBTag> getTags() {
    return tags;
  }
}
