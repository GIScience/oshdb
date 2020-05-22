package org.heigit.ohsome.filter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;

/**
 * A tag filter which executes a "key not in (value1, value2, …)" check.
 */
abstract class TagFilterAnyOf implements Filter {
  final int keyId;
  final Set<OSHDBTag> tags;

  TagFilterAnyOf(@Nonnull Collection<OSHDBTag> tags) {
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
}
