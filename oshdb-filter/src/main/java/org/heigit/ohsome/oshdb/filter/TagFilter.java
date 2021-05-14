package org.heigit.ohsome.oshdb.filter;

import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTag;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTagInterface;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTagKey;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import org.jetbrains.annotations.Contract;

/**
 * A filter which selects OSM entities by their OSM tags (e.g., key=value, key=*, etc.).
 */
public interface TagFilter extends Filter {
  /**
   * Type of tag filter.
   */
  enum Type {
    /** A tag filter which checks for entities which match a key=value (or key=*) filter. */
    EQUALS,
    /** A tag filter which checks for entities which match a key!=value (or key!=*) filter. */
    NOT_EQUALS
  }

  /**
   * Returns the OSM tag or key of this filter.
   *
   * @return the OSM tag of this filter. Either an object of class OSHDBTag or OSHDBTagKey.
   */
  @Contract(pure = true)
  Object getTag();

  /**
   * Returns a new tag filter object fulfilling the given "selector" and OSM tag.
   *
   * <p>This can be either an exact tag match (key=value), or a tag presence check (key=*), or
   * the inverse of these.</p>
   *
   * @param selector The type of tag filter, such as "=" or "!=".
   * @param tag The tag to use for this filter - can be either a OSMTag or OSMTagKey object.
   * @param tt Tag Translator object for converting OSM tags to OSHDB tag ids
   * @return A new tag-filter object fulfilling the given parameters.
   */
  @Contract(pure = true)
  static TagFilter fromSelector(@Nonnull Type selector, OSMTagInterface tag, TagTranslator tt) {
    switch (selector) {
      case EQUALS:
        if (tag instanceof OSMTag) {
          return new TagFilterEquals(tt.getOSHDBTagOf((OSMTag) tag));
        } else {
          return new TagFilterEqualsAny(tt.getOSHDBTagKeyOf((OSMTagKey) tag));
        }
      case NOT_EQUALS:
        if (tag instanceof OSMTag) {
          return new TagFilterNotEquals(tt.getOSHDBTagOf((OSMTag) tag));
        } else {
          return new TagFilterNotEqualsAny(tt.getOSHDBTagKeyOf((OSMTagKey) tag));
        }
      default:
        assert false : "invalid or null tag filter selector encountered";
        throw new IllegalStateException("invalid or null tag filter selector encountered");
    }
  }
}
