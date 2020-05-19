package org.heigit.ohsome.filter;

import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagInterface;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagKey;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import org.jetbrains.annotations.NotNull;

/**
 * A filter which selects OSM entities by their OSM tags (e.g., key=value, key=*, etc.).
 */
public interface TagFilter extends Filter {
  enum Type {
    EQUALS,
    NOT_EQUALS
  }

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
  static TagFilter fromSelector(@NotNull Type selector, OSMTagInterface tag, TagTranslator tt) {
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
