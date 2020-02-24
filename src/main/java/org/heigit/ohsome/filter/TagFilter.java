package org.heigit.ohsome.filter;

import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagInterface;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagKey;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;

/**
 * A filter which selects OSM entities by their OSM tags (e.g., key=value, key=*, etc.).
 */
public interface TagFilter extends Filter {
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
   * @throws IllegalStateException if an unknown selector was given.
   */
  static TagFilter fromSelector(String selector, OSMTagInterface tag, TagTranslator tt) {
    switch (selector) {
      case "=":
        if (tag instanceof OSMTag) {
          return new TagFilterEquals(tt.getOSHDBTagOf((OSMTag) tag));
        } else {
          return new TagFilterEqualsAny(tt.getOSHDBTagKeyOf((OSMTagKey) tag));
        }
      case "!=":
        if (tag instanceof OSMTag) {
          return new TagFilterNotEquals(tt.getOSHDBTagOf((OSMTag) tag));
        } else {
          return new TagFilterNotEqualsAny(tt.getOSHDBTagKeyOf((OSMTagKey) tag));
        }
      default:
        throw new IllegalStateException("unknown tagfilter selector: " + selector);
    }
  }
}
