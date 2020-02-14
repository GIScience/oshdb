package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagInterface;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagKey;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;

interface TagFilter extends Filter {
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
