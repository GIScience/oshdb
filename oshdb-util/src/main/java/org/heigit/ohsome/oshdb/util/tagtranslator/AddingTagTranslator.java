package org.heigit.ohsome.oshdb.util.tagtranslator;

import java.util.Collection;
import java.util.Map;
import org.heigit.ohsome.oshdb.OSHDBTag;

public interface AddingTagTranslator extends TagTranslator {

   Map<OSMTag, OSHDBTag> getOrAddOSHDBTagOf(Collection<OSMTag> tags);
}
