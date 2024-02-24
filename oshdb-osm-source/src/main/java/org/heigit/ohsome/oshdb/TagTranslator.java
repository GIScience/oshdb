package org.heigit.ohsome.oshdb;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMRole;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTag;

public interface TagTranslator {

  Map<OSMTag, OSHDBTag> getOSHDBTagOf(Collection<? extends OSMTag> tags);

  Map<OSHDBTag, OSMTag> lookupTag(Set<? extends OSHDBTag> tags);

  Map<OSMRole, OSHDBRole> getOSHDBRoleOf(Collection<? extends OSMRole> values);

  Map<OSHDBRole, OSMRole> lookupRole(Set<? extends OSHDBRole> roles);
}
