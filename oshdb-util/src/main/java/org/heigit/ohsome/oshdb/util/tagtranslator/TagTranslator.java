package org.heigit.ohsome.oshdb.util.tagtranslator;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.heigit.ohsome.oshdb.OSHDBRole;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBTagOrRoleNotFoundException;

public interface TagTranslator {

  /**
   * Get oshdb's internal representation of a tag key (string).
   *
   * @param key the tag key as a string
   * @return the corresponding oshdb representation of this key
   */
  default Optional<OSHDBTagKey> getOSHDBTagKeyOf(String key) {
    return getOSHDBTagKeyOf(new OSMTagKey(key));
  }

  /**
   * Get oshdb's internal representation of a tag key.
   *
   * @param key the tag key as an OSMTagKey object
   * @return the corresponding oshdb representation of this key
   */
  Optional<OSHDBTagKey> getOSHDBTagKeyOf(OSMTagKey key);

  /**
   * Get oshdb's internal representation of a tag (key-value string pair).
   *
   * @param key the (string) key of the tag
   * @param value the (string) value of the tag
   * @return the corresponding oshdb representation of this tag
   */
  default Optional<OSHDBTag> getOSHDBTagOf(String key, String value) {
    return getOSHDBTagOf(new OSMTag(key, value));
  }

  /**
   * Get oshdb's internal representation of a tag (key-value pair).
   *
   * @param tag a key-value pair as an OSMTag object
   * @return the corresponding oshdb representation of this tag
   */
  Optional<OSHDBTag> getOSHDBTagOf(OSMTag tag);

  Map<OSMTag, OSHDBTag> getOSHDBTagOf(Collection<OSMTag> tags);

  /**
   * Get oshdb's internal representation of a role (string).
   *
   * @param role the role string to fetch
   * @return the corresponding oshdb representation of this role
   */
  default Optional<OSHDBRole> getOSHDBRoleOf(String role) {
    return getOSHDBRoleOf(new OSMRole(role));
  }

  /**
   * Get oshdb's internal representation of a role.
   *
   * @param role the role to fetch as an OSMRole object
   * @return the corresponding oshdb representation of this role
   */
  Optional<OSHDBRole> getOSHDBRoleOf(OSMRole role);

  Map<OSMRole, OSHDBRole> getOSHDBRoleOf(Collection<OSMRole> roles);

  /**
   * Get a tag's string representation from oshdb's internal data format.
   *
   * @param tag the tag (as an OSHDBTag object)
   * @return the textual representation of this tag
   */
  OSMTag lookupTag(OSHDBTag tag);

  Map<OSHDBTag, OSMTag> lookupTag(Set<? extends OSHDBTag> tags);

  /**
   * Get a tag's string representation from oshdb's internal data format.
   *
   * @param key the key of the tag (represented as an integer)
   * @param value the value of the tag (represented as an integer)
   * @return the textual representation of this tag
   */
  default OSMTag lookupTag(int key, int value) {
    return lookupTag(new OSHDBTag(key, value));
  }

  /**
   * Get a role's string representation from oshdb's internal data format.
   *
   * @param role the role ID (as an OSHDBRole object)
   * @return the textual representation of this role
   */
  OSMRole lookupRole(OSHDBRole role);

  /**
   * Get a role's string representation from oshdb's internal data format.
   *
   * @param role the role ID (represented as an integer)
   * @return the textual representation of this role
   */
  default OSMRole lookupRole(int role) {
    return lookupRole(OSHDBRole.of(role));
  }
}
