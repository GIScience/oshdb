package org.heigit.ohsome.oshdb;

import java.io.Serializable;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * Class for representing the role of a relation member.
 *
 */
public class OSHDBRole implements Serializable {
  /**
   * The empty OSHDBRole.
   */
  public static final OSHDBRole EMPTY = new OSHDBRole(-1);

  private static final int CACHE_SIZE = 256;
  private static final OSHDBRole[] cache = IntStream.range(0, CACHE_SIZE)
          .mapToObj(OSHDBRole::new)
          .toArray(OSHDBRole[]::new);

  /**
   * Returns an {@code OSHDBRole} instance representing the specified
   * {@code role id} value.
   *
   * @param id integer id of the OSHDBRole.
   * @return OSHDBRole instance.
   */
  public static OSHDBRole of(int id) {
    if (id == -1) {
      return EMPTY;
    }
    if (id >= 0 && id < CACHE_SIZE) {
      return cache[id];
    }
    return new OSHDBRole(id);
  }

  private static final long serialVersionUID = 1L;
  private int role;

  private OSHDBRole(int role) {
    this.role = role;
  }

  /**
   * Return integer id representation for this OSHDBRole object.
   *
   * @return integer id
   */
  public int getId() {
    return this.role;
  }

  @Override
  public int hashCode() {
    return Objects.hash(role);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof OSHDBRole)) {
      return false;
    }
    OSHDBRole other = (OSHDBRole) obj;
    return role == other.role;
  }

  @Override
  public String toString() {
    return Integer.toString(this.role);
  }
}
