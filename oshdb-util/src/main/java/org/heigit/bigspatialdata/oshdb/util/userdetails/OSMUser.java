package org.heigit.bigspatialdata.oshdb.util.userdetails;

import java.util.ArrayList;
import java.util.Objects;

/**
 * An OSMUser consisting of a unique ID and a changable Name. = a valid e-mail-address; not
 * necessarily = ONE mapper.
 */
public class OSMUser {
  private final long id;
  private final ArrayList<String> names;

  /**
   *
   * @param id
   * @param names
   */
  public OSMUser(long id, ArrayList<String> names) {
    this.id = id;
    this.names = names;
  }

  /**
   *
   * @param id
   * @param name
   */
  public OSMUser(long id, String name) {
    this.id = id;
    ArrayList<String> namesTemp = new ArrayList<>(1);
    namesTemp.add(name);
    this.names = namesTemp;
  }

  /**
   *
   * @return
   */
  public long getId() {
    return id;
  }

  /**
   * Get latest name of User.
   *
   * @return
   */
  public String getCurrentName() {
    return names.get(0);
  }

  /**
   * Get ALL names a user had in his/her mapping carrer.
   *
   * @return
   */
  public ArrayList<String> getAllNames() {
    return names;
  }

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 59 * hash + (int) (this.id ^ (this.id >>> 32));
    hash = 59 * hash + Objects.hashCode(this.names);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final OSMUser other = (OSMUser) obj;
    if (this.id != other.id) {
      return false;
    }
    return Objects.equals(this.names, other.names);
  }

  @Override
  public String toString() {
    return "OSMUser{" + "id=" + id + ", names=" + names + '}';
  }

}
