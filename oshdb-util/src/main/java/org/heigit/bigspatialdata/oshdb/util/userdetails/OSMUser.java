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

  /** Create a user with all names.
   *
   * @param id The user-id
   * @param names A list of all names a user had
   */
  public OSMUser(long id, ArrayList<String> names) {
    this.id = id;
    this.names = names;
  }

  /** Create a user with most recent name.
   *
   * @param id The user-id
   * @param name The most recent name a user has
   */
  public OSMUser(long id, String name) {
    this.id = id;
    ArrayList<String> namesTemp = new ArrayList<>(1);
    namesTemp.add(name);
    this.names = namesTemp;
  }

  /** Get id of a user.
   *
   * @return The id of the user
   */
  public long getId() {
    return id;
  }

  /**
   * Get latest name of User.
   *
   * @return The latest name of the user
   */
  public String getCurrentName() {
    return names.get(0);
  }

  /**
   * Get ALL names a user had in his/her mapping carrer.
   *
   * @return A list of all names a user ever had
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
