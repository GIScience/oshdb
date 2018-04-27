package org.heigit.bigspatialdata.oshdb;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;

import java.util.Optional;

/**
 * Names for JDBC-Tables.
 */
public enum TableNames {

  /**
   * Table in
   * {@link org.heigit.bigspatialdata.oshdb.etl.EtlFiles#E_TEMPRELATIONS}
   * holding nodes that are parts of ways.
   */
  E_NODE2WAY("node2way"),
  /**
   * Table in
   * {@link org.heigit.bigspatialdata.oshdb.etl.EtlFiles#E_TEMPRELATIONS}
   * holding nodes that are parts of relations.
   */
  E_NODE2RELATION("node2relation"),
  /**
   * Table in
   * {@link org.heigit.bigspatialdata.oshdb.etl.EtlFiles#E_TEMPRELATIONS}
   * holding ways that are parts of relations.
   */
  E_WAY2RELATION("way2relation"),
  /**
   * Table in
   * {@link org.heigit.bigspatialdata.oshdb.etl.EtlFiles#E_TEMPRELATIONS}
   * holding relations that are parts of relations.
   */
  E_RELATION2RELATION("relation2relation"),
  /**
   * Table holding keys in keytables.
   */
  E_KEY("key"),
  /**
   * Table holding keysvalues in keytables.
   */
  E_KEYVALUE("keyvalue"),
  /**
   * Table holding roles in keytables.
   */
  E_ROLE("role"),
  /**
   * Table holding users in keytables.
   */
  E_USER("user"),
  /**
   * Table that holds Grid-OSH-Nodes in the oshdb.
   */
  T_NODES("grid_node"),
  /**
   * Table that holds Grid-OSH-Ways in the oshdb.
   */
  T_WAYS("grid_way"),
  /**
   * Table that holds Grid-OSH-Relations in the oshdb.
   */
  T_RELATIONS("grid_relation"),
  /**
   * Table that holds metadata in the oshdb.
   */
  T_METADATA("metadata");

  private final String tablename;

  TableNames(String name) {
    this.tablename = name;
  }

  @Override
  public String toString() {
    return tablename;
  }

  public String toString(String prefix) {
    if(prefix != null && !prefix.trim().isEmpty())
      return prefix+"_"+this.toString();
    return this.toString();
  }

  public static Optional<TableNames> forOSMType(OSMType type) {
    switch(type) {
      case NODE: return Optional.of(T_NODES);
      case WAY: return Optional.of(T_WAYS);
      case RELATION: return Optional.of(T_RELATIONS);
      default:
        return Optional.empty();
    }
  }
}
