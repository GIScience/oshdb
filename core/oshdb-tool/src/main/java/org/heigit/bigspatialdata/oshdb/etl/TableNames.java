package org.heigit.bigspatialdata.oshdb.etl;

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
  T_RELATIONS("grid_relation");

  private final String tablename;

  TableNames(String name) {
    this.tablename = name;
  }

  @Override
  public String toString() {
    return tablename;
  }
}
