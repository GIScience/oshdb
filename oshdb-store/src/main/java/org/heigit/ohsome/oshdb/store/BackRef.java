package org.heigit.ohsome.oshdb.store;

import static java.util.Collections.emptySet;

import java.util.Set;
import org.heigit.ohsome.oshdb.osm.OSMType;

public class BackRef {
  private final OSMType type;
  private final long id;

  private final Set<Long> ways;
  private final Set<Long> relations;

  public BackRef(OSMType type, long id, Set<Long> relations) {
    this(type, id, emptySet(), relations);
  }

  public BackRef(OSMType type, long id, Set<Long> ways, Set<Long> relations) {
    this.type = type;
    this.id = id;
    this.ways = ways;
    this.relations = relations;
  }

  public OSMType getType() {
    return type;
  }

  public long getId() {
    return id;
  }

  public Set<Long> ways(){
    return ways;
  }

  public Set<Long> relations(){
    return relations;
  }
}
