package org.heigit.ohsome.oshdb.store;

import java.util.Collections;
import java.util.Set;
import org.heigit.ohsome.oshdb.osm.OSMType;

public class BackRefs {
  private final OSMType type;
  private final long id;

  private final Set<Long> ways;
  private final Set<Long> relations;

  public BackRefs(OSMType type, long id, Set<Long> relations){
    this(type, id, Collections.emptySet(), relations);
  }

  public BackRefs(OSMType type, long id, Set<Long> ways, Set<Long> relations) {
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

  public Set<Long> ways() {
    return ways;
  }

  public Set<Long> relations() {
    return relations;
  }
}
