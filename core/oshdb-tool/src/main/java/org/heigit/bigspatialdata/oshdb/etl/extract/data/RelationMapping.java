package org.heigit.bigspatialdata.oshdb.etl.extract.data;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;

public class RelationMapping {
  
  private final Map<Long, SortedSet<Long>> nodeToWay = new HashMap<>();
  private final Map<Long, SortedSet<Long>> nodeToRelation = new HashMap<>();
  private final Map<Long, SortedSet<Long>> wayToRelation = new HashMap<>();
  private final Map<Long, SortedSet<Long>> relationToRelation = new HashMap<>();
  
  public Map<Long, SortedSet<Long>> nodeToWay(){
    return nodeToWay;
  }
  
  public Map<Long, SortedSet<Long>> nodeToRelation(){
    return nodeToRelation;
  }
  
  public Map<Long, SortedSet<Long>> wayToRelation(){
    return wayToRelation;
  }
  
  public Map<Long, SortedSet<Long>> relationToRelation(){
    return relationToRelation;
  }

}
