package org.heigit.bigspatialdata.oshdb.tool.etl.extract;

import java.util.Map;
import java.util.SortedSet;

import org.heigit.bigspatialdata.oshdb.tool.etl.extract.data.KeyValuesFrequency;
import org.heigit.bigspatialdata.oshdb.tool.etl.extract.data.RelationMapping;
import org.heigit.bigspatialdata.oshpbf.HeaderInfo;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity.Type;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfUser;

public class ExtractMapperResult {

  final Map<String, KeyValuesFrequency> tagToKeyValuesFrequency;
  final Map<String, Integer> roleToFrequency;
  final SortedSet<OSMPbfUser> uniqueUser;
  final RelationMapping mapping;

  final HeaderInfo headerInfo;
  final Map<Type, Long> pbfTypeBlockFirstPosition;
  final long countNodes;
  final long countWays;
  final long countRelations;

  public ExtractMapperResult( //
      final Map<String, KeyValuesFrequency> tagToKeyValuesFrequency, //
      final Map<String, Integer> roleToFrequency, //
      final SortedSet<OSMPbfUser> uniqueUser, //
      final RelationMapping mapping, //

      final HeaderInfo headerInfo, //
      final Map<Type, Long> pbfTypeBlockFirstPosition, //
      final long countNodes, //
      final long countWays, //
      final long countRelations) {
    
    this.tagToKeyValuesFrequency = tagToKeyValuesFrequency;
    this.roleToFrequency = roleToFrequency;
    this.uniqueUser = uniqueUser;
    this.mapping = mapping;
    this.headerInfo = headerInfo;
    this.pbfTypeBlockFirstPosition = pbfTypeBlockFirstPosition;
    this.countNodes = countNodes;
    this.countWays = countWays;
    this.countRelations = countRelations;
  }

  public Map<String, KeyValuesFrequency> getTagToKeyValuesFrequency() {
    return tagToKeyValuesFrequency;
  }

  public Map<String, Integer> getRoleToFrequency() {
    return roleToFrequency;
  }

  public SortedSet<OSMPbfUser> getUniqueUser() {
    return uniqueUser;
  }

  public RelationMapping getMapping() {
    return mapping;
  }

  public HeaderInfo getHeaderInfo() {
    return headerInfo;
  }

  public Map<Type, Long> getPbfTypeBlockFirstPosition() {
    return pbfTypeBlockFirstPosition;
  }

  public long getCountNodes() {
    return countNodes;
  }

  public long getCountWays() {
    return countWays;
  }

  public long getCountRelations() {
    return countRelations;
  }
  
  
  
}
