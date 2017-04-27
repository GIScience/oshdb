package org.heigit.bigspatialdata.hosmdb.etl.extract;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.heigit.bigspatialdata.hosmdb.etl.extract.data.KeyValuesFrequency;
import org.heigit.bigspatialdata.hosmdb.etl.extract.data.RelationMapping;
import org.heigit.bigspatialdata.oshpbf.HeaderInfo;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity.Type;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfUser;

public class ExtractReducer {

  public ExtractMapperResult reduce(Iterable<ExtractMapperResult> mapResults) {


    final Map<String, KeyValuesFrequency> tagToKeyValuesFrequency = new HashMap<>();
    final Map<String, Integer> roleToFrequency = new HashMap<>();;
    final SortedSet<OSMPbfUser> uniqueUser = new TreeSet<>();;
    final RelationMapping mapping = new RelationMapping();
    final Map<Type, Long> pbfTypeBlockFirstPosition = new HashMap<>();

    HeaderInfo headerInfo = null;

    long countNodes = 0;
    long countWays = 0;
    long countRelations = 0;


    for (ExtractMapperResult mapResult : mapResults) {

      for (Map.Entry<String, KeyValuesFrequency> entry : mapResult.getTagToKeyValuesFrequency()
          .entrySet()) {
        KeyValuesFrequency keyValuesFrequency = tagToKeyValuesFrequency.get(entry.getKey());
        if (keyValuesFrequency == null) {
          tagToKeyValuesFrequency.put(entry.getKey(), entry.getValue());
        } else {
          keyValuesFrequency.inc();
          Map<String, Integer> valueFrequency = keyValuesFrequency.values();
          for (Map.Entry<String, Integer> entry2 : entry.getValue().values().entrySet()) {
            Integer frequency = valueFrequency.get(entry2.getKey());
            if (frequency == null) {
              valueFrequency.put(entry2.getKey(), entry2.getValue());
            } else {
              frequency = Integer.valueOf(frequency.intValue() + entry2.getValue().intValue());
              valueFrequency.put(entry2.getKey(), frequency);
            }
          }
        }
      }

      for (Map.Entry<String, Integer> entry : mapResult.getRoleToFrequency().entrySet()) {
        Integer frequency = roleToFrequency.get(entry.getKey());
        if (frequency == null) {
          roleToFrequency.put(entry.getKey(), entry.getValue());
        } else {
          frequency = Integer.valueOf(frequency.intValue() + entry.getValue().intValue());
          roleToFrequency.put(entry.getKey(), frequency);
        }
      }

      uniqueUser.addAll(mapResult.getUniqueUser());

      RelationMapping mapResultMapping = mapResult.getMapping();

      mergeRelationMapping(mapping.nodeToWay(), mapResultMapping.nodeToWay());
      mergeRelationMapping(mapping.nodeToRelation(), mapResultMapping.nodeToRelation());
      mergeRelationMapping(mapping.wayToRelation(), mapResultMapping.wayToRelation());
      mergeRelationMapping(mapping.relationToRelation(), mapResultMapping.relationToRelation());


      headerInfo = mapResult.getHeaderInfo();
      countNodes += mapResult.getCountNodes();
      countWays += mapResult.getCountWays();
      countRelations += mapResult.getCountRelations();
      pbfTypeBlockFirstPosition.putAll(mapResult.getPbfTypeBlockFirstPosition());
    }
    
    return new ExtractMapperResult(tagToKeyValuesFrequency, roleToFrequency, uniqueUser, mapping, headerInfo, pbfTypeBlockFirstPosition, countNodes, countWays, countRelations);
  }

  private void mergeRelationMapping(//
      final Map<Long, SortedSet<Long>> mapping, //
      final Map<Long, SortedSet<Long>> resultMapping) {
    for (Map.Entry<Long, SortedSet<Long>> entry : resultMapping.entrySet()) {
      SortedSet<Long> relations = mapping.get(entry.getKey());
      if (relations == null) {
        mapping.put(entry.getKey(), entry.getValue());
      } else {
        relations.addAll(entry.getValue());
      }
    }
  }



}
