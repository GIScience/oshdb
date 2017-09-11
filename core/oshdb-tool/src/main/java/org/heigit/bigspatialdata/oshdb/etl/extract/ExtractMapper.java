package org.heigit.bigspatialdata.oshdb.etl.extract;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.heigit.bigspatialdata.oshdb.etl.extract.data.KeyValuesFrequency;
import org.heigit.bigspatialdata.oshdb.etl.extract.data.RelationMapping;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshpbf.HeaderInfo;
import org.heigit.bigspatialdata.oshpbf.OshPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPrimitiveBlockIterator;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity.Type;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfRelation;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfRelation.OSMMember;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfTag;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfUser;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfWay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtractMapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExtractMapper.class);

  public ExtractMapperResult map(InputStream in) {
    try ( //
            final OsmPrimitiveBlockIterator blockItr = new OsmPrimitiveBlockIterator(in)) {

      final OsmPbfIterator osmIterator = new OsmPbfIterator(blockItr);
      final OshPbfIterator oshIterator = new OshPbfIterator(osmIterator);

      final SortedMap<String, KeyValuesFrequency> tagToKeyValuesFrequency = new TreeMap<>();
      final Map<String, Integer> roleToFrequency = new HashMap<>();
      final SortedSet<OSMPbfUser> uniqueUser = new TreeSet<>();
      final RelationMapping mapping = new RelationMapping();

      final HeaderInfo headerInfo = blockItr.getHeaderInfo();
      final Map<Type, Long> pbfTypeBlockFirstPosition = new HashMap<>();
      long countNodes = 0;
      long countWays = 0;
      long countRelations = 0;

      while (oshIterator.hasNext()) {
        final List<OSMPbfEntity> versions = oshIterator.next();
        if (versions.isEmpty()) {
          LOGGER.warn("emyty list of versions!");
          continue;
        }
        final long id = versions.get(0).getId();
        final Type type = versions.get(0).getType();

        Long blockPosRelative = pbfTypeBlockFirstPosition.get(type);
        if (blockPosRelative == null) {
          blockPosRelative = Long.valueOf(blockItr.getBlockPos());
        } else {
          blockPosRelative
                  = Long.valueOf(Math.min(blockPosRelative.longValue(), blockItr.getBlockPos()));
        }
        pbfTypeBlockFirstPosition.put(type, blockPosRelative);

        switch (type) {
          case NODE:
            countNodes++;
            break;
          case WAY:
            countWays++;
            break;
          case RELATION:
            countRelations++;
            break;
          default:
            break;
        }

        tagKeyValues(id, type, versions, tagToKeyValuesFrequency);
        users(id, type, versions, uniqueUser);
        roles(id, type, versions, roleToFrequency);
        relationMapping(id, type, versions, mapping);
      } // while(oshIterator.hasNext()

      return new ExtractMapperResult(tagToKeyValuesFrequency, roleToFrequency, uniqueUser, mapping, headerInfo, pbfTypeBlockFirstPosition, countNodes, countWays, countRelations);

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  private void tagKeyValues(long id, Type type, List<OSMPbfEntity> versions, SortedMap<String, KeyValuesFrequency> tagToKeyValuesFrequency) {
    Set<OSMPbfTag> uniqueTags = new HashSet<>();
    for (OSMPbfEntity entity : versions) {
      for (OSMPbfTag tag : entity.getTags()) {
        uniqueTags.add(tag);
      }
    }

    for (OSMPbfTag tag : uniqueTags) {
      String key = tag.getKey();
      String value = tag.getValue();

      KeyValuesFrequency tvf = tagToKeyValuesFrequency.get(key);
      if (tvf == null) {
        tvf = new KeyValuesFrequency();
        tagToKeyValuesFrequency.put(key, tvf);
      }
      tvf.inc();

      Integer vf = tvf.values().get(value);
      if (vf == null) {
        vf = Integer.valueOf(0);
      }
      vf = Integer.valueOf(vf.intValue() + 1);
      tvf.values().put(value, vf);
    }
  }

  private void roles(long id, Type type, List<OSMPbfEntity> versions, Map<String, Integer> roleToFrequency) {
    if (type != Type.RELATION) {
      return;
    }

    Set<String> uniqueRoles = new HashSet<>();

    for (OSMPbfEntity entity : versions) {
      OSMPbfRelation relation = (OSMPbfRelation) entity;
      for (OSMMember member : relation.getMembers()) {
        String role = member.getRole().trim();
        uniqueRoles.add(role);
      }
    }

    for (String role : uniqueRoles) {
      Integer freq = roleToFrequency.get(role);
      if (freq == null) {
        freq = Integer.valueOf(0);
      }
      freq = Integer.valueOf(freq.intValue() + 1);
      roleToFrequency.put(role, freq);
    }
  }

  private void users(long id, Type type, List<OSMPbfEntity> versions, SortedSet<OSMPbfUser> uniqueUser) {
    for (OSMPbfEntity entity : versions) {
      OSMPbfUser user = entity.getUser();
      uniqueUser.add(user);
    }
  }

  private void relationMapping(long id, Type type, List<OSMPbfEntity> versions, RelationMapping mapping) {
    switch (type) {
      case NODE: {
        // nodes don't have a relation
        return;
      }
      case WAY: {
        Set<Long> uniqueRef = new HashSet<>();
        for (OSMPbfEntity entity : versions) {
          OSMPbfWay way = (OSMPbfWay) entity;
          for (Long ref : way.getRefs()) {
            uniqueRef.add(ref);
          }
        }

        addRelation(id, uniqueRef, mapping.nodeToWay());
        return;
      }
      case RELATION: {
        Set<Long> uniqueNodes = new HashSet<>();
        Set<Long> uniqueWays = new HashSet<>();
        Set<Long> uniqueRelation = new HashSet<>();

        for (OSMPbfEntity entity : versions) {
          OSMPbfRelation relation = (OSMPbfRelation) entity;
          for (OSMMember member : relation.getMembers()) {
              switch (OSMType.fromInt(member.getType())) {
                case NODE:
                  uniqueNodes.add(member.getMemId());
                  break;
                case WAY:
                  uniqueWays.add(member.getMemId());
                  break;
                case RELATION:
                  uniqueRelation.add(member.getMemId());
                  break;
                default:
                  LOGGER.error("unknown member type");
              }
          }
        }

        addRelation(id, uniqueNodes, mapping.nodeToRelation());
        addRelation(id, uniqueWays, mapping.wayToRelation());
        addRelation(id, uniqueRelation, mapping.relationToRelation());
        return;
      }
      case CHANGESET:
        break;
      case OTHER:
        break;
      default: {
        LOGGER.error("unknown type");
      }
    }
  }

  private void addRelation(long id, Set<Long> refs, Map<Long, SortedSet<Long>> map) {
    refs.forEach(ref -> {
      SortedSet<Long> relations = map.get(ref);
      if (relations == null) {
        relations = new TreeSet<>();
        map.put(ref, relations);
      }
      relations.add(id);
    });
  }
}
