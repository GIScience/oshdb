package org.heigit.bigspatialdata.updater.oschandling;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.bigspatialdata.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.bigspatialdata.oshdb.impl.osh.OSHWayImpl;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.etl.EtlFileStore;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.etl.EtlStore;
import org.heigit.bigspatialdata.oshdb.util.OSHDBRole;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.TableNames;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMRole;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import org.openstreetmap.osmosis.core.container.v0_6.ChangeContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.RelationMember;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.common.ChangeAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a static method to transform osmium-ChangeContainrs to OSHDB-Objects. Returns itself as
 * an Iterator.
 */
public class OscOshTransformer implements Iterator<Map<OSMType, Map<Long, OSHEntity>>> {

  //Attention: does not propperly handled missing data at time of Update. 
  //If data is provided with a later update, previous referencing Entities 
  //are not updated and remain in an incomplete state -> see comment about handling missing data
  private static final Logger LOG = LoggerFactory.getLogger(OscOshTransformer.class);
  private final int batchSize;

  private final Iterator<ChangeContainer> containers;
  private final EtlStore etlStore;
  private final PreparedStatement insertKeyStatement;
  private final PreparedStatement insertKeyValueStatement;
  private final PreparedStatement insertRoleStatement;
  private final TagTranslator tt;

  private OscOshTransformer(Path etlFiles, Connection keytables, int batchSize,
      Iterable<ChangeContainer> changes)
      throws OSHDBKeytablesNotFoundException, SQLException {
    this.containers = changes.iterator();
    this.tt = new TagTranslator(keytables);
    this.etlStore = new EtlFileStore(etlFiles);
    this.batchSize = batchSize;
    this.insertKeyStatement
        = keytables.prepareStatement("INSERT INTO " + TableNames.E_KEY + " VALUES(?,?);");
    this.insertKeyValueStatement
        = keytables.prepareStatement("INSERT INTO " + TableNames.E_KEYVALUE + " VALUES(?,?,?);");
    this.insertRoleStatement
        = keytables.prepareStatement("INSERT INTO " + TableNames.E_ROLE + " VALUES(?,?);");
  }

  /**
   * Transforms ChangeContainers to OSHDBEntities.
   *
   * @param etlFiles the etlFiles old versions can be read from
   * @param keytables the keytables-DB to be queried and updated
   * @param batchSize the number of ChangeContainers to be processed at once (large numbers may help
   *     if you expect entities to be changed multiple times during a short timeframe, wich seams
   *     unlikely)
   * @param changes the ChangeContainers to be processed
   * @return the Class itself as an Iterable with OSHDBEntities
   */
  public static Iterable<Map<OSMType, Map<Long, OSHEntity>>> transform(
      Path etlFiles,
      Connection keytables,
      int batchSize,
      Iterable<ChangeContainer> changes) {
    LOG.info("processing");
    //define Iterable that creates Iterators as needed
    return () -> {
      try {
        return new OscOshTransformer(etlFiles, keytables, batchSize, changes);
      } catch (OSHDBKeytablesNotFoundException | SQLException ex) {
        LOG.error("", ex);
      }
      return null;
    };
  }

  private static OSMType convertType(EntityType type) {
    switch (type) {
      case Bound:
        return null;
      case Node:
        return OSMType.NODE;
      case Way:
        return OSMType.WAY;
      case Relation:
        return OSMType.RELATION;
      default:
        throw new AssertionError(type.name());
    }
  }

  @Override
  public boolean hasNext() {
    return this.containers.hasNext();
  }

  @Override
  public Map<OSMType, Map<Long, OSHEntity>> next() {

    Map<EntityType, Map<Long, List<ChangeContainer>>> changes = new HashMap<>(3);

    //it should be checked, whether is actually improves things!
    for (int i = 0; this.containers.hasNext() && i < this.batchSize; i++) {
      ChangeContainer currContainer = this.containers.next();
      Entity entity = currContainer.getEntityContainer().getEntity();

      Map<Long, List<ChangeContainer>> typeEntities
          = changes.getOrDefault(entity.getType(), new HashMap<>(this.batchSize));

      List<ChangeContainer> entityChanges
          = typeEntities.getOrDefault(entity.getId(), new ArrayList<>());

      entityChanges.add(currContainer);
      typeEntities.put(entity.getId(), entityChanges);
      changes.put(entity.getType(), typeEntities);
    }

    Map<OSMType, Map<Long, OSHEntity>> result = new HashMap<>(3);
    for (Entry<EntityType, Map<Long, List<ChangeContainer>>> typeEntry : changes.entrySet()) {
      for (Entry<Long, List<ChangeContainer>> entityEntry : typeEntry.getValue().entrySet()) {
        try {
          Map<OSMType, Map<Long, OSHEntity>> updateEntityAndAffiliates = this.updateEntity(typeEntry
              .getKey(), entityEntry.getKey(), entityEntry.getValue());
          for (Entry<OSMType, Map<Long, OSHEntity>> resultEntry : updateEntityAndAffiliates
              .entrySet()) {
            result.compute(resultEntry.getKey(), (k, v) -> {
              if (v == null) {
                return resultEntry.getValue();
              } else {
                v.putAll(resultEntry.getValue());
                return v;
              }
            });
          }
        } catch (SQLException | IOException ex) {
          LOG.error(
              "Could not update Entity of Type: " + typeEntry.getKey() + " with ID: " + entityEntry
              .getKey(), ex);
        }
      }
    }
    return result;
  }

  private OSHEntity combineNode(
      long id,
      List<ChangeContainer> changes,
      OSHEntity ent2)
      throws IOException {

    ArrayList<OSMNode> nodes = new ArrayList<>();

    for (ChangeContainer cont : changes) {

      Entity entity = cont.getEntityContainer().getEntity();

      //get basic information on object
      int version = entity.getVersion();
      if (cont.getAction() == ChangeAction.Delete) {
        version *= -1;
      }
      OSHDBTimestamp timestamp = new OSHDBTimestamp(entity.getTimestamp());
      long changeset = entity.getChangesetId();
      int userId = entity.getUser().getId();
      int[] tagsArray;
      try {
        tagsArray = this.getTags(entity.getTags());
      } catch (SQLException e) {
        //empty array better?
        tagsArray = null;
        LOG.error("Could not get Tags for this Object.", e);
      }

      //get object specific information
      long latitude = (long) (OSHDB.GEOM_PRECISION_TO_LONG * ((Node) entity).getLatitude());
      long longitude = (long) (OSHDB.GEOM_PRECISION_TO_LONG * ((Node) entity).getLongitude());
      nodes.add(
          new OSMNode(id, version, timestamp, changeset, userId, tagsArray, longitude, latitude)
      );
    }
    //get other versions (if any)
    if (ent2 != null) {
      ent2.getVersions().forEach(node -> nodes.add((OSMNode) node));
    }
    //create object
    OSHNode theNode = OSHNodeImpl.build(nodes);
    //append object
    this.etlStore.appendEntity(theNode, new HashSet<>(), new HashSet<>());

    return theNode;
  }

  private OSHEntity combineRelation(
      long id,
      List<ChangeContainer> changes,
      OSHEntity ent2)
      throws IOException, SQLException {

    ArrayList<OSMRelation> relations = new ArrayList<>();
    Set<Long> missingNodeIds = new HashSet<>();
    Set<Long> missingWayIds = new HashSet<>();
    Set<OSHNode> relationNodes = new HashSet<>();
    Set<OSHWay> relationWays = new HashSet<>();

    for (ChangeContainer cont : changes) {

      Entity entity = cont.getEntityContainer().getEntity();

      //get basic information on object
      int version = entity.getVersion();
      if (cont.getAction() == ChangeAction.Delete) {
        version *= -1;
      }
      OSHDBTimestamp timestamp = new OSHDBTimestamp(entity.getTimestamp());
      long changeset = entity.getChangesetId();
      int userId = entity.getUser().getId();
      int[] tagsArray;
      try {
        tagsArray = this.getTags(entity.getTags());
      } catch (SQLException e) {
        tagsArray = null;
        LOG.error("Could not get Tags for this Object.", e);
      }

      Relation relation = (Relation) entity;
      Iterator<RelationMember> it = relation.getMembers().iterator();
      OSMMember[] refs2 = new OSMMember[relation.getMembers().size()];
      int j = 0;
      while (it.hasNext()) {
        RelationMember rm = it.next();
        int roleId = this.getRole(rm.getMemberRole());
        switch (rm.getMemberType()) {
          case Node:
            OSHNode relationNode = (OSHNode) this.etlStore.getEntity(
                OSMType.NODE, rm.getMemberId());
            if (relationNode != null) {
              missingNodeIds.add(relationNode.getId());
              relationNodes.add(relationNode);
            } else {
              LOG.warn(
                  "Missing Data for "
                  + rm.getMemberType()
                  + " with ID "
                  + rm.getMemberId()
                  + ". Data output might be corrupt?");
            }
            OSMMember memberN = new OSMMember(
                rm.getMemberId(),
                OSMType.NODE,
                roleId,
                relationNode
            );
            refs2[j] = memberN;
            break;
          case Way:
            OSHWay relationWay = (OSHWay) this.etlStore.getEntity(OSMType.WAY, rm.getMemberId());
            if (relationWay != null) {
              missingWayIds.add(relationWay.getId());
              relationWays.add(relationWay);
            } else {
              LOG.warn(
                  "Missing Data for "
                  + rm.getMemberType()
                  + " with ID "
                  + rm.getMemberId()
                  + ". Data output might be corrupt?");
            }
            OSMMember memberW = new OSMMember(
                rm.getMemberId(),
                OSMType.WAY,
                roleId,
                relationWay
            );
            refs2[j] = memberW;
            break;
          case Relation:
            OSHRelation relationRelation
                = (OSHRelation) this.etlStore.getEntity(OSMType.RELATION, rm.getMemberId());
            if (relationRelation == null) {
              LOG.warn(
                  "Missing Data for "
                  + rm.getMemberType()
                  + " with ID "
                  + rm.getMemberId()
                  + ". Data output might be corrupt?");
            }
            OSMMember memberR = new OSMMember(
                rm.getMemberId(),
                OSMType.RELATION,
                roleId,
                relationRelation
            );
            refs2[j] = memberR;
            break;
          default:
            throw new AssertionError(rm.getMemberType().name());
        }
        j++;
      }
      relations.add(new OSMRelation(id, version, timestamp, changeset, userId, tagsArray, refs2));
    }
    if (ent2 != null) {
      ent2.getNodes().forEach(node -> missingNodeIds.remove(node.getId()));
      ent2.getWays().forEach(way -> missingWayIds.remove(way.getId()));
      ent2.getVersions().forEach(relation3 -> {
        relations.add((OSMRelation) relation3);
        OSMMember[] refs1 = ((OSMRelation) relation3).getMembers();
        for (OSMMember mem : refs1) {
          switch (mem.getType()) {
            case NODE:
              relationNodes.add((OSHNode) mem.getEntity());
              break;
            case WAY:
              relationWays.add((OSHWay) mem.getEntity());
              break;
            case RELATION:
              break;
            default:
              throw new AssertionError(mem.getType().name());
          }
        }
      });
    }

    OSHRelation theRelation = OSHRelationImpl.build(relations, relationNodes, relationWays);

    this.etlStore.appendEntity(
        theRelation,
        missingNodeIds,
        missingWayIds);

    return theRelation;
  }

  private OSHEntity combineWay(
      long id,
      List<ChangeContainer> changes,
      OSHEntity ent2)
      throws IOException {

    ArrayList<OSMWay> ways = new ArrayList<>();
    Set<Long> missingNodeIds = new HashSet<>();
    Set<OSHNode> allNodes = new HashSet<>();

    for (ChangeContainer cont : changes) {

      Entity entity = cont.getEntityContainer().getEntity();

      //get basic information on object
      int version = entity.getVersion();
      if (cont.getAction() == ChangeAction.Delete) {
        version *= -1;
      }
      OSHDBTimestamp timestamp = new OSHDBTimestamp(entity.getTimestamp());
      long changeset = entity.getChangesetId();
      int userId = entity.getUser().getId();
      int[] tagsArray;
      try {
        tagsArray = this.getTags(entity.getTags());
      } catch (SQLException e) {
        tagsArray = null;
        LOG.error("Could not get Tags for this Object.", e);
      }

      Way way = (Way) entity;

      List<WayNode> wayNodes = way.getWayNodes();

      OSMMember[] refs = new OSMMember[wayNodes.size()];
      int i = 0;
      //all members in current version
      for (WayNode wn : wayNodes) {
        missingNodeIds.add(wn.getNodeId());
        OSHNode node = (OSHNode) this.etlStore.getEntity(OSMType.NODE, wn.getNodeId());
        //Handling missing data: account for updates coming unordered 
        //(e.g. way creation before referencing node creation. 
        //Maybe dummy node with ID would be better?
        if (node != null) {
          allNodes.add(node);
        } else {
          LOG.warn(
              "Missing Data for Node with ID: "
              + wn.getNodeId()
              + ". Data output might be corrupt?");
        }
        OSMMember member = new OSMMember(wn.getNodeId(), OSMType.NODE, 0, node);
        refs[i] = member;
        i++;
      }

      ways.add(new OSMWay(id, version, timestamp, changeset, userId, tagsArray, refs));
    }
    if (ent2 != null) {
      ent2.getNodes().forEach(node -> missingNodeIds.remove(node.getId()));
      ent2.getVersions().forEach(way3 -> {
        ways.add((OSMWay) way3);
        OSMMember[] refs1 = ((OSMWay) way3).getRefs();
        for (OSMMember mem : refs1) {
          allNodes.add((OSHNode) mem.getEntity());
        }
      });
    }

    OSHWay theWay = OSHWayImpl.build(ways, allNodes);

    this.etlStore.appendEntity(
        theWay,
        missingNodeIds,
        new HashSet<>()
    );

    return theWay;
  }

  private int getRole(String memberRole) throws SQLException {
    int role = this.tt.getOSHDBRoleOf(memberRole).toInt();
    if (role < 0) {
      //update
      try (Statement getMaxKey = this.tt.getConnection().createStatement()) {
        getMaxKey.execute("SELECT MAX(id) from " + TableNames.E_ROLE);
        try (ResultSet resultSet = getMaxKey.getResultSet()) {
          int maxKey;
          if (resultSet.next()) {
            maxKey = resultSet.getInt(1);
          } else {
            maxKey = -1;
          }
          this.insertRoleStatement.setInt(1, maxKey + 1);
          this.insertRoleStatement.setString(2, memberRole);
          this.insertRoleStatement.execute();
          role = maxKey + 1;

          this.tt.updateRole(new OSMRole(memberRole), new OSHDBRole(role));
        }
      }
    }
    return role;
  }

  private int[] getTags(Collection<Tag> tags) throws SQLException {
    int[] tagsArray = new int[tags.size() * 2];
    int i = 0;
    for (Tag tag : tags) {
      OSHDBTag oshdbTag = this.tt.getOSHDBTagOf(tag.getKey(), tag.getValue());
      //insert yet unknown tags (do same with roles and at other occurances
      if (oshdbTag.getKey() < 0) {
        //update
        try (Statement getMaxKey = this.tt.getConnection().createStatement()) {
          getMaxKey.execute("SELECT MAX(id) + 1 from " + TableNames.E_KEY);
          try (ResultSet resultSet = getMaxKey.getResultSet()) {
            int maxKey;
            if (resultSet.next()) {
              maxKey = resultSet.getInt(1);
            } else {
              maxKey = -1;
            }
            this.insertKeyStatement.setInt(1, maxKey);
            this.insertKeyStatement.setString(2, tag.getKey());
            this.insertKeyStatement.execute();

            this.insertKeyValueStatement.setInt(1, maxKey);
            this.insertKeyValueStatement.setInt(2, 0);
            this.insertKeyValueStatement.setString(3, tag.getValue());
            this.insertKeyValueStatement.execute();

            oshdbTag = new OSHDBTag(maxKey, 0);

            this.tt.updateTag(new OSMTag(tag.getKey(), tag.getValue()), oshdbTag);
          }
        }
      } else if (oshdbTag.getValue() < 0) {

        try (Statement getMaxKey = this.tt.getConnection().createStatement()) {
          getMaxKey.execute(
              "SELECT MAX(valueid) + 1 FROM "
              + TableNames.E_KEYVALUE
              + " WHERE keyid = "
              + oshdbTag.getKey()
          );
          try (ResultSet resultSet = getMaxKey.getResultSet()) {
            int maxValue;
            if (resultSet.next()) {
              maxValue = resultSet.getInt(1);
            } else {
              maxValue = -1;
            }
            this.insertKeyValueStatement.setInt(1, oshdbTag.getKey());
            this.insertKeyValueStatement.setInt(2, maxValue);
            this.insertKeyValueStatement.setString(3, tag.getValue());
            this.insertKeyValueStatement.execute();

            oshdbTag = new OSHDBTag(oshdbTag.getKey(), maxValue);

            this.tt.updateTag(new OSMTag(tag.getKey(), tag.getValue()), oshdbTag);
          }
        }
      }
      tagsArray[i] = oshdbTag.getKey();
      i++;
      tagsArray[i] = oshdbTag.getValue();
      i++;
    }
    return tagsArray;
  }

  private Map<OSMType, Map<Long, OSHEntity>> updateDependent(OSHEntity currEntity) throws
      IOException {

    //get dependentEntities
    Map<OSMType, Map<Long, OSHEntity>> dependent = this.etlStore.getDependent(
        currEntity.getType(),
        currEntity.getId()
    );

    Map<OSMType, Map<Long, OSHEntity>> result = new HashMap<>();
    //iterate dependend Entities
    for (Entry<OSMType, Map<Long, OSHEntity>> entry : dependent.entrySet()) {
      for (Entry<Long, OSHEntity> entityEntry : entry.getValue().entrySet()) {

        //change to relation does not update dependent relations (might change in later Update)
        if (currEntity.getType() != OSMType.RELATION) {

          //dependenEntity==Way||Relation
          //currEntity==Node||Way
          OSHEntity dependEntity = entityEntry.getValue();
          List<OSHNode> nodes = dependEntity.getNodes();
          List<OSHWay> ways = dependEntity.getWays();

          //replace with new current Entity
          switch (currEntity.getType()) {
            case NODE:
              nodes.replaceAll(node ->
                  node.getId() == currEntity.getId() ? (OSHNode) currEntity : node);
              break;

            case WAY:
              ways
                  .replaceAll(way -> way.getId() == currEntity.getId() ? (OSHWay) currEntity : way);
              break;
            default:
              throw new AssertionError(currEntity.getType().name());
          }

          //recreate dependency
          OSHEntity resultEnt;
          switch (dependEntity.getType()) {
            case WAY:
              resultEnt = OSHWayImpl.build(
                  Lists.newArrayList(((OSHWay) dependEntity).getVersions()),
                  nodes);
              break;
            case RELATION:
              resultEnt = OSHRelationImpl.build(
                  Lists.newArrayList(((OSHRelation) dependEntity).getVersions()),
                  nodes,
                  ways);
              break;
            default:
              throw new AssertionError(dependEntity.getType().name());
          }

          //update dependencies of dependency...
          result.putAll(this.updateDependent(resultEnt));

          //put current updated Dependency
          Map<Long, OSHEntity> dependendDefaultMap = result
              .getOrDefault(dependEntity.getType(), new TreeMap<>());
          dependendDefaultMap.put(dependEntity.getId(), dependEntity);
          result.put(dependEntity.getType(), dependendDefaultMap);
        }
      }
    }
    return result;
  }

  //Combines an entity and an OSHEntity
  private Map<OSMType, Map<Long, OSHEntity>> updateEntity(
      EntityType type,
      long id,
      List<ChangeContainer> changes)
      throws IOException, SQLException {
    //get previous version of entity, if any. 
    //This ensures that updates may come in any order and may handle reactivations
    OSHEntity currEntity = this.etlStore.getEntity(OscOshTransformer.convertType(type), id);

    switch (type) {
      case Node:
        currEntity = this.combineNode(id, changes, currEntity);
        break;
      case Way:
        currEntity = this.combineWay(id, changes, currEntity);
        break;
      case Relation:
        currEntity = this.combineRelation(id, changes, currEntity);
        break;
      default:
        throw new AssertionError(type);
    }

    //add updated Entity to result
    Map<OSMType, Map<Long, OSHEntity>> result = new HashMap<>();
    Map<Long, OSHEntity> defaultMap = result.getOrDefault(currEntity.getType(), new TreeMap<>());
    defaultMap.put(currEntity.getId(), currEntity);
    result.put(currEntity.getType(), defaultMap);

    result.putAll(this.updateDependent(currEntity));

    return result;
  }

}
