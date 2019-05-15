package org.heigit.bigspatialdata.updater.util;

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
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.TableNames;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import org.openstreetmap.osmosis.core.container.v0_6.ChangeContainer;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainerFactory;
import org.openstreetmap.osmosis.core.container.v0_6.RelationContainerFactory;
import org.openstreetmap.osmosis.core.container.v0_6.WayContainerFactory;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.RelationMember;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OSCOSHTransformer implements Iterator<Map<OSMType, Map<Long, OSHEntity>>> {

  //Attention: does not propperly handled missing data at time of Update. If data is provided with a later update, previous referencing Entities are not updated and remain in an incomplete state -> see comment about handling missing data
  private static final Logger LOG = LoggerFactory.getLogger(OSCOSHTransformer.class);

  private final Iterator<ChangeContainer> containers;
  private final EtlStore etlStore;
  private final PreparedStatement insertKeyStatement;
  private final PreparedStatement insertKeyValueStatement;
  private final PreparedStatement insertRoleStatement;
  private final TagTranslator tt;

  private OSCOSHTransformer(Path etlFiles, Connection keytables, Iterable<ChangeContainer> changes)
      throws OSHDBKeytablesNotFoundException, SQLException {
    this.containers = changes.iterator();
    this.tt = new TagTranslator(keytables);
    this.etlStore = new EtlFileStore(etlFiles);
    this.insertKeyStatement
        = keytables.prepareStatement("INSERT INTO " + TableNames.E_KEY + " VALUES(?,?);");
    this.insertKeyValueStatement
        = keytables.prepareStatement("INSERT INTO " + TableNames.E_KEYVALUE + " VALUES(?,?,?);");
    this.insertRoleStatement
        = keytables.prepareStatement("INSERT INTO " + TableNames.E_ROLE + " VALUES(?,?);");
  }

  public static OSMType convertType(EntityType type) {
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

  public static Iterable<Map<OSMType, Map<Long, OSHEntity>>> transform(
      Path etlFiles,
      Connection keytables,
      Iterable<ChangeContainer> changes) {
    LOG.info("processing");
    //define Iterable that creates Iterators as needed
    return () -> {
      try {
        return new OSCOSHTransformer(etlFiles, keytables, changes);
      } catch (OSHDBKeytablesNotFoundException | SQLException ex) {
        LOG.error("", ex);
      }
      return null;
    };
  }

  @Override
  public boolean hasNext() {
    return this.containers.hasNext();
  }

  @Override
  public Map<OSMType, Map<Long, OSHEntity>> next() {
    ChangeContainer currContainer = this.containers.next();
    try {
      LOG.trace(currContainer.getAction() + " : " + currContainer.getEntityContainer().getEntity());

      switch (currContainer.getAction()) {
        case Create:
          return this.onChange(currContainer);
        case Modify:
          return this.onChange(currContainer);
        case Delete:
          return this.onDelete(currContainer);
        default:
          throw new AssertionError(currContainer.getAction().name());
      }
    } catch (IOException | SQLException ex) {
      LOG.error("error", ex);
    }
    return null;
  }

  //Combines an entity and an OSHEntity
  private Map<OSMType, Map<Long, OSHEntity>> combine(
      Entity entity,
      Map<OSMType, Map<Long, OSHEntity>> dependen,
      OSHEntity currEntity1)
      throws IOException, SQLException {

    //get basic information on object
    long id = entity.getId();
    int version = entity.getVersion();
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

    OSHEntity currEntity = currEntity1;
    switch (entity.getType()) {
      case Bound:
        throw new UnsupportedOperationException("Unknown type in this context");
      case Node:
        currEntity = this.combineNode(
            id,
            version,
            timestamp,
            changeset,
            userId,
            tagsArray,
            entity,
            currEntity);
        break;
      case Way:
        currEntity = this.combineWay(
            id,
            version,
            timestamp,
            changeset,
            userId,
            tagsArray,
            entity,
            currEntity);
        break;
      case Relation:
        currEntity = this.combineRelation(
            id,
            version,
            timestamp,
            changeset,
            userId,
            tagsArray,
            entity,
            currEntity
        );
        break;
      default:
        throw new AssertionError(entity.getType().name());
    }

    //add updated Entity to result
    Map<OSMType, Map<Long, OSHEntity>> result = new HashMap<>();
    Map<Long, OSHEntity> defaultMap = result.getOrDefault(currEntity.getType(), new TreeMap<>());
    defaultMap.put(currEntity.getId(), currEntity);
    result.put(currEntity.getType(), defaultMap);

    //get updated entities of dependend objects and add to result
    for (Entry<OSMType, Map<Long, OSHEntity>> entry : dependen.entrySet()) {
      for (Entry<Long, OSHEntity> entityEntry : entry.getValue().entrySet()) {
        //change to relation does not update dependent relations
        if (entry.getKey() != OSMType.RELATION) {
          OSHEntity updateDependent
              = this.updateDependent(entityEntry.getValue(), currEntity);

          Map<Long, OSHEntity> dependendDefaultMap = result
              .getOrDefault(updateDependent.getType(), new TreeMap<>());
          dependendDefaultMap.put(updateDependent.getId(), updateDependent);
          result.put(updateDependent.getType(), dependendDefaultMap);

          //TODO: Dependencies of Dependencies need to be updated
          //also and relations might be included in a later version
        }
      }
    }
    return result;
  }

  private OSHEntity combineNode(
      long id,
      int version,
      OSHDBTimestamp timestamp,
      long changeset,
      int userId,
      int[] tagsArray,
      Entity entity,
      OSHEntity ent2)
      throws IOException {

    //get object specific information
    long latitude = (long) (OSHDB.GEOM_PRECISION_TO_LONG * ((Node) entity).getLatitude());
    long longitude = (long) (OSHDB.GEOM_PRECISION_TO_LONG * ((Node) entity).getLongitude());
    ArrayList<OSMNode> nodes = new ArrayList<>(1);
    nodes.add(
        new OSMNode(id, version, timestamp, changeset, userId, tagsArray, longitude, latitude)
    );
    //get other versions (if any)
    if (ent2 != null) {
      ent2.getVersions().forEach(node -> nodes.add((OSMNode) node));
      nodes.sort((OSMNode node1, OSMNode node2) -> {
        return node1.compareTo(node2);
      });
    }
    //create object
    OSHNode theNode = OSHNodeImpl.build(nodes);
    //append object
    this.etlStore.appendEntity(theNode, new HashSet<>(), new HashSet<>());

    return theNode;
  }

  private OSHEntity combineRelation(
      long id,
      int version,
      OSHDBTimestamp timestamp,
      long changeset,
      int userId,
      int[] tagsArray,
      Entity entity,
      OSHEntity ent2)
      throws IOException, SQLException {

    Relation relation = (Relation) entity;
    Set<Long> missingNodeIds = new HashSet<>(1);
    Set<Long> missingWayIds = new HashSet<>(1);
    Iterator<RelationMember> it = relation.getMembers().iterator();
    Set<OSHNode> rNodes = new HashSet<>(0);
    Set<OSHWay> rWays = new HashSet<>(0);
    OSMMember[] refs2 = new OSMMember[relation.getMembers().size()];
    int j = 0;
    while (it.hasNext()) {
      RelationMember rm = it.next();
      int roleId = this.getRole(rm.getMemberRole());
      switch (rm.getMemberType()) {
        case Node:
          OSHNode rNode = (OSHNode) this.etlStore.getEntity(OSMType.NODE, rm.getMemberId());
          if (rNode != null) {
            missingNodeIds.add(rNode.getId());
            rNodes.add(rNode);
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
              rNode
          );
          refs2[j] = memberN;
          break;
        case Way:
          OSHWay rWay = (OSHWay) this.etlStore.getEntity(OSMType.WAY, rm.getMemberId());
          if (rWay != null) {
            missingWayIds.add(rWay.getId());
            rWays.add(rWay);
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
              rWay
          );
          refs2[j] = memberW;
          break;
        case Relation:
          OSHRelation rRelation
              = (OSHRelation) this.etlStore.getEntity(OSMType.RELATION, rm.getMemberId());
          if (rRelation == null) {
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
              rRelation
          );
          refs2[j] = memberR;
          break;
        default:
          throw new AssertionError(rm.getMemberType().name());
      }
      j++;
    }
    ArrayList<OSMRelation> relations = new ArrayList<>(1);
    relations.add(new OSMRelation(id, version, timestamp, changeset, userId, tagsArray, refs2));

    if (ent2 != null) {
      ent2.getNodes().forEach(node -> missingNodeIds.remove(node.getId()));
      ent2.getWays().forEach(way -> missingWayIds.remove(way.getId()));
      ent2.getVersions().forEach(relation3 -> {
        relations.add((OSMRelation) relation3);
        OSMMember[] refs1 = ((OSMRelation) relation3).getMembers();
        for (OSMMember mem : refs1) {
          switch (mem.getType()) {
            case NODE:
              rNodes.add((OSHNode) mem.getEntity());
              break;
            case WAY:
              rWays.add((OSHWay) mem.getEntity());
              break;
            case RELATION:
              break;
            default:
              throw new AssertionError(mem.getType().name());
          }
        }
      });
      relations.sort((OSMRelation realation1, OSMRelation relation2) -> {
        return realation1.compareTo(relation2);
      });
    }

    OSHRelation theRelation = OSHRelationImpl.build(relations, rNodes, rWays);

    this.etlStore.appendEntity(
        theRelation,
        missingNodeIds,
        missingWayIds);

    return theRelation;
  }

  private OSHEntity combineWay(
      long id,
      int version,
      OSHDBTimestamp timestamp,
      long changeset,
      int userId,
      int[] tagsArray,
      Entity entity,
      OSHEntity ent2)
      throws IOException {

    Way way = (Way) entity;
    Set<Long> missingNodeIds = new HashSet<>(1);
    List<WayNode> wayNodes = way.getWayNodes();
    Set<OSHNode> allNodes = new HashSet<>(0);
    OSMMember[] refs = new OSMMember[wayNodes.size()];
    int i = 0;
    //all members in current version
    for (WayNode wn : wayNodes) {
      missingNodeIds.add(wn.getNodeId());
      OSHNode node = (OSHNode) this.etlStore.getEntity(OSMType.NODE, wn.getNodeId());
      //Handling missing data: account for updates coming unordered (e.g. way creation before referencing node creation. Maybe dummy node with ID would be better?
      if (node != null) {
        allNodes.add(node);
      } else {
        LOG.warn(
            "Missing Data for Node with ID: " + wn.getNodeId() + ". Data output might be corrupt?");
      }
      OSMMember member = new OSMMember(wn.getNodeId(), OSMType.NODE, 0, node);
      refs[i] = member;
      i++;
    }
    ArrayList<OSMWay> ways = new ArrayList<>(1);
    ways.add(new OSMWay(id, version, timestamp, changeset, userId, tagsArray, refs));

    if (ent2 != null) {
      ent2.getNodes().forEach(node -> missingNodeIds.remove(node.getId()));
      ent2.getVersions().forEach(way3 -> {
        ways.add((OSMWay) way3);
        OSMMember[] refs1 = ((OSMWay) way3).getRefs();
        for (OSMMember mem : refs1) {
          allNodes.add((OSHNode) mem.getEntity());
        }
      });
      ways.sort((OSMWay way1, OSMWay way2) -> way1.compareTo(way2));
    }

    OSHWay theWay = OSHWayImpl.build(ways, allNodes);

    this.etlStore.appendEntity(
        theWay,
        missingNodeIds,
        new HashSet<>(0)
    );

    return theWay;
  }

  private int getRole(String memberRole) throws SQLException {
    int role = this.tt.getOSHDBRoleOf(memberRole).toInt();
    if (role < 0) {
      //update
      Statement getMaxKey = this.tt.getConnection().createStatement();
      getMaxKey.execute("SELECT MAX(id) from " + TableNames.E_ROLE);
      ResultSet resultSet = getMaxKey.getResultSet();
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
    }
    //tt also needs to be updated if keytables got updated (or insert exception neets to be caught)
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
        Statement getMaxKey = this.tt.getConnection().createStatement();
        getMaxKey.execute("SELECT MAX(id) from " + TableNames.E_KEY);
        ResultSet resultSet = getMaxKey.getResultSet();
        int maxKey;
        if (resultSet.next()) {
          maxKey = resultSet.getInt(1);
        } else {
          maxKey = -1;
        }
        this.insertKeyStatement.setInt(1, maxKey + 1);
        this.insertKeyStatement.setString(2, tag.getKey());
        this.insertKeyStatement.execute();

        this.insertKeyValueStatement.setInt(1, maxKey + 1);
        this.insertKeyValueStatement.setInt(2, 0);
        this.insertKeyValueStatement.setString(3, tag.getValue());
        this.insertKeyValueStatement.execute();

        oshdbTag = new OSHDBTag(maxKey + 1, 0);
      }

      if (oshdbTag.getValue() < 0) {
        Statement getMaxKey = this.tt.getConnection().createStatement();
        getMaxKey.execute(
            "SELECT MAX(valueid) FROM "
            + TableNames.E_KEYVALUE
            + " WHERE keyid = "
            + oshdbTag.getKey()
        );
        ResultSet resultSet = getMaxKey.getResultSet();
        int maxValue;
        if (resultSet.next()) {
          maxValue = resultSet.getInt(1);
        } else {
          maxValue = -1;
        }
        this.insertKeyValueStatement.setInt(1, oshdbTag.getKey());
        this.insertKeyValueStatement.setInt(2, maxValue + 1);
        this.insertKeyValueStatement.setString(3, tag.getValue());
        this.insertKeyValueStatement.execute();

        oshdbTag = new OSHDBTag(oshdbTag.getKey(), maxValue);
      }
      tagsArray[i] = oshdbTag.getKey();
      i++;
      tagsArray[i] = oshdbTag.getValue();
      i++;
    }
    //tt also needs to be updated if keytables got updated (or insert exception neets to be caught)
    return tagsArray;
  }

  //reads previeous versions of OSHEntity and combines them with this  change-entity to a new OSHEntity
  private Map<OSMType, Map<Long, OSHEntity>> onChange(ChangeContainer change) throws IOException,
      SQLException {
    Entity entity = change.getEntityContainer().getEntity();
    //get previous version of entity, if any. This ensures that updates may come in any order and may handle reactivations
    OSHEntity currEnt = this.etlStore.getEntity(
        OSCOSHTransformer.convertType(entity.getType()),
        entity.getId()
    );

    Map<OSMType, Map<Long, OSHEntity>> dependent = this.etlStore.getDependent(
        OSCOSHTransformer.convertType(entity.getType()),
        entity.getId()
    );
    return this.combine(entity, dependent, currEnt);
  }

  //creates a new change-entity with a deleted object (version nur *-1)
  private Map<OSMType, Map<Long, OSHEntity>> onDelete(ChangeContainer change) throws IOException,
      SQLException {
    Entity newEnt = change.getEntityContainer().getEntity();
    newEnt.setVersion(-1 * newEnt.getVersion());
    EntityContainer newCont;
    switch (newEnt.getType()) {
      case Node:
        NodeContainerFactory ncf = new NodeContainerFactory();
        newCont = ncf.createContainer((Node) newEnt);
        break;
      case Way:
        WayContainerFactory wcf = new WayContainerFactory();
        newCont = wcf.createContainer((Way) newEnt);
        break;
      case Relation:
        RelationContainerFactory rcf = new RelationContainerFactory();
        newCont = rcf.createContainer((Relation) newEnt);
        break;
      default:
        throw new AssertionError(newEnt.getType().name());
    }
    ChangeContainer cc = new ChangeContainer(newCont, change.getAction());
    return this.onChange(cc);
  }

  private OSHEntity updateDependent(
      OSHEntity dependEntity,
      OSHEntity currEntity) throws IOException {
    //dependenEntity==Way||Relation
    //currEntity==Node||Way
    List<OSHNode> nodes = dependEntity.getNodes();
    List<OSHWay> ways = dependEntity.getWays();

    switch (currEntity.getType()) {
      case NODE:
        nodes.replaceAll(node -> node.getId() == currEntity.getId() ? (OSHNode) currEntity : node);
        break;

      case WAY:
        ways.replaceAll(way -> way.getId() == currEntity.getId() ? (OSHWay) currEntity : way);
        break;
      default:
        throw new AssertionError(currEntity.getType().name());
    }

    switch (dependEntity.getType()) {
      case WAY:
        return OSHWayImpl.build(
            Lists.newArrayList(((OSHWay) dependEntity).getVersions()),
            nodes);

      case RELATION:
        return OSHRelationImpl.build(
            Lists.newArrayList(((OSHRelation) dependEntity).getVersions()),
            nodes,
            ways);
      default:
        throw new AssertionError(dependEntity.getType().name());
    }
  }

}
