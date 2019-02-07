package org.heigit.bigspatialdata.updater.util;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
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

public class OSCOSHTransformer implements Iterator<OSHEntity> {
  //Attention: does not propperly handled missing data at time of Update. If data is provided with a later update, previous referencing Entities are not updated and remain in an incomplete state -> see comment about handling missing data
  private static final Logger LOG = LoggerFactory.getLogger(OSCOSHTransformer.class);

  public static Iterable<OSHEntity> transform(Path etlFiles, Connection keytables, Iterable<ChangeContainer> changes) {
    LOG.info("processing");
    return new Iterable<OSHEntity>() {
      @Override
      public Iterator<OSHEntity> iterator() {
        return new OSCOSHTransformer(etlFiles, keytables, changes);
      }
    };
  }
  private final Path etlFiles;

  private OSHEntity onChange(ChangeContainer change) throws IOException, OSHDBKeytablesNotFoundException, EntityNotFoudException {
    Entity entity = change.getEntityContainer().getEntity();
    //get previous version of entity, if any. This ensures that updates may come in any order and may handle reactivations
    OSHEntity currEnt = (OSHEntity) EtlFileHandler.getEntity(this.etlFiles, entity.getType(), entity.getId());
    if (currEnt != null) {
      return this.combine(entity, currEnt);
    }
    return this.combine(entity, null);
  }

  private int[] getTags(Collection<Tag> tags) throws OSHDBKeytablesNotFoundException {
    TagTranslator tt = new TagTranslator(this.keytables);
    int[] tagsArray = new int[tags.size() * 2];
    int i = 0;
    for (Tag tag : tags) {
      OSHDBTag oshdbTag = tt.getOSHDBTagOf(tag.getKey(), tag.getValue());
      tagsArray[i] = oshdbTag.getKey();
      i++;
      tagsArray[i] = oshdbTag.getValue();
      i++;
    };
    return tagsArray;
  }

  private OSHEntity onDelete(ChangeContainer change) throws IOException, OSHDBKeytablesNotFoundException, EntityNotFoudException {
    Entity newEnt = change.getEntityContainer().getEntity();
    newEnt.setId(-1 * newEnt.getId());
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

  private final Iterator<ChangeContainer> containers;
  private final Connection keytables;

  private OSCOSHTransformer(Path etlFiles, Connection keytables, Iterable<ChangeContainer> changes) {
    this.containers = changes.iterator();
    this.keytables = keytables;
    this.etlFiles = etlFiles;
  }

  @Override
  public boolean hasNext() {
    return this.containers.hasNext();
  }

  @Override
  public OSHEntity next() {
    ChangeContainer currContainer = this.containers.next();
    try {
      LOG.trace(currContainer.getAction() + ":" + currContainer.getEntityContainer().getEntity());

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
    } catch (IOException ex) {
      LOG.error("error", ex);
    } catch (OSHDBKeytablesNotFoundException ex) {
      LOG.error("error", ex);
    } catch (EntityNotFoudException ex) {
      LOG.error("error", ex);
    }
    return null;
  }

  private OSHEntity combine(Entity entity, OSHEntity ent2) throws OSHDBKeytablesNotFoundException, EntityNotFoudException, IOException {
    //get basic information on object
    TagTranslator tt = new TagTranslator(this.keytables);
    long id = entity.getId();
    int version = entity.getVersion();
    OSHDBTimestamp timestamp = new OSHDBTimestamp(entity.getTimestamp());
    long changeset = entity.getChangesetId();
    int userId = entity.getUser().getId();
    int[] tagsArray;
    try {
      tagsArray = this.getTags(entity.getTags());
    } catch (Exception e) {
      tagsArray = null;
      LOG.error("error", e);
    }

    switch (entity.getType()) {
      case Bound:
        throw new UnsupportedOperationException("Unknown type in this context");
      case Node:
        //get object specific information
        long latitude = (long) (OSHDB.GEOM_PRECISION_TO_LONG * ((Node) entity).getLatitude());
        long longitude = (long) (OSHDB.GEOM_PRECISION_TO_LONG * ((Node) entity).getLongitude());
        ArrayList<OSMNode> nodes = new ArrayList<>(1);
        nodes.add(new OSMNode(id, version, timestamp, changeset, userId, tagsArray, latitude, longitude));
        //get other versions (if any)
        if (ent2 != null) {
          ent2.forEach(node -> nodes.add((OSMNode) node));
          nodes.sort((OSMNode node1, OSMNode node2) -> {
            return node1.compareTo(node2);
          });
        }
        //create object
        OSHNode theNode = OSHNode.build(nodes);
        //append object
        EtlFileHandler.appendEntity(this.etlFiles, theNode);
        //return object
        return theNode;

      case Way:
        Way way = (Way) entity;
        List<WayNode> wayNodes = way.getWayNodes();
        Set<OSHNode> allNodes = new HashSet<>(0);
        OSMMember[] refs = new OSMMember[wayNodes.size()];
        int i = 0;
        //all members in current version
        for (WayNode wn : wayNodes) {
          OSHNode node = (OSHNode) EtlFileHandler.getEntity(this.etlFiles, EntityType.Node, wn.getNodeId());
          //Handling missing data: account for updates coming unordered (e.g. way creation before referencing node creation. Maybe dummy node with ID would be better?
          if (node != null) {
            allNodes.add(node);
          } else {
            LOG.warn("Missing Data for Node with ID: " + wn.getNodeId() + ". Data output might be corrupt?");
          }
          OSMMember member = new OSMMember(wn.getNodeId(), OSMType.NODE, 0, node);
          refs[i] = member;
          i++;
        }
        ArrayList<OSMWay> ways = new ArrayList<>(1);
        ways.add(new OSMWay(id, version, timestamp, changeset, userId, tagsArray, refs));

        if (ent2 != null) {
          ent2.forEach(way3 -> {
            ways.add((OSMWay) way3);
            OSMMember[] refs1 = ((OSMWay) way3).getRefs();
            for (OSMMember mem : refs1) {
              allNodes.add((OSHNode) mem.getEntity());
            }
          });
          ways.sort((OSMWay way1, OSMWay way2) -> {
            return way1.compareTo(way2);
          });
        }

        OSHWay theWay = OSHWay.build(ways, allNodes);

        EtlFileHandler.appendEntity(this.etlFiles, theWay);

        return theWay;

      case Relation:
        Relation relation = (Relation) entity;
        Iterator<RelationMember> it = relation.getMembers().iterator();
        Set<OSHNode> rNodes = new HashSet<>(0);
        Set<OSHWay> rWays = new HashSet<>(0);
        OSMMember[] refs2 = new OSMMember[relation.getMembers().size()];
        int j = 0;
        while (it.hasNext()) {
          RelationMember rm = it.next();
          switch (rm.getMemberType()) {
            case Node:
              OSHNode rNode = (OSHNode) EtlFileHandler.getEntity(etlFiles, EntityType.Node, rm.getMemberId());
              if (rNode != null) {;
                rNodes.add(rNode);
              } else {
                LOG.warn("Missing Data for " + rm.getMemberType() + " with ID " + rm.getMemberId() + ". Data output might be corrupt?");
              }
              OSMMember memberN = new OSMMember(rm.getMemberId(), OSMType.NODE, tt.getOSHDBRoleOf(rm.getMemberRole()).toInt(), rNode);
              refs2[j] = memberN;
              break;
            case Way:
              OSHWay rWay = (OSHWay) EtlFileHandler.getEntity(etlFiles, EntityType.Way, rm.getMemberId());
              if (rWay != null) {
                rWays.add(rWay);
              } else {
                LOG.warn("Missing Data for " + rm.getMemberType() + " with ID " + rm.getMemberId() + ". Data output might be corrupt?");
              }
              OSMMember memberW = new OSMMember(rm.getMemberId(), OSMType.WAY, tt.getOSHDBRoleOf(rm.getMemberRole()).toInt(), rWay);
              refs2[j] = memberW;
              break;
            case Relation:
              OSHRelation rRelation = (OSHRelation) EtlFileHandler.getEntity(etlFiles, EntityType.Relation, rm.getMemberId());
              if (rRelation == null) {
                LOG.warn("Missing Data for " + rm.getMemberType() + " with ID " + rm.getMemberId() + ". Data output might be corrupt?");
              }
              OSMMember memberR = new OSMMember(rm.getMemberId(), OSMType.RELATION, tt.getOSHDBRoleOf(rm.getMemberRole()).toInt(), rRelation);
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
          ent2.forEach(relation3 -> {
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

        OSHRelation theRelation = OSHRelation.build(relations, rNodes, rWays);

        EtlFileHandler.appendEntity(this.etlFiles, theRelation);

        return theRelation;
      default:
        throw new AssertionError(entity.getType().name());
    }
  }

}
