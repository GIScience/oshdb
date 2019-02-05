package org.heigit.bigspatialdata.updater.util;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.RelationMember;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OSCOSHTransformer implements Iterator<OSHEntity> {
  private static final Logger LOG = LoggerFactory.getLogger(OSCOSHTransformer.class);

  public static Iterable<OSHEntity> transform(Map<OSMType, File> etlFiles, Connection keytables, Iterable<ChangeContainer> changes) {
    LOG.info("processing");
    return new Iterable<OSHEntity>() {
      @Override
      public Iterator<OSHEntity> iterator() {
        return new OSCOSHTransformer(etlFiles, keytables, changes);
      }
    };
  }
  private final Map<OSMType, File> etlFiles;

  private OSHEntity onCreate(ChangeContainer change) throws IOException, OSHDBKeytablesNotFoundException, EntityNotFoudException {
    TagTranslator tt = new TagTranslator(this.keytables);
    Entity entity = change.getEntityContainer().getEntity();
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

    switch (change.getEntityContainer().getEntity().getType()) {
      case Bound:
        throw new UnsupportedOperationException("Unknown type in this context");
      case Node:
        long latitude = (long) (OSHDB.GEOM_PRECISION_TO_LONG * ((Node) entity).getLatitude());
        long longitude = (long) (OSHDB.GEOM_PRECISION_TO_LONG * ((Node) entity).getLongitude());
        ArrayList<OSMNode> nodes = new ArrayList<>(1);
        nodes.add(new OSMNode(id, version, timestamp, changeset, userId, tagsArray, latitude, longitude));
        return OSHNode.build(nodes);
      case Way:
        Way way = (Way) entity;
        List<WayNode> wayNodes = way.getWayNodes();
        ArrayList<OSHNode> allNodes = new ArrayList<>(0);
        OSMMember[] refs = new OSMMember[wayNodes.size()];
        int i = 0;
        for (WayNode wn : wayNodes) {
          OSHNode node = (OSHNode) EtlFileHandler.getEntity(etlFiles.get(OSMType.NODE), wn.getNodeId());
          allNodes.add(node);
          OSMMember member = new OSMMember(wn.getNodeId(), OSMType.NODE, 0, node);
          refs[i] = member;
          i++;
        }
        ArrayList<OSMWay> ways = new ArrayList<>(1);
        ways.add(new OSMWay(id, version, timestamp, changeset, userId, tagsArray, refs));
        return OSHWay.build(ways, allNodes);
      case Relation:
        Relation relation = (Relation) entity;
        Iterator<RelationMember> it = relation.getMembers().iterator();
        ArrayList<OSHNode> rNodes = new ArrayList<>(0);
        ArrayList<OSHWay> rWays = new ArrayList<>(0);
        OSMMember[] refs2 = new OSMMember[relation.getMembers().size()];
        int j = 0;
        while (it.hasNext()) {
          RelationMember rm = it.next();
          switch (rm.getMemberType()) {
            case Node:
              OSHNode rNode = (OSHNode) EtlFileHandler.getEntity(etlFiles.get(OSMType.NODE), rm.getMemberId());
              rNodes.add(rNode);
              OSMMember memberN = new OSMMember(rm.getMemberId(), OSMType.NODE, tt.getOSHDBRoleOf(rm.getMemberRole()).toInt(), rNode);
              refs2[j] = memberN;
              break;
            case Way:
              OSHWay rWay = (OSHWay) EtlFileHandler.getEntity(etlFiles.get(OSMType.WAY), rm.getMemberId());
              rWays.add(rWay);
              OSMMember memberW = new OSMMember(rm.getMemberId(), OSMType.WAY, tt.getOSHDBRoleOf(rm.getMemberRole()).toInt(), rWay);
              refs2[j] = memberW;
              break;
            case Relation:
              OSHRelation rRelation = (OSHRelation) EtlFileHandler.getEntity(etlFiles.get(OSMType.RELATION), rm.getMemberId());
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
        return OSHRelation.build(relations, rNodes, rWays);
      default:
        throw new AssertionError(change.getEntityContainer().getEntity().getType().name());
    }
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

  private OSHEntity onModify(ChangeContainer change) throws IOException, OSHDBKeytablesNotFoundException, EntityNotFoudException {
    Entity ent = change.getEntityContainer().getEntity();
    OSHEntity ent2;
    try {
      switch (ent.getType()) {
        case Node:
          ent2 = EtlFileHandler.getEntity(this.etlFiles.get(OSMType.NODE), ent.getId());
          return this.conbine(ent, ent2);
        case Way:
          ent2 = EtlFileHandler.getEntity(this.etlFiles.get(OSMType.WAY), ent.getId());
          return this.conbine(ent, ent2);
        case Relation:
          ent2 = EtlFileHandler.getEntity(this.etlFiles.get(OSMType.RELATION), ent.getId());
          return this.conbine(ent, ent2);
        default:
          throw new AssertionError(ent.getType().name());
      }
    } catch (EntityNotFoudException e) {
      LOG.warn("Could not find enty, creating", e);
      return this.onCreate(change);
    }
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
    return this.onModify(change);
  }
  private final Iterator<ChangeContainer> containers;
  private final Connection keytables;

  private OSCOSHTransformer(Map<OSMType, File> etlFiles, Connection keytables, Iterable<ChangeContainer> changes) {
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
          return this.onCreate(currContainer);
        case Modify:
          return this.onModify(currContainer);
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

  private OSHEntity conbine(Entity ent, OSHEntity ent2) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

}
