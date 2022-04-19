package org.heigit.ohsome.oshdb.util.taginterpreter;

import static java.util.Collections.emptySet;
import java.util.Map;
import java.util.Set;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.osh.OSHWay;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;

/**
 * instances of this class are used to determine whether a OSM way represents a polygon or
 * linestring geometry.
 */
class BaseTagInterpreter implements TagInterpreter {
  int areaNoTagKeyId;
  int areaNoTagValueId;
  Map<Integer, Set<Integer>> wayAreaTags;
  Map<Integer, Set<Integer>> relationAreaTags;
  Set<Integer> uninterestingTagKeys;
  int outerRoleId;
  int innerRoleId;
  int emptyRoleId;

  BaseTagInterpreter(
      int areaNoTagKeyId,
      int areaNoTagValueId,
      Map<Integer, Set<Integer>> wayAreaTags,
      Map<Integer, Set<Integer>> relationAreaTags,
      Set<Integer> uninterestingTagKeys,
      int outerRoleId,
      int innerRoleId,
      int emptyRoleId
  ) {
    this.areaNoTagKeyId = areaNoTagKeyId;
    this.areaNoTagValueId = areaNoTagValueId;
    this.wayAreaTags = wayAreaTags;
    this.relationAreaTags = relationAreaTags;
    this.uninterestingTagKeys = uninterestingTagKeys;
    this.outerRoleId = outerRoleId;
    this.innerRoleId = innerRoleId;
    this.emptyRoleId = emptyRoleId;
  }

  private boolean evaluateWayForArea(OSMWay entity) {
    var tags = entity.getTags();
    if (tags.hasTagValue(areaNoTagKeyId, areaNoTagValueId)) {
      return false;
    }
    for (var tag : entity.getTags()) {
      if (wayAreaTags.getOrDefault(tag.getKey(), emptySet())
          .contains(tag.getValue())) {
        return true;
      }
    }
    return false;
  }

  protected boolean evaluateRelationForArea(OSMRelation entity) {
    // skip area=no check, since that doesn't make much sense for multipolygon relations (does it??)
    for (var tag : entity.getTags()) {
      if (relationAreaTags.getOrDefault(tag.getKey(), emptySet())
          .contains(tag.getValue())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isArea(OSMEntity entity) {
    if (entity instanceof OSMNode) {
      return false;
    } else if (entity instanceof OSMWay) {
      OSMWay way = (OSMWay) entity;
      OSMMember[] nds = way.getMembers();
      // must form closed ring with at least 3 vertices
      if (nds.length < 4 || nds[0].getId() != nds[nds.length - 1].getId()) {
        return false;
      }
      return this.evaluateWayForArea((OSMWay) entity);
    } else /*if (entity instanceof OSMRelation)*/ {
      return this.evaluateRelationForArea((OSMRelation) entity);
    }
  }

  @Override
  public boolean isLine(OSMEntity entity) {
    if (entity instanceof OSMNode) {
      return false;
    }
    return !isArea(entity);
  }

  @Override
  public boolean hasInterestingTagKey(OSMEntity osm) {
    for(var tag : osm.getTags()) {
      if (!uninterestingTagKeys.contains(tag.getKey())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isOldStyleMultipolygon(OSMRelation osmRelation) {
    int outerWayCount = 0;
    OSMMember[] members = osmRelation.getMembers();
    for (int i = 0; i < members.length; i++) {
      if (members[i].getType() == OSMType.WAY && members[i].getRawRoleId() == outerRoleId) {
        if (++outerWayCount > 1) {
          // exit early if two outer ways were already found
          return false;
        }
      }
    }
    if (outerWayCount != 1) {
      return false;
    }
    var tags = osmRelation.getTags();
    for (var tag : tags) {
      if (relationAreaTags.getOrDefault(tag.getKey(), emptySet())
          .contains(tag.getValue())) {
        continue;
      }
      if (!uninterestingTagKeys.contains(tag.getKey())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isMultipolygonOuterMember(OSMMember osmMember) {
    if (!(osmMember.getEntity() instanceof OSHWay)) {
      return false;
    }
    int roleId = osmMember.getRawRoleId();
    // some historic osm data may still be mapped without roles set
    // -> assume empty role to mean outer. see
    // https://wiki.openstreetmap.org/w/index.php?title=Relation:multipolygon&oldid=366967#Members
    return roleId == this.outerRoleId
        || roleId == this.emptyRoleId;
    // todo: check if we need a more clever outer/inner detection for empty role case with old data
  }

  @Override
  public boolean isMultipolygonInnerMember(OSMMember osmMember) {
    if (!(osmMember.getEntity() instanceof OSHWay)) {
      return false;
    }
    return osmMember.getRawRoleId() == this.innerRoleId;
  }

}
