package org.heigit.bigspatialdata.oshdb.api.objects;

import com.vividsolutions.jts.geom.Geometry;
import java.util.EnumSet;
import java.util.Objects;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.ContributionType;

public class OSMContribution {
  private final OSHDBTimestamp _tstamp;
  private final OSHDBTimestamp _validTo;
  private final Geometry _geometryBefore;
  private final Geometry _geometryAfter;
  private final OSMEntity _entityBefore;
  private final OSMEntity _entityAfter;
  private final EnumSet<ContributionType> _contributionTypes;
  
  public OSMContribution(OSHDBTimestamp tstamp, OSHDBTimestamp validTo, Geometry geometryBefore, Geometry geometryAfter, OSMEntity entityBefore, OSMEntity entityAfter, EnumSet<ContributionType> contributionTypes) {
    this._tstamp = tstamp;
    this._validTo = validTo;
    this._geometryBefore = geometryBefore;
    this._geometryAfter = geometryAfter;
    this._entityBefore = entityBefore;
    this._entityAfter = entityAfter;
    this._contributionTypes = contributionTypes;
  }
  
  public OSHDBTimestamp getTimestamp() {
    return this._tstamp;
  }
  
  public OSHDBTimestamp getValidTo() {
    return this._validTo;
  }
  
  public Geometry getGeometryBefore() {
    return this._geometryBefore;
  }
  
  public Geometry getGeometryAfter() {
    return this._geometryAfter;
  }
  
  public OSMEntity getEntityBefore() {
    return this._entityBefore;
  }
  
  public OSMEntity getEntityAfter() {
    return this._entityAfter;
  }
  
  public EnumSet<ContributionType> getContributionTypes() {
    return this._contributionTypes;
  }


  /**
   * Returns the user id of the osm contributor responsible for this data modification.
   * This user id can be different from what one gets by calling `.getEntityAfter().getUserId()` if the contribution
   * is a pure geometry change (i.e. the entity itself has not ben modified, but one or more of its child entities).
   *
   * @return returns the user id of the contributor who made this modification
   */
  public int getContributorUserId() {
    // todo: optimizable if done directly in CellIterator??
    OSMEntity entity = this.getEntityAfter();
    long contributionTimestamp = this.getTimestamp().toLong();
    int userId = entity.getUserId();
    if (!(entity instanceof OSMNode)) {
      // if the entity is a way or relation, and in this contribution *only* the geometry has been changed, we can't out
      // the respective user only by looking at the entity alone – instead, we need to iterate over all the element's
      // children to find the corresponding contributor's user id.
      if (this.getContributionTypes().size() == 1 && this.getContributionTypes().contains(ContributionType.GEOMETRY_CHANGE)) {
        if (entity.getTimestamp() != contributionTimestamp) // search children for actual contributor's userId
          if (entity instanceof OSMWay) {
            userId = ((OSMWay)entity).getRefEntities(contributionTimestamp)
                .filter(Objects::nonNull)
                .filter(n -> n.getTimestamp() == contributionTimestamp)
                .findFirst()
                .map(OSMEntity::getUserId)
                .orElse(-1); // "rare" race condition, caused by not properly ordered timestaps (t_x > t_{x+1}) // todo: what to do here??
          } else if (entity instanceof OSMRelation) {
            userId = ((OSMRelation) entity).getMemberEntities(contributionTimestamp)
                .filter(Objects::nonNull)
                .filter(_e -> _e.getTimestamp() == contributionTimestamp)
                .findFirst()
                .map(OSMEntity::getUserId)
                .orElseGet(() ->
                    ((OSMRelation) entity).getMemberEntities(contributionTimestamp)
                        .filter(Objects::nonNull)
                        .filter(_e -> _e instanceof OSMWay)
                        .map(_e -> (OSMWay) _e)
                        .flatMap(_w -> _w.getRefEntities(contributionTimestamp))
                        .filter(n -> n.getTimestamp() == contributionTimestamp)
                        .findFirst()
                        .map(OSMEntity::getUserId)
                        .orElse(-1) // possible "rare" race condition, caused by not properly ordered timestaps (t_x > t_{x+1}) // todo: what to do here??
                );
          }
      }
    }
    return userId;
  }
}
