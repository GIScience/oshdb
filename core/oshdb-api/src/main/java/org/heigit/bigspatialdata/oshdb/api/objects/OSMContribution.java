package org.heigit.bigspatialdata.oshdb.api.objects;

import com.vividsolutions.jts.geom.Geometry;
import java.util.EnumSet;
import java.util.Objects;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.ContributionType;

/**
 * Holds information about a single modification ("contribution") of a single entity in database.
 *
 * It holds the information about:
 * <ul>
 *   <li>the timestamp at which this change happened</li>
 *   <li>state of the entity before and after the modification</li>
 *   <li>the geometry of the entity before and after the modification</li>
 *   <li>the type(s) of change which has happened here (e.g. creation/deletion of an entity, modification of a geometry, altering of the tag list, etc.)</li>
 * </ul>
 */
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

  /**
   * Returns the timestamp at which this data modification has happened.
   *
   * @return the modification timestamp as a OSHDBTimestamp object
   */
  public OSHDBTimestamp getTimestamp() {
    return this._tstamp;
  }

  @Deprecated
  public OSHDBTimestamp getValidTo() {
    return this._validTo;
  }

  /**
   * Returns the geometry of the entity before this modification.
   * Is `null` if this is a entity creation.
   *
   * @return a JTS Geometry object representing the entity's state before the modification
   */
  public Geometry getGeometryBefore() {
    return this._geometryBefore;
  }

  /**
   * Returns the geometry of the entity after this modification.
   * Is `null` if this is a entity deletion.
   *
   * @return a JTS Geometry object representing the entity's state after the modification
   */
  public Geometry getGeometryAfter() {
    return this._geometryAfter;
  }

  /**
   * Returns the entity object in its state before this modification.
   * Is `null` if this is a entity creation.
   *
   * @return the entity object as it was before this modification
   */
  public OSMEntity getEntityBefore() {
    return this._entityBefore;
  }

  /**
   * Returns the entity object in its state after this modification.
   * If this is a entity deletion, the returned entity will have the visible flag set to false: `entity.getEntityAfter().isVisible == false`
   *
   * @return the entity object as it was after this modification
   */
  public OSMEntity getEntityAfter() {
    return this._entityAfter;
  }

  /**
   * Determined the type of modification this contribution has made. Can be one or more of:
   * <ul>
   *   <li>CREATION</li>
   *   <li>DELETION</li>
   *   <li>TAG_CHANGE</li>
   *   <li>MEMBERLIST_CHANGE</li>
   *   <li>GEOMETRY_CHANGE</li>
   * </ul>
   *
   * If this is a entity creation or deletion, the other flags are not set (even though one might argue that a just
   * created object clearly has a different geometry than before, for example).
   *
   * The MEMBERLIST_CHANGE flag is set if the list of "direct" children of this object (i.e. the nodes of a way or the
   * members of a relation, but not for example the nodes of a way of a relation) has changed or rearranged. The flag is
   * also set if only the role of a member of a relation has been changed.
   *
   * @return a set of modification type this contribution made on the underlying data
   */
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
      if (!(
          this.getContributionTypes().contains(ContributionType.TAG_CHANGE) ||
          this.getContributionTypes().contains(ContributionType.MEMBERLIST_CHANGE) ||
          this.getContributionTypes().contains(ContributionType.CREATION) ||
          this.getContributionTypes().contains(ContributionType.DELETION)
      )) {
        if (entity.getTimestamp() != contributionTimestamp) {
          // search children for actual contributor's userId
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
      //}
    }
    return userId;
  }
}
