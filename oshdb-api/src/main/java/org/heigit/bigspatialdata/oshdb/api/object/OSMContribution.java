package org.heigit.bigspatialdata.oshdb.api.object;

import com.vividsolutions.jts.geom.Geometry;
import java.util.EnumSet;
import java.util.Objects;

import javax.annotation.Nonnull;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.celliterator.ContributionType;
import org.heigit.bigspatialdata.oshdb.util.celliterator.LazyEvaluatedContributionTypes;
import org.heigit.bigspatialdata.oshdb.util.celliterator.LazyEvaluatedObject;

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
public class OSMContribution implements OSHDBMapReducible {
  private final OSHDBTimestamp _tstamp;
  private final LazyEvaluatedObject<Geometry> _geometryBefore;
  private final LazyEvaluatedObject<Geometry> _geometryAfter;
  private final OSMEntity _entityBefore;
  @Nonnull
  private final OSMEntity _entityAfter;
  private final LazyEvaluatedContributionTypes _contributionTypes;
  
  public OSMContribution(OSHDBTimestamp tstamp,
      LazyEvaluatedObject<Geometry> geometryBefore, LazyEvaluatedObject<Geometry> geometryAfter,
      OSMEntity entityBefore, @Nonnull OSMEntity entityAfter,
      LazyEvaluatedContributionTypes contributionTypes
  ) {
    this._tstamp = tstamp;
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

  /**
   * Returns the geometry of the entity before this modification.
   * Is `null` if this is a entity creation.
   *
   * @return a JTS Geometry object representing the entity's state before the modification
   */
  public Geometry getGeometryBefore() {
    return this._geometryBefore.get();
  }

  /**
   * Returns the geometry of the entity after this modification.
   * Is `null` if this is a entity deletion.
   *
   * @return a JTS Geometry object representing the entity's state after the modification
   */
  public Geometry getGeometryAfter() {
    return this._geometryAfter.get();
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
   * Checks if this contribution is of the given contribution type. It can be one or more of:
   * <ul>
   *   <li>CREATION</li>
   *   <li>DELETION</li>
   *   <li>TAG_CHANGE</li>
   *   <li>GEOMETRY_CHANGE</li>
   * </ul>
   *
   * If this is a entity creation or deletion, the other flags are not set (even though one might argue that a just
   * created object clearly has a different geometry than before, for example).
   *
   * @return a set of modification type this contribution made on the underlying data
   */
  public boolean is(ContributionType contributionType) {
    return this._contributionTypes.contains(contributionType);
  }

  /**
   * Determined the type of modification this contribution has made. Can be one or more of:
   * <ul>
   *   <li>CREATION</li>
   *   <li>DELETION</li>
   *   <li>TAG_CHANGE</li>
   *   <li>GEOMETRY_CHANGE</li>
   * </ul>
   *
   * If this is a entity creation or deletion, the other flags are not set (even though one might argue that a just
   * created object clearly has a different geometry than before, for example).
   *
   * @return a set of modification type this contribution made on the underlying data
   */
  public EnumSet<ContributionType> getContributionTypes() {
    return this._contributionTypes.get();
  }


  /**
   * Returns the user id of the osm contributor responsible for this data modification.
   * This user id can be different from what one gets by calling `.getEntityAfter().getUserId()` if the contribution
   * is a pure geometry change (i.e. the entity itself has not ben modified, but one or more of its child entities):
   *
   * If the entity is a way or relation, and in a contribution *"only"* the geometry has been changed, we can't find
   * out the respective contributor's user id only by looking at the entity alone – instead, we need to iterate over
   * all the entity's children to find the actual contributor's user id.
   *
   * @return returns the user id of the contributor who made this modification
   */
  public int getContributorUserId() {
    // todo: optimizable if done directly in CellIterator??
    OSMEntity entity = this.getEntityAfter();
    OSHDBTimestamp contributionTimestamp = this.getTimestamp();
    EnumSet<ContributionType> contributionTypes = this.getContributionTypes();
    // if the entity itself was modified at this exact timestamp, or we know from the contribution type that the entity
    // must also have been modified, we can just return the uid directly
    if (contributionTimestamp.equals(entity.getTimestamp()) ||
        this._entityBefore == null ||
        this._entityBefore.getVersion() != this._entityAfter.getVersion()
    ) {
      return entity.getUserId();
    }
    int userId = -1;
    // search children for actual contributor's userId
    if (entity instanceof OSMWay) {
      userId = ((OSMWay)entity).getRefEntities(contributionTimestamp)
          .filter(Objects::nonNull)
          .filter(n -> n.getTimestamp().equals(contributionTimestamp))
          .findFirst()
          .map(OSMEntity::getUserId)
          .orElse(-1); // "rare" race condition, caused by not properly ordered timestamps (t_x > t_{x+1}) // todo: what to do here??
    } else if (entity instanceof OSMRelation) {
      userId = ((OSMRelation) entity).getMemberEntities(contributionTimestamp)
          .filter(Objects::nonNull)
          .filter(e -> e.getTimestamp().equals(contributionTimestamp))
          .findFirst()
          .map(OSMEntity::getUserId)
          .orElseGet(() ->
              ((OSMRelation) entity).getMemberEntities(contributionTimestamp)
                  .filter(Objects::nonNull)
                  .filter(e -> e instanceof OSMWay) // todo: what to do with relation->node member changes or relation->relation[->*] changes?
                  .map(e -> (OSMWay)e)
                  .flatMap(w -> w.getRefEntities(contributionTimestamp))
                  .filter(Objects::nonNull)
                  .filter(n -> n.getTimestamp().equals(contributionTimestamp))
                  .findFirst()
                  .map(OSMEntity::getUserId)
                  .orElse(-1) // possible "rare" race condition, caused by not properly ordered timestamps (t_x > t_{x+1}) // todo: what to do here??
          );
    }
    return userId;
  }
}
