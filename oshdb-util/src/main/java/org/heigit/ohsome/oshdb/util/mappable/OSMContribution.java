package org.heigit.ohsome.oshdb.util.mappable;

import java.util.EnumSet;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.celliterator.ContributionType;
import org.locationtech.jts.geom.Geometry;

/**
 * A modification ("contribution") of a single OSM object.
 */
public interface OSMContribution extends OSHDBMapReducible, Comparable<OSMContribution> {

  /**
   * Returns the geometry of the entity before this modification clipped to the requested area of
   * interest. May be `null` if this is an entity creation.
   *
   * @return a JTS Geometry object representing the entity's state before the modification (clipped
   *         to the respective area of interest)
   */
  Geometry getGeometryBefore();

  /**
   * Returns the geometry of the entity before this modification. This is the full (unclipped)
   * geometry of the entity. May be `null` if this is an entity creation.
   *
   * @return a JTS Geometry object representing the entity's state before the modification (not
   *         clipped to the respective area of interest)
   */
  Geometry getGeometryUnclippedBefore();

  /**
   * Returns the geometry of the entity after this modification clipped to the requested area of
   * interest. May be `null` if this is an entity deletion.
   *
   * @return a JTS Geometry object representing the entity's state after the modification (clipped
   *         to the respective area of interest)
   */
  Geometry getGeometryAfter();

  /**
   * Returns the geometry of the entity after this modification. This is the full (unclipped)
   * geometry of the entity. May be `null` if this is an entity deletion.
   *
   * @return a JTS Geometry object representing the entity's state after the modification (not
   *         clipped to the respective area of interest)
   */
  Geometry getGeometryUnclippedAfter();

  /**
   * Returns the entity object in its state before this modification.
   * Is `null` if this is a entity creation.
   *
   * @return the entity object as it was before this modification
   */
  OSMEntity getEntityBefore();

  /**
   * Returns the entity object in its state after this modification.
   *
   * <p>
   * If this is a entity deletion, the returned entity will have the visible flag set to false:
   * `entity.getEntityAfter().isVisible == false`
   * </p>
   *
   * @return the entity object as it was after this modification
   */
  OSMEntity getEntityAfter();

  /**
   * The (parent) osh entity of the osm entities involved in this contribution.
   *
   * @return the OSHEntity object of this contribution
   */
  OSHEntity getOSHEntity();

  /**
   * Checks if this contribution is of the given contribution type.
   *
   * <p>
   * It can be one or more of:
   * </p>
   * <ul>
   *   <li>CREATION</li>
   *   <li>DELETION</li>
   *   <li>TAG_CHANGE</li>
   *   <li>GEOMETRY_CHANGE</li>
   * </ul>
   *
   * <p>
   * If this is a entity creation or deletion, the other flags are not set (even though one might
   * argue that a just created object clearly has a different geometry than before, for example).
   * </p>
   *
   * @return a set of modification type this contribution made on the underlying data
   */
  boolean is(ContributionType contributionType);

  /**
   * Determined the type of modification this contribution has made.
   *
   * <p>
   * Can be one or more of:
   * </p>
   * <ul>
   *   <li>CREATION</li>
   *   <li>DELETION</li>
   *   <li>TAG_CHANGE</li>
   *   <li>GEOMETRY_CHANGE</li>
   * </ul>
   *
   * <p>
   * If this is a entity creation or deletion, the other flags are not set (even though one might
   * argue that a just created object clearly has a different geometry than before, for example).
   * </p>
   *
   * @return a set of modification type this contribution made on the underlying data
   */
  EnumSet<ContributionType> getContributionTypes();

  /**
   * Returns the user id of the osm contributor responsible for this data modification.
   *
   * <p>
   * This user id can be different from what one gets by calling `.getEntityAfter().getUserId()`
   * if the contribution is a pure geometry change (i.e. the entity itself has not ben modified,
   * but one or more of its child entities):
   * </p>
   *
   * <p>
   * If the entity is a way or relation, and in a contribution *"only"* the geometry has been
   * changed, we can't find out the respective contributor's user id only by looking at the
   * entity alone – instead, we need to iterate over all the entity's children to find the actual
   * contributor's user id.
   * </p>
   *
   * @return returns the user id of the contributor who made this modification
   */
  int getContributorUserId();

  /**
   * Returns the osm changeset id of the contribution.
   *
   * @return the id of the osm changeset represented by the current contribution object
   */
  long getChangesetId();
}
