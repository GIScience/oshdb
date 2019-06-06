package org.heigit.bigspatialdata.oshdb.api.util.contributionevaluator;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.util.contributionevaluator.object.GeomAnalysis;
import org.heigit.bigspatialdata.oshdb.api.util.contributionevaluator.object.OSHDBGeometryChange;
import org.heigit.bigspatialdata.oshdb.api.util.contributionevaluator.object.OSHDBModifiedTag;
import org.heigit.bigspatialdata.oshdb.api.util.contributionevaluator.object.OSHDBTagChange;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.celliterator.ContributionType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper Calss to to further investigate a the actual changes made in an @link{OSMContribution}.
 * Defines a set of methods commonly used when workin with @link{OSMContribution}s.
 */
public class OSHDBContributionEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(OSHDBContributionEvaluator.class);

  private OSHDBContributionEvaluator() {
  }

  /**
   * Evaluates @link{GEOMETRY_CHANGE}s in detail.
   *
   * @param contrib The contribution to be analysed.
   * @param geomAnalysis The type of geom-analysis
   * @return A set of results depending on the analyses requested
   */
  public static OSHDBGeometryChange evaluateGeometries(OSMContribution contrib,
      GeomAnalysis geomAnalysis) {
    EnumSet<ContributionType> contributionTypes = contrib.getContributionTypes();
    if (contributionTypes.contains(ContributionType.GEOMETRY_CHANGE)) {
      //use OSM Presision in geometries
      PrecisionModel pm = new PrecisionModel(OSHDB.GEOM_PRECISION_TO_LONG);
      //use OSM Coordinate system WGS84
      GeometryFactory gf = new GeometryFactory(pm, 4326);

      Geometry geomAfter;
      Geometry geomBefore;
      if (contributionTypes.contains(ContributionType.CREATION)) {
        geomBefore = gf.toGeometry(null);
        geomAfter = contrib.getGeometryAfter();
      } else if (contributionTypes.contains(ContributionType.DELETION)) {
        geomBefore = contrib.getGeometryBefore();
        geomAfter = gf.toGeometry(null);
      } else {
        geomBefore = contrib.getGeometryBefore();
        geomAfter = contrib.getGeometryAfter();
      }

      //handle type changes
      double a;
      switch (geomAnalysis) {
        case AREA:
          a = geomAfter.getArea() - geomBefore.getArea();
          return new OSHDBGeometryChange(geomAnalysis, a);
        case LENGTH:
          a = geomAfter.getLength() - geomBefore.getLength();
          return new OSHDBGeometryChange(geomAnalysis, a);
        case MOVEMENT:
          //what do we want to calculate here?
          //In OSM movement may be quite common (new satellite data, wrong digitisation)
          //we check if the element is similar by looking deliberatly only on the type and numbeer of vertices and calculate the distance between centroids
          Coordinate[] coordinatesBef = geomBefore.getCoordinates();
          Coordinate[] coordinatesAf = geomAfter.getCoordinates();
          //check if geoms are similar (http://lin-ear-th-inking.blogspot.com/2009/01/computing-geometric-similarity.html)
          if (geomBefore.getGeometryType().equals(geomAfter.getGeometryType())
              && coordinatesBef.length == coordinatesAf.length
              && !geomBefore.isEmpty()
              && !geomAfter.isEmpty()) {
            a = geomBefore.getCentroid().distance(geomAfter.getCentroid());
            return new OSHDBGeometryChange(geomAnalysis, a);
          }
          return new OSHDBGeometryChange(geomAnalysis, null);
        case OBJECTCOUNT:
          break;
        case SQUAREDNESS:
          //https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/apps/MMNepalAnalyses/blob/master/src/main/java/org/heigit/missingmaps/nepalanalyses/analyses/lambdaimplementations/followup/FMapper.java#L170
          break;
        case ROUNDNESS:
          //https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/apps/MMNepalAnalyses/blob/master/src/main/java/org/heigit/missingmaps/nepalanalyses/analyses/lambdaimplementations/followup/FMapper.java#L170
          break;
        default:
          throw new AssertionError(geomAnalysis.name());

      }
      return new OSHDBGeometryChange(geomAnalysis);
    }

    return new OSHDBGeometryChange(geomAnalysis);

  }

  /**
   * Evaluates @link{TAG_CHANGE}s in detail.
   *
   * @param contrib The contribution to be analysed.
   * @return List of Tags added, deleted and changed. Contains the new values for changed
   */
  public static OSHDBTagChange evaluateTags(OSMContribution contrib) {
    if (contrib.getContributionTypes().contains(ContributionType.TAG_CHANGE)) {
      //collect keys after contribution as reference
      Map<Integer, Integer> tagsAf = new HashMap<>();
      for (OSHDBTag tag : contrib.getEntityAfter().getTags()) {
        tagsAf.put(tag.getKey(), tag.getValue());
      }

      List<OSHDBTag> deleted = new ArrayList<>();
      List<OSHDBTag> added = new ArrayList<>();
      List<OSHDBModifiedTag> changed = new ArrayList<>();

      //iterate over keys before contribution
      for (OSHDBTag tag : contrib.getEntityBefore().getTags()) {
        int key = tag.getKey();
        int oldVal = tag.getValue();
        if (tagsAf.containsKey(key)) {
          Integer newVal = tagsAf.get(key);
          if (oldVal != newVal) {
            changed.add(new OSHDBModifiedTag(
                new OSHDBTag(key, oldVal),
                new OSHDBTag(key, newVal)));
          }
        } else {
          deleted.add(new OSHDBTag(key, oldVal));
        }
        //remove processed keys
        tagsAf.remove(key);
      }

      //only contains new objects
      for (Entry<Integer, Integer> tag : tagsAf.entrySet()) {
        added.add(new OSHDBTag(tag.getKey(), tag.getValue()));
      }

      return new OSHDBTagChange(added, deleted, changed);
    }
    return new OSHDBTagChange(new ArrayList<>(0), new ArrayList<>(0), new ArrayList<>(0));
  }

}
