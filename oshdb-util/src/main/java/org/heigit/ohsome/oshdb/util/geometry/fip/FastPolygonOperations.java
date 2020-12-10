package org.heigit.ohsome.oshdb.util.geometry.fip;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;

public class FastPolygonOperations implements Serializable {
  private static final int AVERAGE_VERTICES_PER_BLOCK = 40; // todo: finetune this value

  private final int numBands;

  private final ArrayList<Geometry /*Polygonal or(??) empty*/> blocks;

  private final Envelope env;
  private final double envWidth;
  private final double envHeight;

  /**
   * Constructor using a given geometry {@code geom} and geometry type {@code P}.
   *
   * @param geom geometry object
   * @param <P> geometry type
   */
  public <P extends Geometry & Polygonal> FastPolygonOperations(P geom) {
    double optNumBands = Math.max(1.0,
        Math.sqrt(1.0 * geom.getNumPoints() / AVERAGE_VERTICES_PER_BLOCK));
    final int bandIterations = (int) Math.ceil(Math.log(optNumBands) / Math.log(2));
    numBands = (int) Math.pow(2, bandIterations);

    env = geom.getEnvelopeInternal();
    envWidth = env.getMaxX() - env.getMinX();
    envHeight = env.getMaxY() - env.getMinY();

    GeometryFactory gf = new GeometryFactory();

    Geometry[] result = new Geometry[numBands * numBands];
    traverseQuads(bandIterations, 0, 0, env, geom, gf, result);

    blocks = new ArrayList<>(Arrays.asList(result));
  }

  private void traverseQuads(
      int level,
      int x, int y,
      Envelope quadEnv,
      Geometry theGeom,
      GeometryFactory gf,
      Geometry[] resultBuffer
  ) {
    if (!(theGeom instanceof Polygonal)) {
      // after clipping, the geometry might contain superfluous points or lines along the clipping
      // edges. These GeometryCollections would cause issues in the intersection method (e.g.
      // during the "union" operation). Here we need to clean these up.
      if (theGeom instanceof GeometryCollection) {
        List<Polygon> gcPolys = new ArrayList<>(theGeom.getNumGeometries());
        for (int i = 0; i < theGeom.getNumGeometries(); i++) {
          Geometry gcGeom = theGeom.getGeometryN(i);
          if (gcGeom instanceof Polygon) {
            gcPolys.add((Polygon) gcGeom);
          } else if (gcGeom instanceof MultiPolygon) {
            for (int j = 0; j < gcGeom.getNumGeometries(); j++) {
              gcPolys.add((Polygon) gcGeom.getGeometryN(j));
            }
          }
        }
        if (gcPolys.size() == 1) {
          theGeom = gcPolys.get(0);
        } else {
          theGeom = gf.createMultiPolygon(gcPolys.toArray(new Polygon[0]));
        }
      } else {
        theGeom = gf.createPolygon();
      }
    }
    if (level == 0) {
      int index = y + x * numBands;
      resultBuffer[index] = theGeom;
    } else {
      final Envelope bottomLeftPart = new Envelope(
          quadEnv.getMinX(),
          (quadEnv.getMinX() + quadEnv.getMaxX()) / 2,
          quadEnv.getMinY(),
          (quadEnv.getMinY() + quadEnv.getMaxY()) / 2
      );
      final Envelope topLeftPart = new Envelope(
          quadEnv.getMinX(),
          (quadEnv.getMinX() + quadEnv.getMaxX()) / 2,
          (quadEnv.getMinY() + quadEnv.getMaxY()) / 2,
          quadEnv.getMaxY()
      );
      final Envelope bottomRightPart = new Envelope(
          (quadEnv.getMinX() + quadEnv.getMaxX()) / 2,
          quadEnv.getMaxX(),
          quadEnv.getMinY(),
          (quadEnv.getMinY() + quadEnv.getMaxY()) / 2
      );
      final Envelope topRightPart = new Envelope(
          (quadEnv.getMinX() + quadEnv.getMaxX()) / 2,
          quadEnv.getMaxX(),
          (quadEnv.getMinY() + quadEnv.getMaxY()) / 2,
          quadEnv.getMaxY()
      );
      traverseQuads(level - 1,
          x * 2, y * 2,
          bottomLeftPart,
          theGeom.intersection(gf.toGeometry(bottomLeftPart)),
          gf,
          resultBuffer
      );
      traverseQuads(level - 1,
          x * 2, y * 2 + 1,
          topLeftPart,
          theGeom.intersection(gf.toGeometry(topLeftPart)),
          gf,
          resultBuffer
      );
      traverseQuads(level - 1,
          x * 2 + 1, y * 2,
          bottomRightPart,
          theGeom.intersection(gf.toGeometry(bottomRightPart)),
          gf,
          resultBuffer
      );
      traverseQuads(level - 1,
          x * 2 + 1, y * 2 + 1,
          topRightPart,
          theGeom.intersection(gf.toGeometry(topRightPart)),
          gf,
          resultBuffer
      );
    }
  }

  /**
   * Calculates the intersection with another {@link Geometry}.
   *
   * @param other an arbitrary {@link Geometry} to intersect
   * @return the intersection of this polygon with the other {@link Geometry}
   */
  public Geometry intersection(Geometry other) {
    if (other == null || other.isEmpty()) {
      return other;
    }
    Envelope otherEnv = other.getEnvelopeInternal();
    int minBandX = Math.max(0, Math.min(numBands - 1,
        (int) Math.floor((otherEnv.getMinX() - env.getMinX()) / envWidth * numBands)));
    int maxBandX = Math.max(0, Math.min(numBands - 1,
        (int) Math.floor((otherEnv.getMaxX() - env.getMinX()) / envWidth * numBands)));
    int minBandY = Math.max(0, Math.min(numBands - 1,
        (int) Math.floor((otherEnv.getMinY() - env.getMinY()) / envHeight * numBands)));
    int maxBandY = Math.max(0, Math.min(numBands - 1,
        (int) Math.floor((otherEnv.getMaxY() - env.getMinY()) / envHeight * numBands)));

    Geometry intersector = null;

    for (int x = minBandX; x <= maxBandX; x++) {
      for (int y = minBandY; y <= maxBandY; y++) {
        Geometry block = blocks.get(y + x * numBands);
        if (intersector == null) {
          intersector = block;
        } else {
          intersector = intersector.union(block);
        }
      }
    }

    assert intersector != null;
    if (other instanceof GeometryCollection) {
      return other.intersection(intersector);
    } else {
      return intersector.intersection(other);
    }
  }
}
