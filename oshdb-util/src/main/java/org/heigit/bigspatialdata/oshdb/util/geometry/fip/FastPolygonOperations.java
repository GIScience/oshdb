package org.heigit.bigspatialdata.oshdb.util.geometry.fip;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygonal;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class FastPolygonOperations implements Serializable {
  private final int AVERAGE_VERTICES_PER_BLOCK = 40; // todo: finetune this value

  private int numBands;

  private ArrayList<Geometry /*Polygonal or(??) empty*/> blocks;

  private Envelope env;
  private double envWidth;
  private double envHeight;

  public <P extends Geometry & Polygonal> FastPolygonOperations(P geom) {
    double optNumBands = Math.sqrt(1.0 * geom.getNumPoints() / AVERAGE_VERTICES_PER_BLOCK);
    final int bandIterations = (int) Math.ceil(Math.log(optNumBands) / Math.log(2));
    numBands = (int) Math.pow(2, bandIterations);

    env = geom.getEnvelopeInternal();
    envWidth = env.getMaxX() - env.getMinX();
    envHeight = env.getMaxY() - env.getMinY();

    GeometryFactory gf = new GeometryFactory();

    Geometry[] result = new Geometry[numBands*numBands];
    traverseQuads(bandIterations, 0,0, env, geom, gf, result);

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
    if (level == 0) {
      int index = y + x * numBands;
      resultBuffer[index] = theGeom;
    } else {
      Envelope bottomLeftPart = new Envelope(
          quadEnv.getMinX(),
          (quadEnv.getMinX() + quadEnv.getMaxX()) / 2,
          quadEnv.getMinY(),
          (quadEnv.getMinY() + quadEnv.getMaxY()) / 2
      );
      Envelope topLeftPart = new Envelope(
          quadEnv.getMinX(),
          (quadEnv.getMinX() + quadEnv.getMaxX()) / 2,
          (quadEnv.getMinY() + quadEnv.getMaxY()) / 2,
          quadEnv.getMaxY()
      );
      Envelope bottomRightPart = new Envelope(
          (quadEnv.getMinX() + quadEnv.getMaxX()) / 2,
          quadEnv.getMaxX(),
          quadEnv.getMinY(),
          (quadEnv.getMinY() + quadEnv.getMaxY()) / 2
      );
      Envelope topRightPart = new Envelope(
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

  public Geometry intersection(Geometry other) {
    if (other == null) return null;
    Envelope otherEnv = other.getEnvelopeInternal();

    int minBandX = Math.max(0, Math.min(numBands - 1, (int)Math.floor((otherEnv.getMinX() - env.getMinX())/envWidth * numBands)));
    int maxBandX = Math.max(0, Math.min(numBands - 1, (int)Math.floor((otherEnv.getMaxX() - env.getMinX())/envWidth * numBands)));
    int minBandY = Math.max(0, Math.min(numBands - 1, (int)Math.floor((otherEnv.getMinY() - env.getMinY())/envHeight * numBands)));
    int maxBandY = Math.max(0, Math.min(numBands - 1, (int)Math.floor((otherEnv.getMaxY() - env.getMinY())/envHeight * numBands)));

    Geometry intersector = null;

    for (int x = minBandX; x <= maxBandX; x++) {
      for (int y = minBandY; y <= maxBandY; y++) {
        Geometry block = blocks.get(y + x*numBands);
        if (intersector == null) {
          intersector = block;
        } else {
          intersector = intersector.union(block);
        }
      }
    }

    assert intersector != null;
    return other.intersection(intersector);
  }



}
