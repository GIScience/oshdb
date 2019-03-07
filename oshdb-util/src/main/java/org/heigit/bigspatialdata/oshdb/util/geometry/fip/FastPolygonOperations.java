package org.heigit.bigspatialdata.oshdb.util.geometry.fip;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygonal;
import java.io.Serializable;
import java.util.ArrayList;
import org.geotools.geometry.jts.JTS;
import org.heigit.bigspatialdata.oshdb.util.geometry.Geo;

public class FastPolygonOperations implements Serializable {
  private final int AvgVerticesPerBlock = 40; // todo: finetune this value

  private int numBands;

  private ArrayList<Geometry /*Polygonal or(??) empty*/> blocks;

  private Envelope env;
  private double envWidth;
  private double envHeight;


  public <P extends Geometry & Polygonal> FastPolygonOperations(P geom) {
    numBands = (int)Math.ceil(Math.sqrt(1.0*geom.getNumPoints()/AvgVerticesPerBlock));
    blocks = new ArrayList<>(numBands*numBands);

    env = geom.getEnvelopeInternal();
    envWidth = env.getMaxX() - env.getMinX();
    envHeight = env.getMaxY() - env.getMinY();

    for (int x = 0; x < numBands; x++) {
      for (int y = 0; y < numBands; y++) {
        Envelope envPart = new Envelope(
            env.getMinX() + envWidth * x/numBands,
            env.getMinX() + envWidth * (x+1)/numBands,
            env.getMinY() + envHeight * y/numBands,
            env.getMinY() + envHeight * (y+1)/numBands
        );
        blocks.add(// index: y + x*numBands,
            geom.intersection(JTS.toGeometry(envPart))
        );
      }
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
        if (intersector == null)
          intersector = block;
        else
          intersector = intersector.union(block);
      }
    }

    return other.intersection(intersector);
  }



}
