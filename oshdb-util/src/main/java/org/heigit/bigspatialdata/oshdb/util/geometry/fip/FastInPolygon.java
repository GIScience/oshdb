package org.heigit.bigspatialdata.oshdb.util.geometry.fip;

import com.vividsolutions.jts.geom.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * fast *-in-polygon test inspired by
 * https://blog.jochentopf.com/2017-02-06-expedicious-and-exact-extracts-with-osmium.html
 */
abstract class FastInPolygon implements Serializable {
  private class Segment implements Serializable {
    private final double startX;
    private final double startY;
    private final double endX;
    private final double endY;

    Segment(Coordinate p0, Coordinate p1) {
      this.startX = p0.x;
      this.startY = p0.y;
      this.endX = p1.x;
      this.endY = p1.y;
    }
  }

  private final int AvgSegmentsPerBand = 10; // something in the order of 10-20 works fine according to the link above

  private int numBands;

  private ArrayList<List<Segment>> horizBands;
  private ArrayList<List<Segment>> vertBands;

  private Envelope env;
  private double envWidth;
  private double envHeight;

  protected <P extends Geometry & Polygonal> FastInPolygon(P geom) {
    MultiPolygon mp;
    if (geom instanceof Polygon)
      mp = (new GeometryFactory()).createMultiPolygon(new Polygon[]{(Polygon) geom});
    else
      mp = (MultiPolygon) geom;

    List<Segment> segments = new LinkedList<>();
    for (int i = 0; i < mp.getNumGeometries(); i++) {
      Polygon p = (Polygon) mp.getGeometryN(i);
      LineString er = p.getExteriorRing();
      for (int k = 1; k < er.getNumPoints(); k++) {
        Coordinate p1 = er.getCoordinateN(k - 1);
        Coordinate p2 = er.getCoordinateN(k);
        segments.add(new Segment(p1, p2));
      }
      for (int j = 0; j < p.getNumInteriorRing(); j++) {
        LineString r = p.getInteriorRingN(j);
        for (int k = 1; k < r.getNumPoints(); k++) {
          Coordinate p1 = r.getCoordinateN(k - 1);
          Coordinate p2 = r.getCoordinateN(k);
          segments.add(new Segment(p1, p2));
        }
      }
    }
    this.numBands = Math.max(1, segments.size() / AvgSegmentsPerBand); // possible optimization: start with this value of numBands, and if the result has over-full bands, increase numBands (e.g. x2) and do it again
    this.horizBands = new ArrayList<>(numBands);
    for (int i = 0; i < numBands; i++) this.horizBands.add(new LinkedList<>());
    this.vertBands = new ArrayList<>(numBands);
    for (int i = 0; i < numBands; i++) this.vertBands.add(new LinkedList<>());

    this.env = mp.getEnvelopeInternal();
    this.envWidth = env.getMaxX() - env.getMinX();
    this.envHeight = env.getMaxY() - env.getMinY();
    segments.forEach(segment -> {
      int startHorizBand = Math.max(0, Math.min(numBands - 1, (int) Math.floor(((segment.startY - env.getMinY()) / envHeight) * numBands)));
      int endHorizBand = Math.max(0, Math.min(numBands - 1, (int) Math.floor(((segment.endY - env.getMinY()) / envHeight) * numBands)));
      for (int i = Math.min(startHorizBand, endHorizBand); i <= Math.max(startHorizBand, endHorizBand); i++) {
        horizBands.get(i).add(segment);
      }
      int startVertBand = Math.max(0, Math.min(numBands - 1, (int) Math.floor(((segment.startX - env.getMinX()) / envWidth) * numBands)));
      int endVertBand = Math.max(0, Math.min(numBands - 1, (int) Math.floor(((segment.endX - env.getMinX()) / envWidth) * numBands)));
      for (int i = Math.min(startVertBand, endVertBand); i <= Math.max(startVertBand, endVertBand); i++) {
        vertBands.get(i).add(segment);
      }
    });
  }

  /**
   * ported from http://geomalgorithms.com/a03-_inclusion.html
   * which is derived from https://wrf.ecse.rpi.edu//Research/Short_Notes/pnpoly.html
   *
   * @param point
   * @param dir   boolean: true -> horizontal test, false -> vertical test
   * @return crossing number of this point in the chosen direction, if the value is even the point is outside of the polygon, otherwise it is inside
   */
  protected int crossingNumber(Point point, boolean dir) {
    return dir ? crossingNumberX(point) : crossingNumberY(point);
  }

  private int crossingNumberX(Point point) {
    List<Segment> band;
    int horizBand = (int) Math.floor(((point.getY() - env.getMinY()) / envHeight) * numBands);
    horizBand = Math.max(0, Math.min(numBands - 1, horizBand));
    band = horizBands.get(horizBand);

    int cn = 0; // crossing number counter
    for (Segment segment : band) {
      // if (((V[i].y <= P.y) && (V[i+1].y > P.y))     // an upward crossing
      // || ((V[i].y > P.y) && (V[i+1].y <=  P.y))) { // a downward crossing
      if ((segment.startY <= point.getY() && segment.endY > point.getY()) || // an upward crossing
          (segment.startY > point.getY() && segment.endY <= point.getY())) {  // a downward crossing
        // compute  the actual edge-ray intersect x-coordinate
        /*float vt = (float)(P.y  - V[i].y) / (V[i+1].y - V[i].y);
        if (P.x <  V[i].x + vt * (V[i+1].x - V[i].x)) // P.x < intersect
            ++cn; // a valid crossing of y=P.y right of P.x*/
        double vt = (point.getY() - segment.startY) / (segment.endY - segment.startY);
        if (point.getX() < segment.startX + vt * (segment.endX - segment.startX)) { // P.x < intersect
          cn++; // a valid crossing of y=P.y right of P.x
        }
      }
    }
    return cn; // even -> outside, odd -> inside
  }

  private int crossingNumberY(Point point) {
    List<Segment> band;
    int vertBand = (int) Math.floor(((point.getX() - env.getMinX()) / envWidth) * numBands);
    vertBand = Math.max(0, Math.min(numBands - 1, vertBand));
    band = vertBands.get(vertBand);

    int cn = 0; // crossing number counter
    for (Segment segment : band) {
      if ((segment.startX <= point.getX() && segment.endX > point.getX()) || // an "upward" crossing
          (segment.startX > point.getX() && segment.endX <= point.getX())) {  // a "downward" crossing
        // compute  the actual edge-ray intersect x-coordinate
        double vt = (point.getX() - segment.startX) / (segment.endX - segment.startX);
        if (point.getY() < segment.startY + vt * (segment.endY - segment.startY)) { // P.y < intersect
          cn++; // a valid crossing of x=P.x below of P.y
        }
      }
    }
    return cn; // even -> outside, odd -> inside
  }
}