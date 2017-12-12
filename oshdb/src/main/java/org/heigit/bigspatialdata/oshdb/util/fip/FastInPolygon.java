package org.heigit.bigspatialdata.oshdb.util.fip;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.triangulate.Segment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * fast *-in-polygon test inspired by
 * https://blog.jochentopf.com/2017-02-06-expedicious-and-exact-extracts-with-osmium.html
 *
 * todo: does this work for random winding order polygons? -> think it should always work
 * todo: can this be improved to work on long/int coordinates directly???
 */
abstract class FastInPolygon {
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
            mp = (new GeometryFactory()).createMultiPolygon(new Polygon[] {(Polygon)geom});
        else
            mp = (MultiPolygon)geom;

        List<Segment> segments = new LinkedList<>();
        for (int i=0; i<mp.getNumGeometries(); i++) {
            Polygon p = (Polygon)mp.getGeometryN(i);
            LineString er = p.getExteriorRing();
            for (int k=1; k<er.getNumPoints(); k++) {
                Coordinate p1 = er.getCoordinateN(k-1);
                Coordinate p2 = er.getCoordinateN(k);
                segments.add(new Segment(p1,p2));
            }
            for (int j=0; j<p.getNumInteriorRing(); j++) {
                LineString r = p.getInteriorRingN(j);
                for (int k=0; k<r.getNumPoints(); k++) {
                    Coordinate p1 = r.getCoordinateN(k-1);
                    Coordinate p2 = r.getCoordinateN(k);
                    segments.add(new Segment(p1,p2));
                }
            }
        }
        this.numBands = Math.max(1, segments.size() / AvgSegmentsPerBand); // todo: do something more clever here? e.g. this is not so optimal for a multipolygon with far away exclaves
        //this.horizBands = new ArrayList<>(Collections.nCopies(numBands, new LinkedList<>()));
        this.horizBands = new ArrayList<>(numBands);
        for (int i=0; i<numBands; i++) this.horizBands.add(new LinkedList<>());
        //this.vertBands = new ArrayList<>(Collections.nCopies(numBands, new LinkedList<>()));
        this.vertBands = new ArrayList<>(numBands);
        for (int i=0; i<numBands; i++) this.vertBands.add(new LinkedList<>());

        this.env = mp.getEnvelopeInternal();
        this.envWidth = env.getMaxX() - env.getMinX();
        this.envHeight = env.getMaxY() - env.getMinY();
        segments.forEach(segment -> {
            int startHorizBand = Math.max(0, (int) Math.floor(((segment.getStartX() - env.getMinX()) / envWidth) * numBands));
            int endHorizBand = Math.min(numBands-1, (int) Math.floor(((segment.getEndX() - env.getMinX()) / envWidth) * numBands));
            for (int i=startHorizBand; i<=endHorizBand; i++) {
                horizBands.get(i).add(segment);
            }
            int startVertBand = Math.max(0, (int) Math.floor(((segment.getStartY() - env.getMinY()) / envHeight) * numBands));
            int endVertBand = Math.min(numBands-1, (int) Math.floor(((segment.getEndY() - env.getMinY()) / envHeight) * numBands));
            for (int i=startVertBand; i<=endVertBand; i++) {
                horizBands.get(i).add(segment);
            }
        });
    }

    /**
     * http://geomalgorithms.com/a03-_inclusion.html
     *
     * @param point
     * @param dir boolean: true -> horizontal test, false -> vertical test
     * @return
     */
    protected int crossingNumber(Point point, boolean dir) {
        return dir ? crossingNumberX(point) : crossingNumberY(point);
    }
    private int crossingNumberX(Point point) {
        List<Segment> band;
        int horizBand = (int) Math.floor(((point.getX() - env.getMinX()) / envWidth) * numBands);
        horizBand = Math.max(0, Math.min(numBands-1, horizBand));
        band = horizBands.get(horizBand);

        int cn = 0; // crossing number counter
        for (Segment segment : band) {
            // compute  the actual edge-ray intersect x-coordinate
            /*float vt = (float)(P.y  - V[i].y) / (V[i+1].y - V[i].y);
            if (P.x <  V[i].x + vt * (V[i+1].x - V[i].x)) // P.x < intersect
                ++cn;   // a valid crossing of y=P.y right of P.x*/
            double vt = (point.getY() - segment.getStartY()) / (segment.getEndY() - segment.getStartY());
            if (point.getX() < segment.getStartX() + vt * (segment.getEndX() - segment.getStartX())) { // P.x < intersect
                cn++; // a valid crossing of y=P.y right of P.x
            }
        }
        return cn; //  even -> outside, edd -> inside
        //return cn % 2 == 1; // 0 if even (out), and 1 if  odd (in)
    }
    private int crossingNumberY(Point point) {
        List<Segment> band;
        int vertBand = (int) Math.floor(((point.getY() - env.getMinY()) / envHeight) * numBands);
        vertBand = Math.max(0, Math.min(numBands-1, vertBand));
        band = vertBands.get(vertBand);

        int cn = 0; // crossing number counter
        for (Segment segment : band) {
            // compute  the actual edge-ray intersect x-coordinate
            double vt = (point.getX() - segment.getStartX()) / (segment.getEndX() - segment.getStartX());
            if (point.getY() < segment.getStartY() + vt * (segment.getEndY() - segment.getStartY())) { // P.y < intersect
                cn++; // a valid crossing of x=P.x below of P.y
            }
        }
        return cn; //  even -> outside, edd -> inside
        //return cn % 2 == 1; // 0 if even (out), and 1 if  odd (in)
    }
}