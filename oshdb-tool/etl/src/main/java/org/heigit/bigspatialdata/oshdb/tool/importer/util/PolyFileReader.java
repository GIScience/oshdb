package org.heigit.bigspatialdata.oshdb.tool.importer.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.wololo.geojson.GeoJSON;
import org.wololo.jts2geojson.GeoJSONWriter;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;

public class PolyFileReader {

  public static class GeomWithHoles {
    final LinearRing shell;
    final Polygon poly;
    List<LinearRing> holes = new ArrayList<LinearRing>();

    public GeomWithHoles(LinearRing shell, Polygon poly) {
      this.shell = shell;
      this.poly = poly;
      
    }
    
    @Override
    public String toString() {
      return String.format("shell:%s%n\tholes:%s%n", shell,holes);
    }
  }

  public static GeoJSON parse(Path polyFile) throws ParseException, FileNotFoundException, IOException{
    GeometryFactory geomFactory = new GeometryFactory();
    List<GeomWithHoles> geoms = new ArrayList<>();

    ArrayList<Coordinate> coordinates = new ArrayList<>();
    int ln = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(polyFile.toFile()))) {

      String name = reader.readLine();
      ln++;
      if (name == null || name.trim().isEmpty())
        throw new ParseException("The file must begin with a header naming the polygon file.",-1);

      String section = "";
      boolean insection = false;
      String line;
      while ((line = reader.readLine()) != null) {
        ln++;
        line = line.trim();
        if (line.isEmpty())
          continue;

        if (!insection) {
          if ("END".equalsIgnoreCase(line))
            break;
          section = line;
          insection = true;
          continue;
        }

        if ("END".equalsIgnoreCase(line)) {
          insection = false;

          if (!coordinates.get(0).equals2D(coordinates.get(coordinates.size() - 1)))
            coordinates.add(coordinates.get(0));

          LinearRing ring = geomFactory.createLinearRing(coordinates.toArray(new Coordinate[0]));
          Polygon poly = geomFactory.createPolygon(ring);
          coordinates.clear();
          if (section.startsWith("!")) {
            for(GeomWithHoles geom : geoms){
              if(poly.intersects(geom.poly))
                geom.holes.add(ring);
            }
          } else {
            geoms.add(new GeomWithHoles(ring,poly ));
          }
          continue;
        }

        String[] split = line.split("\\s+");
        if (split.length != 2)
          throw new ParseException("Could not find two coordinates on line (" + line + ")." + Arrays.toString(split),ln);

        Coordinate coord = new Coordinate();
        coord.x = Double.parseDouble(split[0]);
        coord.y = Double.parseDouble(split[1]);

        coordinates.add(coord);
      }
    }
    
   
    List<Polygon> polys = new ArrayList<>(geoms.size());
    for(GeomWithHoles geom : geoms)
      polys.add(geomFactory.createPolygon(geom.shell, geom.holes.toArray(new LinearRing[0])));
    
    Geometry geom = (polys.size() > 1)?geomFactory.createMultiPolygon(polys.toArray(new Polygon[0])):polys.get(0);

    GeoJSONWriter writer = new GeoJSONWriter();
    GeoJSON json = writer.write(geom);
    return json;
  }
}
