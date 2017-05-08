package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.heigit.bigspatialdata.oshdb.examples.activity.ActivityIndicatorFromPolygon;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class ISEA3HGrid {

	public static void main(String[] args) throws IOException, SQLException, IndexOutOfBoundsException, ParseException, ClassNotFoundException {
		// TODO Auto-generated method stub

		//load shapefile
		File file = new File("data/grid19_hd_ISEA3H_poly.shp");
	    Map<String, Object> map = new HashMap<>();
	    map.put("url", file.toURI().toURL());

	    DataStore dataStore = DataStoreFinder.getDataStore(map);
	    String typeName = dataStore.getTypeNames()[0];

	    FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore
	            .getFeatureSource(typeName);
	    Filter filter = Filter.INCLUDE; // ECQL.toFilter("BBOX(THE_GEOM, 10,20,30,40)")

	    Connection conn = DriverManager.getConnection("jdbc:h2:tcp://localhost/d:/eclipseNeon2Workspace/OSH-BigDB/core/hosmdb/resources/oshdb/heidelberg-ccbysa","sa", "");
	    
	    WKTReader reader = new WKTReader();

		FileWriter fw = new FileWriter("hd_activity.csv");
		
		
	    
	    FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures(filter);
	    
	    List<Geometry> list = new ArrayList<>();
		
	    try (FeatureIterator<SimpleFeature> features = collection.features()) {
	        while (features.hasNext()) {
	            SimpleFeature feature = features.next();
//	            System.out.print(feature.getID());
//	            System.out.print(": ");
//	            System.out.println(feature.getDefaultGeometryProperty().getValue());
	           // System.out.println( feature.getAttribute(0).toString() );
	            Geometry multipolygon = reader.read(feature.getAttribute(0).toString());
	            list.add(multipolygon);
	            
	            
//	            System.out.println(feature.getAttribute(1) +";" +  cellresult.get(Long.valueOf(1451606400)) + ";" + feature.getAttribute(0).toString());
	        }
	        
	        list.parallelStream().map(multipoly -> {
	        	ActivityIndicatorFromPolygon aifp = new ActivityIndicatorFromPolygon();
	            Map<Long,Long> cellresult = aifp.execute(conn, multipolygon);
	            fw.append(feature.getAttribute(1) +";" +  cellresult.get(Long.valueOf(1451606400)) + ";" + feature.getAttribute(0).toString() + "\n");
	        return null;
	        }).sum();
	    }
		fw.close();
		System.out.println("Done");
	}

}
