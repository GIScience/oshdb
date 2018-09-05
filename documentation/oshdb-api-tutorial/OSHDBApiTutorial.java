package org.heigit.bigspatialdata.oshdb-tutorial;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;

public class Init {
  

  public static void main(String[] args) {
          
        try {
  
          OSHDBDatabase oshdb = new OSHDBH2("path/to/extraxt.oshdb");
                    
          // create MapReducer
          MapReducer<OSMEntitySnapshot> mapReducer = OSMEntitySnapshotView.on(oshdb);
          // or
          MapReducer<OSMContribution> mapReducerContribution = OSMContributionView.on(oshdb);
          
        } catch (Exception e) {
          e.printStackTrace();
    }

  }

}
