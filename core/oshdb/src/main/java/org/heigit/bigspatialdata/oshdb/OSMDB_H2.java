package org.heigit.bigspatialdata.oshdb;

import org.heigit.bigspatialdata.oshdb.generic.TriFunction;
import org.heigit.bigspatialdata.oshdb.utils.OSMTimeStamp;
import com.vividsolutions.jts.geom.Geometry;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.json.simple.parser.ParseException;

public class OSMDB_H2 extends OSHDB {
    private final Connection _conn;
    
    public OSMDB_H2(String databaseFile) throws SQLException, ClassNotFoundException {
        Class.forName("org.h2.Driver");
        this._conn = DriverManager.getConnection("jdbc:h2:" + databaseFile, "sa", "");
    }
    
    @Override
    protected <R, S> S getCellIterators(Iterable<CellId> cellIds, List<Long> tstampsIds, BoundingBox bbox, Predicate<OSMEntity> filter, TriFunction<OSMTimeStamp, Geometry, OSMEntity, R> f, S s, BiFunction<S, R, S> rf) throws SQLException, IOException, ParseException, ClassNotFoundException {
        //load tag interpreter helper which is later used for geometry building
        final TagInterpreter tagInterpreter = DefaultTagInterpreter.fromH2(this._conn);
        
        for (CellId cellId : cellIds) {
            // prepare SQL statement
            PreparedStatement pstmt = this._conn.prepareStatement("(select data from grid_node where level = ?1 and id = ?2) union (select data from grid_way where level = ?1 and id = ?2) union (select data from grid_relation where level = ?1 and id = ?2)");
            pstmt.setInt(1, cellId.getZoomLevel());
            pstmt.setLong(2, cellId.getId());
            
            // execute statement
            ResultSet oshCellsRawData = pstmt.executeQuery();
            
            // iterate over the result
            while (oshCellsRawData.next()) {
                // get one cell from the raw data stream
                GridOSHEntity oshCellRawData = (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1))).readObject();
                
                // iterate over the history of all OSM objects in the current cell
                List<R> rs = new ArrayList<>();
                CellIterator.iterateByTimestamps(oshCellRawData, bbox, tstampsIds, tagInterpreter, filter, false).forEach(result -> result.entrySet().forEach(entry -> {
                    List<Long> x = tstampsIds;
                    OSMTimeStamp tstamp = new OSMTimeStamp(entry.getKey());
                    Geometry geometry = entry.getValue().getRight();
                    OSMEntity entity = entry.getValue().getLeft();
                    rs.add(f.apply(tstamp, geometry, entity));
                }));
                
                // fold the results
                for (R r : rs) {
                    s = rf.apply(s, r);
                }
            }
        }
        return s;
    }   
}
