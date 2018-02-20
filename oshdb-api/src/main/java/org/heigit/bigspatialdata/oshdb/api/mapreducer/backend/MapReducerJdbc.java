package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.TableNames;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.util.CellId;

abstract class MapReducerJdbc<X> extends MapReducer<X> {
  MapReducerJdbc(OSHDBDatabase oshdb, Class<? extends OSHDBMapReducible> forClass) {
    super(oshdb, forClass);
  }

  // copy constructor
  MapReducerJdbc(MapReducerJdbc obj) {
    super(obj);
  }


  protected ResultSet getOshCellsRawDataFromDb(Pair<CellId, CellId> cellIdRange) throws SQLException {
    String sqlQuery = this._typeFilter.stream()
        .map(osmType -> TableNames.forOSMType(osmType)
            .map(tn -> tn.toString(this._oshdb.prefix())))
        .filter(Optional::isPresent).map(Optional::get)
        .map(tn -> "(select data from " + tn + " where level = ?1 and id between ?2 and ?3)")
        .collect(Collectors.joining(" union all "));
    // fetch data from H2 DB
    PreparedStatement pstmt = ((OSHDBJdbc)this._oshdb).getConnection().prepareStatement(sqlQuery);
    pstmt.setInt(1, cellIdRange.getLeft().getZoomLevel());
    pstmt.setLong(2, cellIdRange.getLeft().getId());
    pstmt.setLong(3, cellIdRange.getRight().getId());

    // execute statement
    return pstmt.executeQuery();
  }

  /**
   * returns data of one cell from the raw data stream
   */
  protected GridOSHEntity readOshCellRawData(ResultSet oshCellsRawData)
      throws IOException, ClassNotFoundException, SQLException {
    return (GridOSHEntity)
        (new ObjectInputStream(oshCellsRawData.getBinaryStream(1))).readObject();
  }
}
