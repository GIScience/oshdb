package org.heigit.bigspatialdata.oshdb.api.db;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;

public abstract class OSHDB_Implementation extends OSHDB {
    public abstract <X> MapReducer<X> createMapReducer(Class<?> forClass);
}
