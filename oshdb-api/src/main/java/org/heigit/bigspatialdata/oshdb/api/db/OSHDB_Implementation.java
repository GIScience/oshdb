package org.heigit.bigspatialdata.oshdb.api.db;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;

public abstract class OSHDB_Implementation extends OSHDB {
    protected String prefix = "";

    /**
     * Factory function that creates a mapReducer object of the appropriate data type class for this oshdb backend implemenation
     *
     * @param forClass the data type class to iterate over in the `mapping` function of the generated MapReducer
     * @return a new mapReducer object operating on the given OSHDB backend
     */
    public abstract <X> MapReducer<X> createMapReducer(Class<?> forClass);

    public OSHDB_Implementation prefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    public String prefix() {
        return this.prefix;
    }
}
