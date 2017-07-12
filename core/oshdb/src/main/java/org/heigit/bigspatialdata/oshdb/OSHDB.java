package org.heigit.bigspatialdata.oshdb;

import org.heigit.bigspatialdata.oshdb.generic.TriFunction;
import com.vividsolutions.jts.geom.Geometry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.utils.OSMTimeStamp;
import org.heigit.bigspatialdata.oshdb.utils.OSMTimeStamps;

public abstract class OSHDB {
    public static final int MAXZOOM = 12;
    
    public CellMapper createCellMapper(BoundingBox bbox, OSMTimeStamps tstamps) {
        return new CellMapper(this, bbox, tstamps);
    }
    
    protected abstract <R, S> S getCellIterators(Iterable<CellId> cellIds, List<Long> tstampsIds, BoundingBox bbox, Predicate<OSMEntity> filter, TriFunction<OSMTimeStamp, Geometry, OSMEntity, R> f, S s, BiFunction<S, R, S> rf) throws Exception;
    
    public class CellMapper {
        private final OSHDB _osmdb;
        private final BoundingBox _bbox;
        private final OSMTimeStamps _tstamps;
        private List<Predicate<OSMEntity>> _filters = new ArrayList<>();
        
        private CellMapper(OSHDB osmdb, BoundingBox bbox, OSMTimeStamps tstamps) {
            this._osmdb = osmdb;
            this._bbox = bbox;
            this._tstamps = tstamps;
        }
        
        public CellMapper filter(Predicate<OSMEntity> f) {
            this._filters.add(f);
            return this;
        }
        
        public <R> List<R> map(TriFunction<OSMTimeStamp, Geometry, OSMEntity, List<R>> f) throws Exception {
            return this._osmdb.getCellIterators(this._getCellIds(), this._getTimestamps(), this._bbox, this._getFilter(), f, new ArrayList<>(), (l, r) -> {
                l.addAll(r);
                return l;
            });
        }
        
        public Double sum(TriFunction<OSMTimeStamp, Geometry, OSMEntity, Double> f) throws Exception {
            return this._osmdb.getCellIterators(this._getCellIds(), this._getTimestamps(), this._bbox, this._getFilter(), f, 0., (s, r) -> s + r);
        }
        
        public SortedMap<OSMTimeStamp, Double> aggregate(TriFunction<OSMTimeStamp, Geometry, OSMEntity, Map.Entry<OSMTimeStamp, Double>> f) throws Exception {
            return this._osmdb.getCellIterators(this._getCellIds(), this._getTimestamps(), this._bbox, this._getFilter(), f, new TreeMap<>(), (m, r) -> {
                m.put(r.getKey(), m.getOrDefault(r.getKey(), 0.) + r.getValue());
                return m;
            });
        }
        
        private Predicate<OSMEntity> _getFilter() {
            return this._filters.stream().reduce(Predicate::and).get();
        }
        
        private Iterable<CellId> _getCellIds() {
            XYGridTree grid = new XYGridTree(OSHDB.MAXZOOM);
            return grid.bbox2CellIds(this._bbox, true);
        }
        
        private List<Long> _getTimestamps() {
            return this._tstamps.getTimeStampIds();
        }
    }
}
