Changelog OSHDB MODULE
======================

## 0.7

### renaming
 - OSMWay.getRef() -> OSMWay.getMember()
 - OSHDBTimestamp.getRawUnixTimestamp() -> OSHDBTimestamp.getEpochSecond()
 
### moving
 - oshdb.util.OSHDBTimestamp - oshdb.OSHDBTimestamp
 - oshdb.util.OSHDBTag - oshdb.OSHDBTag
 - CellIterator.OSHEntityFilter/OSMEntityFilter - oshdb.osh.OSHEntityFilter, oshdb.osm.OSMEntityFilter
 - oshdb-api.generic.function - oshdb.util.function
 

### breaking changes

### new features
 - new interface OSHDBTimeable
 - new interface OSHDBBoundable

