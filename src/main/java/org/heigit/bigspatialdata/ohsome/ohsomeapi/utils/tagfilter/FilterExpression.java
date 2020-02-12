package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import com.google.common.collect.Streams;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

abstract class FilterExpression {
  abstract boolean applyOSM(OSMEntity e);
  boolean applyOSH(OSHEntity e) {
    return Streams.stream(e.getVersions()).anyMatch(this::applyOSM);
  }
}
