package org.heigit.ohsome.oshdb.osm;

import java.io.Serializable;
import java.util.function.Predicate;

public interface OSMEntityFilter extends Predicate<OSMEntity>, Serializable {

}
