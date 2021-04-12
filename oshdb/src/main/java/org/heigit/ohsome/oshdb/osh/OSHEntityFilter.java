package org.heigit.ohsome.oshdb.osh;

import java.io.Serializable;
import java.util.function.Predicate;

public interface OSHEntityFilter extends Predicate<OSHEntity>, Serializable {

}
