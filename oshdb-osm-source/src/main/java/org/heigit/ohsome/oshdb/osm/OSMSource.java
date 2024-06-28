package org.heigit.ohsome.oshdb.osm;

import org.heigit.ohsome.oshdb.TagTranslator;
import reactor.core.publisher.Flux;

public interface OSMSource {

  Flux<OSMEntity> entities(TagTranslator translator);
}
