package org.heigit.ohsome.oshdb.source;

import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

public interface OSMSource {

  Flux<Tuple2<OSMType, Flux<OSMEntity>>> entities(TagTranslator tagTranslator);
}
