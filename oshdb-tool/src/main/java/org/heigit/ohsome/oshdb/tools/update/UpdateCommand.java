package org.heigit.ohsome.oshdb.tools.update;

import static reactor.core.publisher.Flux.concat;
import static reactor.core.publisher.Flux.just;
import static reactor.core.publisher.Flux.range;
import static reactor.core.publisher.Mono.fromCallable;

import java.nio.file.Path;
import java.util.concurrent.Callable;
import org.heigit.ohsome.oshdb.source.osc.ReplicationEndpoint;
import org.heigit.ohsome.oshdb.source.osc.ReplicationState;
import org.heigit.ohsome.oshdb.store.OSHDBStore;
import org.heigit.ohsome.oshdb.store.update.OSHDBUpdater;
import org.heigit.ohsome.oshdb.util.tagtranslator.CachedTagTranslator;
import org.reactivestreams.Publisher;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import reactor.core.publisher.Flux;

@Command(name = "update")
public class UpdateCommand implements Callable<Integer> {
  private static final Logger log = LoggerFactory.getLogger(UpdateCommand.class);

  @Option(names = {"--path"}, required = true)
  Path path;

  @Override
  public Integer call() throws Exception {
    try (var store = openStore()) {
      var tagTranslator = new CachedTagTranslator(store.getTagTranslator(), 10 * SizeUnit.MB);

      Flux.range(0, Integer.MAX_VALUE)
          .concatMap(x -> states(store))
          .concatMap(state -> update(store, tagTranslator, state));

      var currentState = ReplicationEndpoint.stateFromInfo(store.state());
      var serverState = currentState.serverState();

      var startSequence = currentState.getSequenceNumber() + 1;
      var endSequence = serverState.getSequenceNumber();
      var states = concat(range(startSequence, endSequence - startSequence)
          .concatMap(sequence -> fromCallable(() -> serverState.state(sequence))),
          just(serverState));
      states.concatMap(state ->
        state.entities(tagTranslator)

      );


      return 0;
    }
  }

  private Publisher<?> update(OSHDBStore store, CachedTagTranslator tagTranslator, ReplicationState state) {
      var updater = new OSHDBUpdater(store, (type, id, grid) -> {});
      updater.updateEntities(state.entities(tagTranslator));
      store.state(state);
      throw new UnsupportedOperationException();
  }


  private OSHDBStore openStore() {
    throw new UnsupportedOperationException();
  }

  private Flux<ReplicationState> states(OSHDBStore store) {
    try {
      var currentState = ReplicationEndpoint.stateFromInfo(store.state());
      var serverState = currentState.serverState();
      var startSequence = currentState.getSequenceNumber() + 1;
      var endSequence = serverState.getSequenceNumber();
      log.info("currentState: {}", currentState);
      log.info("serverState: {}", serverState);
      log.info("states to {} - {} ({})", startSequence, endSequence, endSequence - startSequence + 1);
      var states = range(startSequence, endSequence - startSequence)
          .concatMap(sequence -> fromCallable(() -> serverState.state(sequence)));
      return concat(states, just(serverState));
    } catch(Exception e) {
      return Flux.error(e);
    }
  }

  //  private Mono<ReplicationState> wait(ReplicationState state) {
//    var wait = Duration.between(Instant.now(), state.nextTimestamp());
//    log.info("wait {}m{}s {}", wait.toMinutesPart(), wait.toSecondsPart(), state);
//    return Flux.interval(wait, Duration.ofSeconds(2))
//        .concatMap(x -> fromCallable(state::serverState))
//        .doOnNext(newState -> log.info("check {}", state))
//        .filter(newState -> newState.getSequenceNumber() > state.getSequenceNumber())
//        .next();
//  }

}
