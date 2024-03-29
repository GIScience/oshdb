package org.heigit.ohsome.oshpbf.parser.rx;

import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import io.reactivex.internal.fuseable.QueueSubscription;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.plugins.RxJavaPlugins;
import java.util.List;
import org.heigit.ohsome.oshpbf.parser.osm.v06.Entity;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class OshMerger extends Flowable<Osh> {

  private final Flowable<Osh> source;

  public OshMerger(Flowable<Osh> source) {
    this.source = source;
  }

  @Override
  protected void subscribeActual(Subscriber<? super Osh> actual) {
    source.subscribe(new OshMergerSubscriber(actual));
  }

  private static final class OshMergerSubscriber implements FlowableSubscriber<Osh>, Subscription {

    /** The downstream subscriber. */
    private final Subscriber<? super Osh> actual;

    /** The upstream subscription. */
    private Subscription upstream;

    /** Flag indicating no further onXXX event should be accepted. */
    private boolean done;

    private Osh oshToMerge = null;

    private OshMergerSubscriber(Subscriber<? super Osh> actual) {
      this.actual = actual;
    }

    @Override
    public void onSubscribe(Subscription s) {
      if (SubscriptionHelper.validate(this.upstream, s)) {

        this.upstream = s;
        if (s instanceof QueueSubscription) {
          System.out.println("s instanceof QueueSubscription");
        }

        actual.onSubscribe(this);
      }

    }

    @Override
    public void onNext(Osh osh) {
      if (osh.isComplete) {
        if (oshToMerge != null) {
          actual.onNext(oshToMerge);
        }
        actual.onNext(osh);
        oshToMerge = null;
        return;
      }

      if (oshToMerge != null) {
        if (oshToMerge.getId() == osh.getId()) {
          actual.onNext(merge(oshToMerge, osh));
          oshToMerge = null;
          return;
        }

        actual.onNext(oshToMerge);
        oshToMerge = osh;
      }

      if (oshToMerge == null) {
        oshToMerge = osh;
        upstream.request(1);
      }

    }

    private static Osh merge(Osh a, Osh b) {
      final List<Entity> versions = a.versions;
      versions.addAll(b.versions);
      final long[] pos = new long[a.pos.length + b.pos.length];
      for (int i = 0; i < a.pos.length; i++) {
        pos[i] = a.pos[i];
      }
      for (int i = 0; i < b.pos.length; i++) {
        pos[a.pos.length + i] = b.pos[i];
      }
      return new Osh(true, versions, pos);
    }

    @Override
    public void onComplete() {
      if (done) {
        return;
      }

      done = true;
      if (oshToMerge != null) {
        actual.onNext(oshToMerge);
      }
      oshToMerge = null;
      actual.onComplete();
    }

    @Override
    public void onError(Throwable t) {
      if (done) {
        RxJavaPlugins.onError(t);
        return;
      }
      done = true;
      actual.onError(t);
    }

    @Override
    public void request(long n) {
      upstream.request(n);
    }

    @Override
    public void cancel() {
      upstream.cancel();
    }

  }
}
