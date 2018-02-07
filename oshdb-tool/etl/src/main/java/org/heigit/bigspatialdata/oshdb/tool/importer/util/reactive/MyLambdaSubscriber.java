package org.heigit.bigspatialdata.oshdb.tool.importer.util.reactive;

import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscription;

import io.reactivex.FlowableSubscriber;
import io.reactivex.disposables.Disposable;
import io.reactivex.exceptions.CompositeException;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.internal.functions.Functions;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.observers.LambdaConsumerIntrospection;
import io.reactivex.plugins.RxJavaPlugins;

public final class MyLambdaSubscriber<T> extends AtomicReference<Subscription>
    implements FlowableSubscriber<T>, Subscription, Disposable, LambdaConsumerIntrospection {

  private static final long serialVersionUID = 2062592533192474429L;
  final Consumer<? super T> onNext;
  final Consumer<? super Throwable> onError;
  final Action onComplete;
  final long requestValue;

  public MyLambdaSubscriber(Consumer<? super T> onNext, Consumer<? super Throwable> onError, Action onComplete, long requestValue) {
    super();
    this.onNext = onNext;
    this.onError = onError;
    this.onComplete = onComplete;
    this.requestValue = requestValue;
  }

  @Override
  public void onSubscribe(Subscription s) {
    if (SubscriptionHelper.setOnce(this, s)) {
      try {
        request(requestValue);
      } catch (Throwable ex) {
        Exceptions.throwIfFatal(ex);
        s.cancel();
        onError(ex);
      }
    }
  }

  @Override
  public void onNext(T t) {
    if (!isDisposed()) {
      try {
        onNext.accept(t);
        request(requestValue);
      } catch (Throwable e) {
        Exceptions.throwIfFatal(e);
        get().cancel();
        onError(e);
      }
    }
  }

  @Override
  public void onError(Throwable t) {
    if (get() != SubscriptionHelper.CANCELLED) {
      lazySet(SubscriptionHelper.CANCELLED);
      try {
        onError.accept(t);
      } catch (Throwable e) {
        Exceptions.throwIfFatal(e);
        RxJavaPlugins.onError(new CompositeException(t, e));
      }
    } else {
      RxJavaPlugins.onError(t);
    }
  }

  @Override
  public void onComplete() {
    if (get() != SubscriptionHelper.CANCELLED) {
      lazySet(SubscriptionHelper.CANCELLED);
      try {
        onComplete.run();
      } catch (Throwable e) {
        Exceptions.throwIfFatal(e);
        RxJavaPlugins.onError(e);
      }
    }
  }

  @Override
  public void dispose() {
    cancel();
  }

  @Override
  public boolean isDisposed() {
    return get() == SubscriptionHelper.CANCELLED;
  }

  @Override
  public void request(long n) {
    get().request(n);
  }

  @Override
  public void cancel() {
    SubscriptionHelper.cancel(this);
  }

  @Override
  public boolean hasCustomOnError() {
    return onError != Functions.ON_ERROR_MISSING;
  }
}