/*
Kontraktor-reactivestreams Copyright (c) Ruediger Moeller, All rights reserved.

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 3.0 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

See https://www.gnu.org/licenses/lgpl.txt
*/
package org.nustaq.kontraktor.reactivestreams;

import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.Callback;
import org.nustaq.kontraktor.annotations.CallerSideMethod;
import org.nustaq.kontraktor.reactivestreams.impl.KxPublisherActor;
import org.nustaq.kontraktor.reactivestreams.impl.KxSubscriber;
import org.nustaq.kontraktor.remoting.base.ActorPublisher;
import org.nustaq.kontraktor.remoting.base.ActorServer;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Created by ruedi on 30/06/15.
 */
public interface KxPublisher<T> extends Publisher<T> {

    /**
     * consuming endpoint. requests data from publisher immediately after
     * receiving onSubscribe callback. Maps from reactive streams Subscriber to Kontraktor Callback.
     *
     * Call inherited subscribe(Publisher) to subscribe a 'standard' reactivestreams subscriber
     *
     * all events are routed to the callback
     * e.g.
     * <pre>
     *      subscriber( (event, err) -> {
     +          if (Actors.isComplete(err)) {
     +              System.out.println("complete");
     +          } else if (Actors.isError(err)) {
     +              System.out.println("ERROR");
     +          } else {
     +              // process event
     +          }
     +      }
     * </pre>
     *
     * @param
     */
    @CallerSideMethod default void subscribe( Callback<T> cb ) {
        subscribe(getKxStreamsInstance().subscriber(cb));
    }

    @CallerSideMethod KxReactiveStreams getKxStreamsInstance();

    /**
     * consuming endpoint. requests data from publisher immediately after
     * receiving onSubscribe callback. Maps from reactive streams Subscriber to Kontraktor Callback
     *
     * Call inherited subscribe(Publisher) to subscribe a 'standard' reactivestreams subscriber
     *
     * all events are routed to the callback
     * e.g.
     * <pre>
     *      subscriber( (event, err) -> {
     +          if (Actors.isComplete(err)) {
     +              System.out.println("complete");
     +          } else if (Actors.isError(err)) {
     +              System.out.println("ERROR");
     +          } else {
     +              // process event
     +          }
     +      }
     * </pre>
     *
     */
    @CallerSideMethod default void subscribe( int batchSize, Callback<T> cb ) {
        subscribe(getKxStreamsInstance().subscriber(batchSize, cb));
    }

    @CallerSideMethod default void stream(Consumer<Stream<T>> streamingCode ) {
        stream( getKxStreamsInstance().getBatchSize(), streamingCode);
    }

    @CallerSideMethod default void stream( int batchSize, Consumer<Stream<T>> streamingCode ) {
        if ( Actor.inside() ) {
            try {
                Stream<T> stream = getKxStreamsInstance().stream(KxPublisher.this, batchSize);
                streamingCode.accept(stream);
            } catch (Throwable ce) {
                Subscription subscription = KxSubscriber.subsToCancel.get();
                if ( subscription != null )
                    subscription.cancel();
                throw ce;
            }
        } else {
            new Thread("Stream Consumer") {
                @Override
                public void run() {
                    try {
                        Stream<T> stream = getKxStreamsInstance().stream(KxPublisher.this, batchSize);
                        streamingCode.accept(stream);
                    } catch (Throwable ce) {
                        Subscription subscription = KxSubscriber.subsToCancel.get();
                        if ( subscription != null )
                            subscription.cancel();
                        throw ce;
                    }
                }
            }.start();
        }
    }

    @CallerSideMethod default void iterator(int batchSize, Consumer<Iterator<T>> iteratingCode ) {
        if ( Actor.inside() ) {
            try {
                iteratingCode.accept(getKxStreamsInstance().iterator(KxPublisher.this, batchSize));
            } catch (Throwable ce) {
                Subscription subscription = KxSubscriber.subsToCancel.get();
                subscription.cancel();
                throw ce;
            }
        } else {
            new Thread("Iterator Consumer") {
                @Override
                public void run() {
                    try {
                        iteratingCode.accept(getKxStreamsInstance().iterator(KxPublisher.this, batchSize));
                    } catch (Throwable ce) {
                        Subscription subscription = KxSubscriber.subsToCancel.get();
                        subscription.cancel();
                        throw ce;
                    }
                }
            }.start();
        }
    }

    @CallerSideMethod default void iterator( Consumer<Iterator<T>> iteratingCode ) {
        iterator(getKxStreamsInstance().getBatchSize(),iteratingCode);
    }

    /**
     * insert an async processor (with dedicated queue, multiple subscribers)
     *
     * @param processor
     * @param <OUT>
     * @return
     */
    @CallerSideMethod default <OUT> KxPublisher<OUT> map(Function<T, OUT> processor) {
        Processor<T, OUT> toutProcessor = getKxStreamsInstance().newAsyncProcessor(processor);
        subscribe(toutProcessor);
        return (KxPublisher<OUT>) toutProcessor;
    }

    /**
     * insert an identity processor (with dedicated queue). Required e.g. if connecting
     * streams/iterators to a synchronous publisher.
     *
     * @param <OUT>
     * @return
     */
    @CallerSideMethod default <OUT> KxPublisher<OUT> async() {
        return (KxPublisher<OUT>) map(x -> x);
    }

    @CallerSideMethod default <OUT> KxPublisher<OUT> lossy() {
        return (KxPublisher<OUT>)lossyMap(x -> x);
    }

    @CallerSideMethod default <OUT> KxPublisher<OUT> lossyMap(Function<T,OUT> processor) {
        return lossyMap(processor, getKxStreamsInstance().getBatchSize());
    }

    @CallerSideMethod default <OUT> KxPublisher<OUT> lossyMap(Function<T,OUT> processor, int batchSize) {
        Processor<T, OUT> toutProcessor = getKxStreamsInstance().newLossyProcessor(processor, batchSize);
        subscribe(toutProcessor);
        return (KxPublisher<OUT>) toutProcessor;
    }

    /**
     * insert an async processor (with dedicated queue, multiple subscribers)
     *
     * @param processor
     * @param batchSize
     * @param <OUT>
     * @return
     */
    @CallerSideMethod default <OUT> KxPublisher<OUT> map(Function<T, OUT> processor, int batchSize) {
        Processor<T, OUT> toutProcessor = getKxStreamsInstance().newAsyncProcessor(processor, batchSize);
        subscribe(toutProcessor);
        return (KxPublisher<OUT>) toutProcessor;
    }

    /**
     * publish current stream onto a network connector, once the stream is complete or in error,
     * the network connection will close. Makes sense mostly for infinite streams (see RxStreamServer
     * for a more sophisticated remoting example)
     *
     * @param publisher
     * @param disconCallback
     * @return
     */
    @CallerSideMethod default ActorServer serve(ActorPublisher publisher, Consumer<Actor> disconCallback) {
        return (ActorServer) getKxStreamsInstance().serve(this, publisher, true, disconCallback).await();
    }

    @CallerSideMethod default ActorServer serve(ActorPublisher publisher, boolean closeOnDiscon, Consumer<Actor> disconCallback) {
        return (ActorServer) getKxStreamsInstance().serve(this, publisher, closeOnDiscon, disconCallback).await();
    }

    /**
     * publish current stream onto a network connector
     * Once the stream is complete or in error,
     * the network connection will close. (see RxStreamServer
     * for a more sophisticated remoting example)
     *
     * @param publisher
     * @return
     */
    @CallerSideMethod default ActorServer serve(ActorPublisher publisher) {
        return this.serve(publisher,true,null);
    }

    /**
     * insert a synchronous processor (runs in provider thread).
     * if 'this' is a remote reference to a remote stream, a queued async processor will be
     * created (need queue + processing thread then)
     *
     * @param processor
     * @param <OUT>
     * @return
     */
    @CallerSideMethod default <OUT> KxPublisher<OUT> syncMap(Function<T, OUT> processor) {
        if ( this instanceof KxPublisherActor && ((KxPublisherActor)this).isRemote() )
            return map(processor); // need a queue when connecting remote stream
        Processor<T,OUT> outkPublisher = getKxStreamsInstance().newSyncProcessor(processor);
        subscribe(outkPublisher);
        return (KxPublisher<OUT>) outkPublisher;
    }

    @CallerSideMethod default Actor asActor() {
        if ( this instanceof Actor ) {
            return (Actor) this;
        }
        return (Actor) async();
    }

}
