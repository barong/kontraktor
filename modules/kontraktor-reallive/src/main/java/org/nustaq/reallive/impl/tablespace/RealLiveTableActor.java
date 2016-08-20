package org.nustaq.reallive.impl.tablespace;

import org.nustaq.kontraktor.*;
import org.nustaq.kontraktor.annotations.CallerSideMethod;
import org.nustaq.kontraktor.annotations.Local;
import org.nustaq.kontraktor.impl.CallbackWrapper;
import org.nustaq.kontraktor.remoting.encoding.CallbackRefSerializer;
import org.nustaq.kontraktor.util.Log;
import org.nustaq.reallive.impl.*;
import org.nustaq.reallive.impl.storage.StorageStats;
import org.nustaq.reallive.interfaces.*;
import org.nustaq.reallive.messages.AddMessage;

import java.util.HashMap;
import java.util.function.*;

/**
 * Created by ruedi on 06.08.2015.
 *
 * core implementation of a table
 *
 * FIXME: missing
 * - CAS/updateActions
 * - originator
 *
 *
 */
public class RealLiveTableActor extends Actor<RealLiveTableActor> implements RealLiveTable {

    public static int MAX_QUERY_BATCH_SIZE = 10;
    public static boolean DUMP_QUERY_TIME = false;

    StorageDriver storageDriver;
    FilterProcessor filterProcessor;
    HashMap<String,Subscriber> receiverSideSubsMap = new HashMap();
    TableDescription description;

    @Local
    public void init( Supplier<RecordStorage> storeFactory, TableDescription desc) {
        this.description = desc;
        Thread.currentThread().setName("Table "+desc.getName()+" main");
        RecordStorage store = storeFactory.get();
        storageDriver = new StorageDriver(store);
        filterProcessor = new FilterProcessor(this);
        storageDriver.setListener( filterProcessor );
    }

    @Override
    public void receive(ChangeMessage change) {
        checkThread();
        try {
            storageDriver.receive(change);
        } catch (Exception th) {
            Log.sError(this, th);
        }
    }

    @Override
    protected void hasStopped() {
    }

    // subscribe/unsubscribe
    // on callerside, the subscription is decomposed to kontraktor primitives
    // and a subscription id (locally unique)
    // remote receiver then builds a unique id by concatening localid#connectionId

    @Override
    @CallerSideMethod public void subscribe(Subscriber subs) {
        // need callerside to transform to Callback
        Callback callback = (r, e) -> {
            if (Actors.isResult(e))
                subs.getReceiver().receive((ChangeMessage) r);
        };
        _subscribe(subs.getFilter(), callback, subs.getId());
    }

    public void _subscribe(RLPredicate<Record> prePatchFilter, Callback cb, int id) {
        checkThread();
        Subscriber localSubs = new Subscriber(prePatchFilter, change -> {
            cb.stream(change);
        }).serverSideCB(cb);
        String sid = addChannelIdIfPresent(cb, ""+id);
        receiverSideSubsMap.put(sid,localSubs);

        queryAll(prePatchFilter, null, (r,e) -> cb.stream(new AddMessage((Record)r)) );
        localSubs.getReceiver().receive(RLUtil.get().done());
        filterProcessor.startListening(localSubs);
    }

    protected String addChannelIdIfPresent(Callback cb, String sid) {
        if ( cb instanceof CallbackWrapper && ((CallbackWrapper) cb).isRemote() ) {
            // hack to get unique id sender#connection
            CallbackRefSerializer.MyRemotedCallback realCallback
                = (CallbackRefSerializer.MyRemotedCallback) ((CallbackWrapper) cb).getRealCallback();
            sid += "#"+realCallback.getChanId();
        }
        return sid;
    }

    @CallerSideMethod @Override
    public void unsubscribe(Subscriber subs) {
        _unsubscribe( (r,e) -> {}, subs.getId() );
    }

    public void _unsubscribe( Callback cb /*dummy required to find sending connection*/, int id ) {
        checkThread();
        String sid = addChannelIdIfPresent(cb, ""+id);
        Subscriber subs = (Subscriber) receiverSideSubsMap.get(sid);
        filterProcessor.unsubscribe(subs);
        receiverSideSubsMap.remove(sid);
        cb.finish();
        subs.getServerSideCB().finish();
    }

    @Override
    public IPromise<Record> get(String key) {
        return resolve(storageDriver.getStore().get(key));
    }

    @Override
    public IPromise<Object> mutate(String key, RLFunction<Record, Object> action) {
        return resolve(storageDriver.mutate(key,action));
    }

    @Override
    public void mutateQuiet(String key, RLFunction<Record, Object> action) {
        storageDriver.mutate(key,action);
    }

    @Override
    public void mutateAll(RLPredicate<Record> filter, RLFunction<Record, Object> action, Callback remoteStream) {
        storageDriver.mutateAll(filter,action,remoteStream);
    }

    @Override
    public void queryAll(RLPredicate<Record> filter, RLFunction<Record, Object> patchingFilter, Callback remoteStream) {
        storageDriver.queryAll(filter,patchingFilter,remoteStream);
    }

    @Override
    public void addOrUpdate(String key, Object... keyVals) {
        storageDriver.addOrUpdate(key,keyVals);
    }

    @Override
    public void add(String key, Object... keyVals) {
        storageDriver.add(key,keyVals);
    }

    @Override
    public void add(Record rec) {
        storageDriver.add(rec);
    }

    @Override
    public void addOrUpdateRec(Record rec) {
        storageDriver.addOrUpdateRec(rec);
    }

    @Override
    public void put(Record rec) {
        storageDriver.put(rec);
    }

    @Override
    public void update(String key, Object... keyVals) {
        storageDriver.update(key,keyVals);
    }

    @Override
    public void remove(String key) {
        storageDriver.remove(key);
    }

    @Override
    public IPromise<Long> size() {
        return resolve(storageDriver.getStore().size());
    }

    @Override
    public IPromise<TableDescription> getDescription() {
        return resolve(description);
    }

    @Override
    public IPromise<StorageStats> getStats() {
        try {
            final StorageStats stats = storageDriver.getStore().getStats();
            return resolve(stats);
        } catch (Throwable th) {
            Log.sWarn(this, th);
            return reject(th.getMessage());
        }
    }

}
