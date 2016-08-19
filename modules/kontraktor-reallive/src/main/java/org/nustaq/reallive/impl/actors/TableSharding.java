package org.nustaq.reallive.impl.actors;

import org.nustaq.kontraktor.*;
import org.nustaq.kontraktor.util.Log;
import org.nustaq.reallive.impl.storage.StorageStats;
import org.nustaq.reallive.interfaces.*;
import org.nustaq.reallive.impl.RLUtil;
import org.nustaq.reallive.messages.AddMessage;
import org.nustaq.reallive.messages.ErrorMessage;
import org.nustaq.reallive.messages.PutMessage;
import org.nustaq.reallive.messages.RemoveMessage;
import org.nustaq.reallive.records.RecordWrapper;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Created by moelrue on 06.08.2015.
 */
public class TableSharding implements RealLiveTable {

    ShardFunc func;
    RealLiveTable shards[];
    final ConcurrentHashMap<Subscriber,List<Subscriber>> subsMap = new ConcurrentHashMap<>();
    private TableDescription description;

    public TableSharding(ShardFunc func, RealLiveTable[] shards, TableDescription desc) {
        this.func = func;
        this.shards = shards;
        this.description = desc;
    }

    @Override
    public void receive(ChangeMessage change) {
        if ( change.getType() != ChangeMessage.QUERYDONE )
            shards[func.apply(change.getKey())].receive(change);
    }

    @Override
    public void subscribe(Subscriber subs) {
        AtomicInteger doneCount = new AtomicInteger(shards.length);
        ChangeReceiver receiver = subs.getReceiver();
        ArrayList<Subscriber> subsList = new ArrayList<>();
        for (int i = 0; i < shards.length; i++) {
            RealLiveTable shard = shards[i];
            Subscriber shardSubs = new Subscriber(subs.getFilter(), change -> {
                if (change.getType() == ChangeMessage.QUERYDONE) {
                    int count = doneCount.decrementAndGet();
                    if (count == 0) {
                        receiver.receive(change);
                    }
                } else {
                    receiver.receive(change);
                }
            });
            subsList.add(shardSubs);
            shard.subscribe(shardSubs);
        }
        subsMap.put(subs,subsList);
    }

    @Override
    public void unsubscribe(Subscriber subs) {
        if ( subs == null ) {
            Log.sWarn(this, "unsubscribed is null");
            return;
        }
        List<Subscriber> subscribers = subsMap.get(subs);
        if ( subscribers == null ) {
            Log.sWarn(this, "unknown subscriber to unsubscribe " + subs);
            return;
        }
        for (int i = 0; i < subscribers.size(); i++) {
            Subscriber subscriber = subscribers.get(i);
            shards[i].unsubscribe(subscriber);
        }
        subsMap.remove(subs);
    }

    @Override
    public void addOrUpdate(String key, Object... keyVals) {
        shards[func.apply(key)].receive(RLUtil.get().addOrUpdate(key, keyVals));
    }

    @Override
    public void add(String key, Object... keyVals) {
        shards[func.apply(key)].receive(RLUtil.get().add(key, keyVals));
    }

    @Override
    public void add(Record rec) {
        if ( rec instanceof RecordWrapper )
            rec = ((RecordWrapper) rec).getRecord();
        shards[func.apply(rec.getKey())].receive(new AddMessage(rec));
    }

    @Override
    public void addOrUpdateRec(Record rec) {
        if ( rec instanceof RecordWrapper )
            rec = ((RecordWrapper) rec).getRecord();
        shards[func.apply(rec.getKey())].receive(new AddMessage(true,rec));
    }

    @Override
    public void put(Record rec) {
        if ( rec instanceof RecordWrapper )
            rec = ((RecordWrapper) rec).getRecord();
        shards[func.apply(rec.getKey())].receive(new PutMessage(rec));
    }

    @Override
    public void update(String key, Object... keyVals) {
        shards[func.apply(key)].receive(RLUtil.get().update(key, keyVals));
    }

    @Override
    public void remove(String key) {
        RemoveMessage remove = RLUtil.get().remove(key);
        shards[func.apply(key)].receive(remove);
    }

    @Override
    public IPromise ping() {
        List<IPromise<Object>> futs = new ArrayList<>();
        for (int i = 0; i < shards.length; i++) {
            RealLiveTable shard = shards[i];
            futs.add(shard.ping());
        }
        return Actors.all(futs);
    }

    @Override
    public IPromise<TableDescription> getDescription() {
        return new Promise(description);
    }

    public void stop() {
        for (int i = 0; i < shards.length; i++) {
            shards[i].stop();
        }
    }

    @Override
    public IPromise<StorageStats> getStats() {
        Promise res = new Promise();
        try {
            Actors.all(shards.length, i -> shards[i].getStats()).then( (shardStats,err) -> {
                if (shardStats!=null) {
                    StorageStats stats = new StorageStats();
                    for (int i = 0; i < shardStats.length; i++) {
                        StorageStats storageStats = shardStats[i].get();
                        storageStats.addTo(stats);
                    }
                    res.resolve(stats);
                } else {
                    res.reject(err);
                }
            });
        } catch (Exception e) {
            Log.sWarn(this, e);
            res.reject(e);
        }
        return res;
    }

    @Override
    public IPromise<Record> get(String key) {
        if ( key == null )
            return null;
        return shards[func.apply(key)].get(key);
    }

    @Override
    public IPromise<Object> mutate(String key, RLFunction<Record, Object> action) {
        return shards[func.apply(key)].mutate(key, action);
    }

    @Override
    public void mutateQuiet(String key, RLFunction<Record, Object> action) {
        shards[func.apply(key)].mutateQuiet(key, action);
    }

    @Override
    public void mutateAll(RLPredicate<Record> filter, RLFunction<Record, Object> action, Callback remoteStream) {
        AtomicInteger count = new AtomicInteger(shards.length);
        for (int i = 0; i < shards.length; i++) {
            RealLiveTable shard = shards[i];
            if ( remoteStream != null ) {
                shard.mutateAll(filter, action, (r, e) -> {
                    if (Actors.isResult(e)) {
                        remoteStream.stream(r);
                    } else {
                        int co = count.decrementAndGet();
                        if (Actors.isError(e)) {
                            remoteStream.stream(new ErrorMessage(e));
                        }
                        if (co == 0) {
                            remoteStream.finish();
                        }
                    }
                });
            } else {
                shard.mutateAll(filter, action, null );
            }
        }
    }

    @Override
    public void queryAll(RLPredicate<Record> filter, RLFunction<Record, Object> patchingFilter, Callback remoteStream) {
        AtomicInteger count = new AtomicInteger(shards.length);
        for (int i = 0; i < shards.length; i++) {
            RealLiveTable shard = shards[i];
            shard.queryAll(filter, patchingFilter, (r, e) -> {
                if (Actors.isResult(e)) {
                    remoteStream.stream(r);
                } else {
                    int co = count.decrementAndGet();
                    if (Actors.isError(e)) {
                        remoteStream.stream(new ErrorMessage(e));
                    }
                    if (co == 0) {
                        remoteStream.finish();
                    }
                }
            });
        }
    }

    @Override
    public IPromise<Long> size() {
        Promise result = new Promise();
        List<IPromise<Long>> futs = new ArrayList<>();
        for (int i = 0; i < shards.length; i++) {
            RealLiveTable shard = shards[i];
            futs.add(shard.size());
        }
        Actors.all(futs).then( longPromisList -> {
            long sum = longPromisList.stream().mapToLong(prom -> prom.get()).sum();
            result.resolve(sum);
        });
        return result;
    }
}
