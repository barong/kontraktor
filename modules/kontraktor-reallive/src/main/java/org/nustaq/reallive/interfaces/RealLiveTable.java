package org.nustaq.reallive.interfaces;

import org.nustaq.kontraktor.Actors;
import org.nustaq.kontraktor.Callback;
import org.nustaq.kontraktor.IPromise;
import org.nustaq.kontraktor.Promise;
import org.nustaq.kontraktor.annotations.CallerSideMethod;
import org.nustaq.reallive.impl.QueryPredicate;
import org.nustaq.reallive.impl.RLUtil;
import org.nustaq.reallive.impl.StorageDriver;
import org.nustaq.reallive.impl.storage.StorageStats;
import org.nustaq.reallive.messages.AddMessage;
import org.nustaq.reallive.messages.PutMessage;
import org.nustaq.reallive.messages.RemoveMessage;
import org.nustaq.reallive.records.RecordWrapper;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by ruedi on 06/08/15.
 */
public interface RealLiveTable extends ChangeReceiver, ChangeStream {

    IPromise ping();
    IPromise<TableDescription> getDescription();
    void stop();
    IPromise<StorageStats> getStats();
    IPromise<Long> size();

    /**
     * return record with given key or null
     *
     * @param key
     * @return
     */
    IPromise<Record> get( String key );

    /**
     * retrieve Object with given key and apply it to the 'action' function.
     * The function might modify the record (diffs will be computed automatically).
     * If the function returns StorageDriver.DELETE, the record will be deleted.
     * The object returned by the action function is passed as a result of this.
     *
     * @param key
     * @param action - a function executed remotely (take care to not to accidentally serialize context objects)
     * @return
     */
    IPromise<Object> mutate(String key, RLFunction<Record,Object> action);

    /**
     * same as mutate, but avoids resulting back messages
     *
     * @param key
     * @param action - a function executed remotely (take care to not to accidentally serialize context objects)
     * @return
     */
    void mutateQuiet(String key, RLFunction<Record,Object> action);

    /**
     * mass mutation. Each record matching 'filter' is passed to action function.
     * If the action function returns DELETE, the record will be deleted.
     * If remoteStream is not null, all objects returned by the action function will be streamed
     * to the callback.
     *
     * @param filter
     * @param action - a function executed remotely (take care to not to accidentally serialize context objects)
     * @param remoteStream - maybe null for quiet operation (performance)
     */
    void mutateAll(RLPredicate<Record> filter, RLFunction<Record, Object> action, Callback remoteStream);

    /**
     * query all records optionally modifying resulting records temporary (only visible to callee)
     * @param filter - read only filter applied to record, can be null
     * @param patchingFilter - read write filter (can modify record). The resulting object is passed
     *                       to callback if != null. patchingFilter can be null
     * @param remoteStream - result receiver
     */
    void queryAll(RLPredicate<Record> filter, RLFunction<Record,Object> patchingFilter, Callback remoteStream);

    /**
     * creates or updates record with given key
     *
     * @param key
     * @param keyVals - array of { "key", value, ... }
     */
    void addOrUpdate(String key, Object... keyVals);

    /**
     * adds a new record. If the record exists, its a NOP
     *
     * @param key
     * @param keyVals
     */
    void add(String key, Object... keyVals);

    /**
     * adds a new record. If the record exists, its a NOP
     *
     */
    void add(Record rec);

    /**
     * adds or updates a new record. If the record exists, a field level
     * merge is performed. Note that null values are ignored so its not possible
     * to null out existing record field values.
     *
     */
    void addOrUpdateRec(Record rec);

    void put(Record rec);

    void update(String key, Object... keyVals);

    void remove(String key);
    
}
