package org.nustaq.reallive.impl;

import org.nustaq.kontraktor.Callback;
import org.nustaq.kontraktor.util.Log;
import org.nustaq.reallive.interfaces.*;
import org.nustaq.reallive.messages.*;
import org.nustaq.reallive.records.PatchingRecord;
import org.nustaq.reallive.records.RecordWrapper;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by moelrue on 03.08.2015.
 *
 * implements transaction processing and change message genertion on top of a physical storage
 * single threaded
 */
public class StorageDriver implements ChangeReceiver {

    public final static String DELETE = "__DELMEPLEASE";

    RecordStorage store;
    ChangeReceiver listener = change -> {};

    public StorageDriver(RecordStorage store) {
        this.store = store;
        Log.sInfo(this, "" + store.getStats());
    }

    public StorageDriver() {
    }

    public static Record unwrap(Record r) {
        if ( r instanceof PatchingRecord ) {
            return unwrap(((PatchingRecord) r).unwrapOrCopy());
        }
        if ( r instanceof RecordWrapper ) {
            return unwrap(((RecordWrapper) r).getRecord());
        }
        return r;
    }

    @Override
    public void receive(ChangeMessage change) {
        switch (change.getType()) {
            case ChangeMessage.QUERYDONE:
                break;
            case ChangeMessage.PUT:
            {
                Record prevRecord = store.get(change.getKey());
                if ( prevRecord == null ) {
                    store.put(change.getKey(),unwrap(change.getRecord()));
                    receive( new AddMessage(true,change.getRecord()));
                } else {
                    Diff diff = ChangeUtils.diff(change.getRecord(), prevRecord);
                    Record newRecord = unwrap(change.getRecord()); // clarification
                    store.put(change.getKey(),newRecord);
                    listener.receive( new UpdateMessage(diff,newRecord) );
                }
                break;
            }
            case ChangeMessage.ADD:
            {
                AddMessage addMessage = (AddMessage) change;
                String key = addMessage.getKey();
                Record prevRecord = store.get(key);
                if ( prevRecord != null && ! addMessage.isUpdateIfExisting() ) {
                    return;
                }
                if ( prevRecord != null ) {
                    Diff diff = ChangeUtils.copyAndDiff(addMessage.getRecord(), prevRecord);
                    Record newRecord = unwrap(prevRecord); // clarification
                    store.put(change.getKey(),newRecord);
                    listener.receive( new UpdateMessage(diff,newRecord) );
                } else {
                    store.put(change.getKey(),unwrap(addMessage.getRecord()));
                    listener.receive(addMessage);
                }
                break;
            }
            case ChangeMessage.REMOVE:
            {
                RemoveMessage removeMessage = (RemoveMessage) change;
                Record v = store.remove(removeMessage.getKey());
                if ( v != null ) {
                    listener.receive(new RemoveMessage(unwrap(v)));
                } else {
//                    System.out.println("*********** failed remove "+change.getKey());
//                    store.put(change.getKey(), new MapRecord(change.getKey()).put("url", "POK"));
//                    System.out.println("  reput and get:" + store.get(change.getKey()));
//                    store.remove(change.getKey());
//                    System.out.println("  re-rem and get:" + store.get(change.getKey()));
//                    store.filter( rec -> rec.getKey().equals(change.getKey()), (r,e) -> {
//                        System.out.println("  "+r);
//                    });
                }
                break;
            }
            case ChangeMessage.UPDATE:
            {
                UpdateMessage updateMessage = (UpdateMessage) change;
                Record oldRec = store.get(updateMessage.getKey());
                if ( oldRec == null && updateMessage.isAddIfNotExists() ) {
                    if ( updateMessage.getNewRecord() == null ) {
                        throw new RuntimeException("updated record does not exist, cannot fall back to 'Add' as UpdateMessage.newRecord is null");
                    }
                    store.put(change.getKey(),updateMessage.getNewRecord());
                    listener.receive( new AddMessage(updateMessage.getNewRecord()) );
                } else if ( updateMessage.getDiff() == null ) {
                    Diff diff = ChangeUtils.copyAndDiff(updateMessage.getNewRecord(), oldRec);
                    Record newRecord = unwrap(oldRec); // clarification
                    store.put(change.getKey(),newRecord);
                    listener.receive( new UpdateMessage(diff,newRecord) );
                } else {
                    // old values are actually not needed inside the diff
                    // however they are needed in a change notification for filter processing (need to reconstruct prev record)
                    Diff newDiff = ChangeUtils.copyAndDiff(updateMessage.getNewRecord(), oldRec, updateMessage.getDiff().getChangedFields());
                    Record newRecord = unwrap(oldRec); // clarification
                    store.put(change.getKey(),newRecord);
                    listener.receive( new UpdateMessage(newDiff,newRecord));
                }
                break;
            }
            default:
                throw new RuntimeException("unknown change type "+change.getType());
        }
    }

    public RecordStorage getStore() {
        return store;
    }

    public ChangeReceiver getListener() {
        return listener;
    }

    public StorageDriver store(final RecordStorage store) {
        this.store = store;
        return this;
    }

    public StorageDriver setListener(final ChangeReceiver listener) {
        this.listener = listener;
        return this;
    }

    public void put(String key, Object ... keyVals) {
        receive(RLUtil.get().put(key,keyVals));
    }

    /**
     * retrieve Object with given key and apply it to the 'action' function.
     * The function might modify the record (diffs will be computed automatically).
     * If the function returns StorageDriver.DELETE, the record will be deleted.
     * The object returned by the action function is passed as a result of this.
     *
     * @param key
     * @param action
     * @return
     */
    public Object mutate(String key, Function<Record,Object> action) {
        Record rec = getStore().get(key);
        if ( rec == null ) {
            return action.apply(rec);
        } else {
            PatchingRecord pr = new PatchingRecord(rec);
            final Object res = action.apply(pr);
            if ( DELETE.equals(res) ) {
                remove(key);
                return res;
            }
            UpdateMessage updates = pr.getUpdates();
            if ( updates != null ) {
                receive(updates);
            }
            return res;
        }
    }

    /**
     * mass mutation. Each record matching 'filter' is passed to action function.
     * If the action function returns DELETE, the record will be deleted.
     * If remoteStream is not null, all objects returned by the action function will be streamed
     * to the callback.
     *
     * @param filter
     * @param action
     * @param remoteStream - maybe null
     */
    public void mutateAll(Predicate<Record> filter, Function<Record, Object> action, Callback remoteStream) {
        store.stream().forEach(r -> {
            if (r != null && filter.test(r) ) {
                PatchingRecord pr = new PatchingRecord(r);
                Object res = action.apply(pr);
                if (DELETE.equals(res)) {
                    receive(RLUtil.get().remove(pr.getKey()));
                } else {
                    UpdateMessage updates = pr.getUpdates();
                    if (updates != null) {
                        receive(updates);
                    }
                }
                if (remoteStream!=null && res != null ) {
                    remoteStream.stream(res);
                }
            }
        });
        if (remoteStream!=null) {
            remoteStream.finish();
        }
    }

    /**
     * query all records optionally modifying resulting records temporary (only visible to callee)
     * @param filter - read only filter applied to record, can be null
     * @param patchingFilter - read write filter (can modify record). The resulting object is passed
     *                       to callback if != null. patchingFilter can be null
     * @param remoteStream - result receiver
     */
    public void queryAll(Predicate<Record> filter, Function<Record,Object> patchingFilter, Callback remoteStream) {
        PatchingRecord pr[] = { new PatchingRecord(null) };
        store.stream().forEach(r -> {
            if (r != null && (filter == null || filter.test(r)) ) {
                if (patchingFilter!=null) {
                    pr[0].reset(r);
                    Object mapped = patchingFilter.apply(pr[0]);
                    if (mapped != null) {
                        remoteStream.stream(mapped);
                    }
                    if (pr[0].hasUpdates()) {
                        pr[0] = new PatchingRecord(null);
                    }
                } else {
                    remoteStream.stream(r);
                }
            }
        });
        remoteStream.finish();
    }

    public void addOrUpdate(String key, Object... keyVals) {
        receive(RLUtil.get().addOrUpdate(key, keyVals));
    }

    public void add(String key, Object... keyVals) {
        receive(RLUtil.get().add(key, keyVals));
    }

    public void add(Record rec) {
        receive(new AddMessage(rec));
    }

    public void addOrUpdateRec(Record rec) {
        receive(new AddMessage(true,rec));
    }

    public void put(Record rec) {
        receive( new PutMessage(rec) );
    }

    public void update(String key, Object... keyVals) {
        receive(RLUtil.get().update(key, keyVals));
    }

    public void remove(String key) {
        RemoveMessage remove = RLUtil.get().remove(key);
        receive(remove);
    }
}
