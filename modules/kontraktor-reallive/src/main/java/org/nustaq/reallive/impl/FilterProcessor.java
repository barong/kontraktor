package org.nustaq.reallive.impl;

import org.nustaq.reallive.impl.tablespace.RealLiveTableActor;
import org.nustaq.reallive.interfaces.*;
import org.nustaq.reallive.messages.*;
import org.nustaq.reallive.records.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by moelrue on 04.08.2015.
 *
 * A filtering listener allows for registration of filtered subscribers and
 * processes + transforms incoming changes on a per subscriber base:
 *
 * in: old record, new record
 * if ( filter matches old && ! new ) => send Remove
 * if ( filter matches old && new ) => send Update
 * if ( filter ! matches old && new ) => send Add
 */
public class FilterProcessor implements ChangeReceiver {

    List<Subscriber> filterList = new ArrayList<>();
    RealLiveTableActor table;

    public FilterProcessor(RealLiveTableActor table) {
        this.table = table;
    }

    public void startListening(Subscriber subs) {
        filterList.add(subs);
    }

    public void unsubscribe( Subscriber subs ) {
        filterList.remove(subs);
    }

    public void receive(ChangeMessage change) {
        switch (change.getType()) {
            case ChangeMessage.QUERYDONE:
                break;
            case ChangeMessage.PUT:
                processPut((PutMessage) change);
                break;
            case ChangeMessage.ADD:
                processAdd((AddMessage) change);
                break;
            case ChangeMessage.UPDATE:
                processUpdate((UpdateMessage) change);
                break;
            case ChangeMessage.REMOVE:
                processRemove((RemoveMessage) change);
                break;
        }
    }

    protected void processPut(PutMessage change) {
        Record record = change.getRecord();
        for ( Subscriber subscriber : filterList ) {
            if ( subscriber.getFilter().test(record) ) {
                subscriber.getReceiver().receive(change);
            }
        }
    }

    protected void processUpdate(UpdateMessage change) {
        Record newRecord = change.getNewRecord();
        String[] changedFields = change.getDiff().getChangedFields();
        Object[] oldValues = change.getDiff().getOldValues();
        Record oldRec = new RecordWrapper(newRecord) {
            @Override
            public Object get(String field) {
                int index = ChangeUtils.indexOf(field, changedFields);
                if ( index >= 0 ) {
                    return oldValues[index];
                }
                return super.get(field);
            }
        };
        for ( Subscriber subscriber : filterList ) {
            boolean matchesOld = subscriber.getFilter().test((Record) oldRec);
            boolean matchesNew = subscriber.getFilter().test(newRecord);

            if ( matchesNew ) {
                matchesNew = subscriber.getFilter().test(newRecord);
            }

            if ( matchesOld ) {
                matchesOld = subscriber.getFilter().test(oldRec);
            }

            // commented conditions are redundant
            if ( matchesOld && matchesNew) {
                subscriber.getReceiver().receive(change);
            } else if ( matchesOld /*&& ! matchesNew*/ ) {
                subscriber.getReceiver().receive(new RemoveMessage((Record)newRecord));
            } else if ( /*! matchesOld &&*/ matchesNew ) {
                subscriber.getReceiver().receive(new AddMessage(newRecord));
            }
        }
    }

    protected void processAdd(AddMessage add) {
        Record record = add.getRecord();
        for ( Subscriber subscriber : filterList ) {
            if ( subscriber.getFilter().test(record) ) {
                subscriber.getReceiver().receive(new AddMessage(add.isUpdateIfExisting(),record));
            }
        }
    }

    protected void processRemove(RemoveMessage remove) {
        Record record = remove.getRecord();
        for ( Subscriber subscriber : filterList ) {
            if ( subscriber.getFilter().test(record) ) {
                if ( subscriber.getFilter().test(record))
                    subscriber.getReceiver().receive((ChangeMessage)remove);
            }
        }
    }

}
