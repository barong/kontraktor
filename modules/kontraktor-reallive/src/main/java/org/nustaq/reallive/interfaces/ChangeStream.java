package org.nustaq.reallive.interfaces;

import org.nustaq.kontraktor.annotations.CallerSideMethod;
import org.nustaq.reallive.impl.QueryPredicate;
import org.nustaq.reallive.query.CompiledQuery;
import org.nustaq.reallive.query.Query;

import java.text.ParseException;

/**
 * Created by moelrue on 03.08.2015.
 */
public interface ChangeStream {

    void subscribe( Subscriber subs );

    default @CallerSideMethod
    Subscriber subscribeOn(RLPredicate<Record> filter, ChangeReceiver receiver) {
        Subscriber subs = new Subscriber(filter,receiver);
        this.subscribe(subs);
        return subs;
    }

    default @CallerSideMethod
    Subscriber subscribeOn(String query, ChangeReceiver receiver) throws ParseException {
        Subscriber subs = new Subscriber(new QueryPredicate<>(query),receiver);
        this.subscribe(subs);
        return subs;
    }

    void unsubscribe( Subscriber subs );

}
