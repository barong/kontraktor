package org.nustaq.reallive.records;

import java.util.function.BiConsumer;

/**
 * Created by ruedi on 28.12.16.
 */
public interface IRecordMap {
    int size();
    void toArray(String[] fields);
    Object get(String field);
    Object put(String field, Object value);
    void remove(String field);
    void forEach(BiConsumer<String,Object> iter);

    String[] getFields();

    Object[] getValues();
}
