package org.nustaq.reallive.records;

import org.nustaq.reallive.interfaces.*;
import org.nustaq.reallive.impl.RLUtil;

import java.util.function.Function;

/**
 * Created by ruedi on 04.08.2015.
 *
 * a record stored by reallive.
 *
 */
public class MapRecord implements Record {

    public static Class<? extends MapRecord> recordClass = MapRecord.class;
    public static Function<MapRecord,MapRecord> conversion;

    public static  MapRecord New() {
        try {
            return recordClass.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    protected String key;

    protected IRecordMap map = new ArrayRecordMap();

    protected MapRecord() {
    }

    public static  MapRecord New(String key) {
        MapRecord mapRecord = New();
        mapRecord.key = key;
        return mapRecord;
    }

    public static  MapRecord New(String key, Object ... values) {
        MapRecord mapRecord = New();
        RLUtil.get().buildRecord(mapRecord,values);
        return mapRecord;
    }

    public int size() {
        return map.size();
    }

    @Override
    public String getKey() {
        return key;
    }

//    @Override
    public void key(String key) {
        this.key = key;
    }

    @Override
    public String[] getFields() {
        return map.getFields();
    }

    @Override
    public Object get(String field) {
        return map.get(field);
    }

    @Override
    public MapRecord put(String field, Object value) {
        field=field.intern();
        if (value == null)
            map.remove(field);
        else
            map.put(field, value);
        return this;
    }

    @Override
    public String toString() {
        return "MapRecord{" + asString() + '}';
    }

    /**
     * @return a shallow copy
     */
    public MapRecord copied() {
        MapRecord newReq = MapRecord.New(getKey());
        map.forEach( (k,v) -> newReq.put(k,v) );
        return newReq;
    }
}
