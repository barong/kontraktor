package org.nustaq.reallive.records;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiConsumer;

/**
 * Created by ruedi on 28.12.16.
 */
public class ArrayRecordMap implements IRecordMap, Serializable {
    public static final int NUMFIELDS = 40;
    public static final int NUM_RECS = 2_000_000;
    String fields[] = new String[0];
    Object values[] = new Object[0];

    @Override
    public int size() {
        return fields.length;
    }

    @Override
    public void toArray(String[] targetfields) {
        for (int i = 0; i < fields.length; i++) {
            targetfields[i] = fields[i];
        }
    }

    @Override
    public Object get(String field) {
        int i = getIndexForKey(field);
        if ( i>=0 )
            return values[i];
        return null;
    }

    private int getIndexForKey(String field) {
        for (int i = 0; i < fields.length; i++) {
            String s = fields[i];
            if ( s.hashCode() == field.hashCode() && field.equals(s) ) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public Object put(String field, Object value) {
        int i = getIndexForKey(field);
        if ( i >= 0 ) {
            Object ret = values[i];
            values[i] = value;
            return ret;
        }
        else {
            String[] prevF = fields;
            Object[] prevValues = values;
            int len = prevF.length;
            fields = new String[len+1];
            values = new Object[len+1];
            System.arraycopy(prevF,0,fields,0,len);
            System.arraycopy(prevValues,0,values,0,len);
            fields[len] = field;
            values[len] = value;
            return null;
        }
    }

    @Override
    public void remove(String field) {
        int i = getIndexForKey(field);
        if ( i >= 0 ) {
            String[] prevF = fields;
            Object[] prevValues = values;
            int len = prevF.length;
            fields = new String[len-1];
            values = new Object[len-1];
            for (int j = 0; j < prevF.length; j++) {
                if ( j <= i ) {
                    fields[j] = prevF[j];
                    values[j] = prevValues[j];
                } else {
                    fields[j-1] = prevF[j];
                    values[j-1] = prevValues[j];
                }
            }
            len--;
        }
    }

    @Override
    public void forEach(BiConsumer<String, Object> iter) {
        Iterator<String> keyIt = Arrays.stream(fields).iterator();
        Iterator<Object> valIt = Arrays.stream(values).iterator();
        while( keyIt.hasNext() ) {
            iter.accept(keyIt.next(),valIt.next());
        }
    }

    @Override
    public String[] getFields() {
        return fields;
    }

    @Override
    public Object[] getValues() {
        return values;
    }

    public static void main0(String[] args) {
        List<IRecordMap> recs = new ArrayList<>();
        System.out.println("create");
        for ( int i = 0; i < NUM_RECS; i++ )
            recs.add(createRec());
        System.out.println("done");

        String key11key1 = "key"+(NUMFIELDS/2)+"key";
        while( true ) {
            long tim = System.currentTimeMillis();
            for (int i = 0; i < recs.size(); i++) {
                IRecordMap iRecordMap = recs.get(i);
                Object key11key = iRecordMap.get(key11key1);
            }
            System.out.println("tim rec "+(System.currentTimeMillis()-tim));
        }
    }

    public static void main(String[] args) {
        new Thread(() -> main0(null)).start();
        List<Map> recs = new ArrayList<>();
        System.out.println("create");
        for (int i = 0; i < NUM_RECS; i++ )
            recs.add(createMap());
        System.out.println("done");

        String key11key1 = "key"+(NUMFIELDS/2)+"key";
        while( true ) {
            long tim = System.currentTimeMillis();
            for (int i = 0; i < recs.size(); i++) {
                Map iRecordMap = recs.get(i);
                Object key11key = iRecordMap.get(key11key1);
            }
            System.out.println("tim map "+(System.currentTimeMillis()-tim));
        }
    }

    private static IRecordMap createRec() {
        ArrayRecordMap rec = new ArrayRecordMap();
        for (int i = 0; i < NUMFIELDS; i++ ) {
            rec.put("key"+i+"key", i);
        }
        return rec;
    }

    private static Map createMap() {
        HashMap rec = new HashMap();
        for (int i=0; i < NUMFIELDS; i++ ) {
            rec.put("key"+i+"key", i);
        }
        return rec;
    }
}
