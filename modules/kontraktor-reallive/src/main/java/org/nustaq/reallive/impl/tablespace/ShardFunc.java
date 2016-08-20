package org.nustaq.reallive.impl.tablespace;

import java.io.Serializable;

/**
 * Created by moelrue on 06.08.2015.
 */
public interface ShardFunc<K> extends Serializable {
    int apply(K key);
}
