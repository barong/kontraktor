package org.nustaq.reallive.interfaces;

import java.io.Serializable;
import java.util.function.BiConsumer;

/**
 * Created by ruedi on 19.08.16.
 */
public interface RLBiConsumer<A,B> extends BiConsumer<A,B>, Serializable {
}
