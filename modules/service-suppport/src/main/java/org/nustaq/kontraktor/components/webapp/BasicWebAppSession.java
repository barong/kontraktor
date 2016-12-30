package org.nustaq.kontraktor.components.webapp;

import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.remoting.base.RemotedActor;

/**
 * Created by ruedi on 30.12.16.
 */
public class BasicWebAppSession<T extends BasicWebAppSession> extends Actor<T> implements RemotedActor {

    @Override
    public void hasBeenUnpublished() {

    }

}
