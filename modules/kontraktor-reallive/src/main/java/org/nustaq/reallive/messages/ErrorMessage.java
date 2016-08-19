package org.nustaq.reallive.messages;

import org.nustaq.reallive.interfaces.ChangeMessage;

/**
 * Created by ruedi on 20.08.16.
 */
public class ErrorMessage implements ChangeMessage {
    Object error;

    public ErrorMessage(Object e) {
        this.error = e;
    }

    @Override
    public int getType() {
        return ERR;
    }

    public Object getError() {
        return error;
    }

    @Override
    public String getKey() {
        return null;
    }

    @Override
    public ChangeMessage reduced(String[] reducedFields) {
        return this;
    }
}
