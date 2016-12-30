package org.nustaq.kontraktor.remoting.http;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import org.nustaq.kontraktor.IPromise;

/**
 * Created by ruedi on 03/09/15.
 *
 * can be used to use an actor to handle special request synchronously (e.g. user registration links clicked
 * from an email)
 *
 */
public class HttpSyncActorAdaptorHandler implements HttpHandler {

    public interface AsyncHttpHandler {
        void handleRequest(HttpServerExchange exchange);
    }

    AsyncHttpHandler asyncHandler;

    public HttpSyncActorAdaptorHandler(AsyncHttpHandler asyncHandler) {
        this.asyncHandler = asyncHandler;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        exchange.dispatch();
        asyncHandler.handleRequest(exchange);
    }

    public AsyncHttpHandler getAsyncHandler() {
        return asyncHandler;
    }
}
