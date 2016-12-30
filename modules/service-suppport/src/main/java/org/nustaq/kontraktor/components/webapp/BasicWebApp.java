package org.nustaq.kontraktor.components.webapp;

import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.nustaq.kontraktor.*;
import org.nustaq.kontraktor.impl.SimpleScheduler;
import org.nustaq.kontraktor.remoting.base.SessionResurrector;
import org.nustaq.kontraktor.remoting.http.HttpSyncActorAdaptorHandler;
import org.nustaq.kontraktor.remoting.tcp.TCPConnectable;
import org.nustaq.kontraktor.services.base.ClusterCfg;
import org.nustaq.kontraktor.services.base.ServiceActor;
import org.nustaq.kontraktor.services.base.ServiceDescription;
import org.nustaq.kontraktor.services.base.ServiceRegistry;
import org.nustaq.kontraktor.services.base.rlclient.DataClient;
import org.nustaq.kontraktor.util.Log;

/**
 * Created by ruedi on 30.12.16.
 */
public class BasicWebApp extends Actor<BasicWebApp> implements HttpSyncActorAdaptorHandler.AsyncHttpHandler, SessionResurrector {

    public static BasicWebAppArgs options;

    private Scheduler[] clientThreads;

    // global context shared ammongst sessions
    // allowed to contain thread safe objects only !
    public static class AppContext {
        public BasicWebAppService service;
        public DataClient dclient;
        public ClusterCfg cfg;
    }

    protected AppContext context;

    public IPromise init() {
        context = new AppContext();

        initService(); // this one connects to the cluster (so this is the webserver with an embedded cluster service)
        // pull required juptr cluster stuff out of the service
        context.dclient = context.service.getDataClient().await();
        context.cfg = context.service.getConfig().await();

        // queue and send mails in a dedicated actor (thread) because java mail api sucks and blocks.
//        hermes = Actors.AsActor(Hermes.class);
//        hermes.init(cfg);

        clientThreads = new Scheduler[context.cfg.getWebServerSessionThreads()];
        for (int i = 0; i < clientThreads.length; i++) {
            clientThreads[i] = new SimpleScheduler(context.cfg.getWebServerSessionThreads(),true);
        }

        // avoid receiving request before init
        return resolve();
    }

    protected void initService() {
        context.service = Actors.asActor(BasicWebAppService.class);
        context.service.init( new TCPConnectable(ServiceRegistry.class, options.getGravityHost(), options.getGravityPort() ), options, true).await();
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) {
        String response = "<html>"+exchange.getRequestPath()+" response</html>";
        try {
            exchange.setResponseCode(200);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/html; charset=utf-8");
            exchange.getResponseSender().send(response);
            exchange.endExchange();
        } catch (Exception e) {
            Log.sWarn(this, e);
        }
    }

    @Override
    public void restoreRemoteRefConnection(String sessionId) {
        Log.sInfo(this, "restore " + sessionId);
    }

    @Override
    public IPromise<Actor> reanimate(String httpSessionId, long remoteRefId) {
        Log.sInfo(this, "reanimating " + httpSessionId + " " + remoteRefId);
        // TODO: recreate, initialize and return a session for the given Id if possible
        return null;
    }

    public static class BasicWebAppService extends ServiceActor<BasicWebAppService> {

        @Override
        protected String[] getRequiredServiceNames() {
            return new String[0];
        }

        @Override
        protected ServiceDescription createServiceDescription() {
            return new ServiceDescription("WebAppServer");
        }
    }


}
