package org.nustaq.kontraktor.components.webapp;

import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.nustaq.kontraktor.*;
import org.nustaq.kontraktor.impl.DispatcherThread;
import org.nustaq.kontraktor.impl.SimpleScheduler;
import org.nustaq.kontraktor.remoting.base.SessionResurrector;
import org.nustaq.kontraktor.remoting.encoding.Coding;
import org.nustaq.kontraktor.remoting.encoding.SerializerType;
import org.nustaq.kontraktor.remoting.http.Http4K;
import org.nustaq.kontraktor.remoting.http.HttpSyncActorAdaptorHandler;
import org.nustaq.kontraktor.remoting.http.builder.BldFourK;
import org.nustaq.kontraktor.remoting.tcp.TCPConnectable;
import org.nustaq.kontraktor.services.base.ClusterCfg;
import org.nustaq.kontraktor.services.base.ServiceRegistry;
import org.nustaq.kontraktor.util.Log;
import org.nustaq.reallive.interfaces.Record;
import org.nustaq.reallive.messages.AddMessage;
import org.nustaq.reallive.messages.QueryDoneMessage;
import org.nustaq.reallive.messages.RemoveMessage;
import org.nustaq.reallive.messages.UpdateMessage;
import org.nustaq.reallive.records.MapRecord;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by ruedi on 30.12.16.
 */
public class BasicWebApp<T extends BasicWebApp> extends Actor<T> implements HttpSyncActorAdaptorHandler.AsyncHttpHandler, SessionResurrector {

    public static BasicWebAppArgs options;

    protected AppContext context;
    private Scheduler[] clientThreads;

    public IPromise init() {
        context = createContext();

        context.service = initService(); // this one connects to the cluster (so this is the webserver with an embedded cluster service)

        // pull required cluster data + config out of the service
        context.dclient = context.service.getDataClient().await();
        context.cfg = context.service.getConfig().await(); // config is provided by registry

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

    protected AppContext createContext() {
        return new AppContext();
    }

    protected BasicWebAppService initService() {
        BasicWebAppService service = Actors.asActor(BasicWebAppService.class);
        service.init( new TCPConnectable(ServiceRegistry.class, options.getGravityHost(), options.getGravityPort() ), options, true).await();
        return service;
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

    protected List<Class> getJsonMsgClasses() {
        return Arrays.asList(
            // fill in classes used in js-client communication to avoid full qualified classnames in protocol
        );
    }

    // static sstartup + init
    protected static Class[] buildMsgClazzlist(BasicWebApp app) {
        List<Class> tmpClz = Arrays.asList(
            AddMessage.class, RemoveMessage.class, UpdateMessage.class,
            QueryDoneMessage.class, Record.class, MapRecord.class
        );
        tmpClz.addAll( app.getJsonMsgClasses() );
        return tmpClz.toArray(new Class[0]);
    }

    public static BasicWebApp realMain(String[] args) throws IOException {

        DispatcherThread.DUMP_CATCHED = true;

        File root = new File("../src/main/web/client"); // expect to run with "run" as working dir

        if ( ! new File(root,"index.html").exists() ) {
            System.out.println("Please run with working dir: '[..]/run");
            System.exit(-1);
        }

        options = (BasicWebAppArgs) ServiceRegistry.parseCommandLine(args, new BasicWebAppArgs());

        // start actor
        BasicWebApp app = asActor(BasicWebApp.class);
        app.init().await(30000);

        Log.sInfo(null, "Run on " + options.webHost + ":" + options.webPort);
        BldFourK builder = Http4K.Build(options.webHost, options.webPort)
            .fileRoot("/files", "./files").httpCachedEnabled(options.prod)
            .httpHandler("/rawhttp", new HttpSyncActorAdaptorHandler(app))
            .resourcePath("/")
                .elements(
                    "../src/main/web/client",
                    "../src/main/web/lib",
                    "../src/main/web/bower_components"
                )
//                .transpile("js", new CoffeeScriptTranspiler())
                .allDev(!options.prod)
                .build()
            .httpAPI("/ep", app)
                .coding(new Coding(SerializerType.JsonNoRef, buildMsgClazzlist(app)))
                .setSessionTimeout(5 * TimeUnit.MINUTES.toMillis(5))
                .build();

        builder.build();

        // debug
//        RemoteRegistry.remoteCallMapper = (reg,ce) -> {
//            RemoteRegistry registry = (RemoteRegistry) reg;
//            String name = registry.getConf().getStreamCoderFactory().getClass().getName();
//            if ( name.indexOf("JSon") >= 0 ) {
//                System.out.println("CALL:"+ce); return ce;
//            }
//            return ce;
//        };
        return app;
    }

}
