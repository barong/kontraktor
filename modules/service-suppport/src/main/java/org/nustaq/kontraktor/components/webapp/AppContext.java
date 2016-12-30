package org.nustaq.kontraktor.components.webapp;

import org.nustaq.kontraktor.services.base.ClusterCfg;
import org.nustaq.kontraktor.services.base.rlclient.DataClient;

/**
 * Created by ruedi on 30.12.16.
 */ // global context shared ammongst sessions
// allowed to contain thread safe objects only !
public class AppContext {
    public BasicWebAppService service;
    public DataClient dclient;
    public ClusterCfg cfg;
}
