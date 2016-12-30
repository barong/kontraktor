package org.nustaq.kontraktor.components.webapp;

import org.nustaq.kontraktor.services.base.ServiceActor;
import org.nustaq.kontraktor.services.base.ServiceDescription;

/**
 * Created by ruedi on 30.12.16.
 */
public class BasicWebAppService extends ServiceActor<BasicWebAppService> {

    @Override
    protected String[] getRequiredServiceNames() {
        return new String[0];
    }

    @Override
    protected ServiceDescription createServiceDescription() {
        return new ServiceDescription("WebAppServer");
    }
}
