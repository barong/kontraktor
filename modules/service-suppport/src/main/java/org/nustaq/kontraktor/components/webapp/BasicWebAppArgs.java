package org.nustaq.kontraktor.components.webapp;

import com.beust.jcommander.Parameter;
import org.nustaq.kontraktor.services.base.ServiceArgs;

/**
 * Created by ruedi on 30.12.16.
 */
public class BasicWebAppArgs extends ServiceArgs {

        @Parameter( names = "-webhost")
        String webHost = "localhost";
        @Parameter( names = "-webport")
        int webPort = 8888;
        @Parameter( names = "-prod")
        Boolean prod = false;

}
