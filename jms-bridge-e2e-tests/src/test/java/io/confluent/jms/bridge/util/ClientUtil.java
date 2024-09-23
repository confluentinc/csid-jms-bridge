package io.confluent.jms.bridge.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.confluent.jms.bridge.util.constants.Constants;

import java.util.HashMap;

public class ClientUtil {
    private static final Logger logger = LogManager.getLogger(ClientUtil.class);
    public static HashMap<String, String> getActiveNode() {
        HashMap<String, String> currentNode = Util.getMasterServer();
        if(Util.isPortOpen(currentNode)) {
            return currentNode;
        }
        logger.debug("Master server is down");
        return Util.getSlaveServer();
    }

    public static String getConnectionUrl(HashMap<String, String> currentNode) {
        if(currentNode == null) {
            return "("+prepareUrl(Util.getMasterServer())+","+prepareUrl(Util.getSlaveServer())+")";
        }
        return prepareUrl(currentNode);
    }


    private static String prepareUrl(HashMap<String, String> currentNode) {
        return "tcp://" + currentNode.get(Constants.HOST) + ":" + currentNode.get(Constants.APP_PORT);
    }
}
