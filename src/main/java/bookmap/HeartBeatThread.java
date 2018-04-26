package bookmap;

import bitfinex.BitfinexApiBroker;
import bitfinex.WebsocketClientEndpoint;
import bitfinex.commands.PingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import velox.api.layer1.common.Log;

import java.util.concurrent.TimeUnit;

public class HeartBeatThread extends Thread {

    private static Logger logger = LoggerFactory.getLogger(HeartBeatThread.class);

    private BitfinexApiBroker bitfinexApiBroker;

    private static final long CONNECTION_TIMEOUT = TimeUnit.SECONDS.toMillis(60);

    private volatile boolean stopped = false;

    public HeartBeatThread(BitfinexApiBroker bitfinexApiBroker) {
        this.bitfinexApiBroker = bitfinexApiBroker;
    }

    @Override
    public void run() {
        while (!stopped && !Thread.interrupted()) {
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(20));
                WebsocketClientEndpoint websocketEndpoint = bitfinexApiBroker.getWebsocketEndpoint();

                if (websocketEndpoint == null) {
                    continue;
                }

                if (!websocketEndpoint.isConnected()) {
                    logger.error("We are not connected, reconnecting");
                    executeReconnect();
                    continue;
                }

                ping();

                boolean reconnectNeeded = checkConnectionTimeout();

                if (reconnectNeeded) {
                    logger.error("Connection heartbeat time out, reconnecting");
                    executeReconnect();
                    continue;
                }
            } catch (Exception e) {
                Log.error("heartbeat thread exception", e);
            }
        }
    }


    public void shutDown() {
        stopped = true;
    }

    private boolean checkConnectionTimeout() {
        long heartbeatTimeout = bitfinexApiBroker.getLastMessageTime().get() + CONNECTION_TIMEOUT;

        if (heartbeatTimeout < System.currentTimeMillis()) {
            return true;
        }

        return false;
    }

    private void ping() {
        logger.debug("HeartBeat thread. Sending ping command.");
        bitfinexApiBroker.sendCommand(new PingCommand());
    }

    private void executeReconnect() {
        Log.info("Trying to reconnect");
        boolean reconnected = bitfinexApiBroker.reconnect();
        if (reconnected) Log.info("Successfully reconnected");
    }
}
