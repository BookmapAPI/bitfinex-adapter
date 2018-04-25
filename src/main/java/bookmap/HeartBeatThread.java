package bookmap;

import bitfinex.BitfinexApiBroker;
import bitfinex.WebsocketClientEndpoint;
import bitfinex.commands.PingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import velox.api.layer1.common.Log;

import java.util.concurrent.TimeUnit;

public class HeartBeatThread implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(HeartBeatThread.class);

    private BitfinexApiBroker bitfinexApiBroker;

    private static final long CONNECTION_TIMEOUT = TimeUnit.SECONDS.toMillis(60);

    public HeartBeatThread(BitfinexApiBroker bitfinexApiBroker) {
        this.bitfinexApiBroker = bitfinexApiBroker;
    }

    @Override
    public void run() {
        try {
            while (!Thread.interrupted()) {
                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
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
            }

        } catch (Exception e) {
            logger.debug("heartbeat thread exception", e);
            Thread.currentThread().interrupt();
            return;
        }
    }

    private boolean checkConnectionTimeout() {
        long heartbeatTimeout = bitfinexApiBroker.getLastMessageTime().get() + CONNECTION_TIMEOUT;

        if (heartbeatTimeout < System.currentTimeMillis()) {
            logger.error("Heartbeat timeout reconnecting");
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
