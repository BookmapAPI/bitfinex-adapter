package bookmap;

import bitfinex.BitfinexApiBroker;
import bitfinex.WebsocketClientEndpoint;
import bitfinex.commands.PingCommand;
import velox.api.layer1.common.Log;

import java.util.concurrent.TimeUnit;

/**
 * Thread that is responsible to keep connection opened. Sends ping commands and checks last response time. Preforms reconnects.
 */
public class HeartBeatThread extends Thread {

    private BitfinexApiBroker bitfinexApiBroker;

    private static final long CONNECTION_TIMEOUT = TimeUnit.SECONDS.toMillis(35);

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
                    Log.debug("We are not connected, reconnecting");
                    executeReconnect();
                    continue;
                }

                ping();

                boolean reconnectNeeded = checkConnectionTimeout();

                if (reconnectNeeded) {
                    Log.debug("Connection heartbeat time out, reconnecting");
                    executeReconnect();
                }
            } catch (InterruptedException e) {
                Log.error("heartbeat thread interrupted", e);
            }
        }
    }


    public void shutDown() {
        stopped = true;
    }

    private boolean checkConnectionTimeout() {
        long heartbeatTimeout = bitfinexApiBroker.getLastMessageTime().get() + CONNECTION_TIMEOUT;
        return heartbeatTimeout < System.currentTimeMillis();
    }

    private void ping() {
        Log.debug("HeartBeat thread. Sending ping command.");
        bitfinexApiBroker.sendCommand(new PingCommand());
    }

    private void executeReconnect() {
        Log.info("Trying to reconnect");
        boolean reconnected = bitfinexApiBroker.reconnect();
        if (reconnected) Log.info("Successfully reconnected");
    }
}
