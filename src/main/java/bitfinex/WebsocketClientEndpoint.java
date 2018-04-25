package bitfinex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import velox.api.layer1.common.Log;

import javax.websocket.*;
import javax.websocket.CloseReason.CloseCodes;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

@ClientEndpoint
public class WebsocketClientEndpoint implements Closeable {

    private Session userSession = null;

    private final List<Consumer<String>> callbackConsumer = new CopyOnWriteArrayList<>();

    private final URI endpointURI;

    private final static Logger logger = LoggerFactory.getLogger(WebsocketClientEndpoint.class);

    public WebsocketClientEndpoint(final URI endpointURI) {
        this.endpointURI = endpointURI;
    }

    /**
     * Open a new connection and wait until connection is ready
     *
     * @throws DeploymentException
     * @throws IOException
     * @throws InterruptedException
     */
    public void connect() throws DeploymentException, IOException, InterruptedException {
        final WebSocketContainer container = ContainerProvider.getWebSocketContainer();
        this.userSession = container.connectToServer(this, endpointURI);
    }

    @OnError
    public void onError(final Session userSession, Throwable throwable) {
        Log.error("Error occurred", throwable);
    }

    @OnOpen
    public void onOpen(final Session userSession) {
        logger.info("Websocket is now open");
    }

    @OnClose
    public void onClose(final Session userSession, final CloseReason reason) {
        logger.info("Closing websocket: {}", reason);
    }

    @OnMessage(maxMessageSize = 1048576)
    public void onMessage(final String message) {
        callbackConsumer.forEach((c) -> c.accept(message));
    }

    /**
     * Send a new message to the server
     *
     * @param message
     */
    public void sendMessage(final String message) {

        if (userSession == null) {
            logger.error("Unable to send message, user session is null");
            return;
        }

        if (userSession.getAsyncRemote() == null) {
            logger.error("Unable to send message, async remote is null");
            return;
        }

        userSession.getAsyncRemote().sendText(message);
    }

    /**
     * Add a new connection data consumer
     *
     * @param consumer
     */
    public void addConsumer(final Consumer<String> consumer) {
        callbackConsumer.add(consumer);
    }

    /**
     * Remove a connection data consumer
     *
     * @param consumer
     * @return
     */
    public boolean removeConsumer(final Consumer<String> consumer) {
        return callbackConsumer.remove(consumer);
    }

    /**
     * Close the connection
     */
    @Override
    public void close() {
        if (userSession == null) {
            return;
        }

        try {
            userSession.close(new CloseReason(CloseCodes.NORMAL_CLOSURE, "Socket closed"));
        } catch (Throwable e) {
            logger.error("Got exception while closing socket", e);
        }
    }

    /**
     * Is this websocket connected
     *
     * @return
     */
    public boolean isConnected() {
        return userSession.isOpen();
    }
}