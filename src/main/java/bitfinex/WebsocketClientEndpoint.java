package bitfinex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.websocket.*;
import javax.websocket.CloseReason.CloseCodes;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

@ClientEndpoint
public class WebsocketClientEndpoint implements Closeable {

    /**
     * The user session
     */
    private Session userSession = null;

    /**
     * The callback consumer
     */
    private final List<Consumer<String>> callbackConsumer;

    /**
     * The wait for connection latch
     */
    private final CountDownLatch connectLatch = new CountDownLatch(1);

    /**
     * The endpoint URL
     */
    private final URI endpointURI;

    /**
     * The Logger
     */
    private final static Logger logger = LoggerFactory.getLogger(WebsocketClientEndpoint.class);

    public WebsocketClientEndpoint(final URI endpointURI) {
        this.endpointURI = endpointURI;
        this.callbackConsumer = new ArrayList<>();
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
        connectLatch.await();
    }

    @OnOpen
    public void onOpen(final Session userSession) {
        logger.info("Websocket is now open");
        connectLatch.countDown();
    }

    @OnClose
    public void onClose(final Session userSession, final CloseReason reason) {
        logger.info("Closing websocket: {}", reason);
        this.userSession = null;
    }

    @OnMessage(maxMessageSize = 1048576)
    public void onMessage(final String message) {

        // Execute callbacks in another thread
        synchronized (callbackConsumer) {
            callbackConsumer.forEach((c) -> c.accept(message));
        }
    }

    @OnError
    public void onError(final Session session, final Throwable t) {
        logger.error("OnError called {}", t);
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
        synchronized (callbackConsumer) {
            callbackConsumer.add(consumer);
        }
    }

    /**
     * Remove a connection data consumer
     *
     * @param consumer
     * @return
     */
    public boolean removeConsumer(final Consumer<String> consumer) {
        synchronized (callbackConsumer) {
            return callbackConsumer.remove(consumer);
        }
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

        userSession = null;
    }

    /**
     * Is this websocket connected
     *
     * @return
     */
    public boolean isConnected() {
        return userSession != null;
    }
}