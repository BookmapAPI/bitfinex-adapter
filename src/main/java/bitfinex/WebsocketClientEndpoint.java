package bitfinex;

import velox.api.layer1.common.Log;
import velox.api.layer1.providers.helper.RawDataHelper;

import javax.websocket.*;
import javax.websocket.CloseReason.CloseCodes;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@ClientEndpoint
public class WebsocketClientEndpoint implements Closeable {

    private Session userSession = null;

    /** Called on incoming message */
    private final List<Consumer<String>> callbackConsumer = new CopyOnWriteArrayList<>();
    /** Called on any interaction, regardless of direction. Useful for debug */
    private final List<Consumer<String>> rawDataConsumer = new CopyOnWriteArrayList<>();

    private final URI endpointURI;
    
    /**
     * Writes what was requested sequentially. Not needed with Jetty, but needed with Tomcat.
     * Tomcat will throw an exception if new asynchronous write is started before previous one completes,
     * so using asynchronous writes becomes problematic.
     * */
    private ExecutorService writeExecutorService = Executors.newSingleThreadExecutor();

    public WebsocketClientEndpoint(final URI endpointURI) {
        this.endpointURI = endpointURI;
    }

    /**
     * Open a new connection and wait until connection is ready
     *
     * @throws DeploymentException
     * @throws IOException
     */
    public void connect() throws DeploymentException, IOException {
        final WebSocketContainer container = ContainerProvider.getWebSocketContainer();
        this.userSession = container.connectToServer(this, endpointURI);
        
        if (RawDataHelper.isRawDataRecordingEnabled()) {
            rawDataConsumer.forEach(e -> e.accept("connect " + endpointURI));
        }
    }

    @OnError
    public void onError(final Session userSession, Throwable throwable) {
        Log.error("Error occurred", throwable);
        if (RawDataHelper.isRawDataRecordingEnabled()) {
            String rawData = "onError " + ExceptionUtils.getStackTrace(throwable);
            rawDataConsumer.forEach(e -> e.accept(rawData));
        }
    }

    @OnOpen
    public void onOpen(final Session userSession) {
        Log.info("Websocket is now open");
        if (RawDataHelper.isRawDataRecordingEnabled()) {
            rawDataConsumer.forEach(e -> e.accept("onOpen"));
        }
    }

    @OnClose
    public void onClose(final Session userSession, final CloseReason reason) {
        Log.info("Closing websocket: " + reason);
        if (RawDataHelper.isRawDataRecordingEnabled()) {
            rawDataConsumer.forEach(e -> e.accept("onClose " + reason));
        }
    }

    @OnMessage(maxMessageSize = 1048576)
    public void onMessage(final String message) {
        //Log.info("message: " + message);
        callbackConsumer.forEach((c) -> c.accept(message));
        if (RawDataHelper.isRawDataRecordingEnabled()) {
            rawDataConsumer.forEach(e -> e.accept("<-" + message));
        }
    }

    /**
     * Send a new message to the server
     *
     * @param message
     */
    public void sendMessage(final String message) {

        if (RawDataHelper.isRawDataRecordingEnabled()) {
            rawDataConsumer.forEach(e -> e.accept("->" + message));
        }
        
        if (userSession == null) {
            Log.error("Unable to send message, user session is null");
            return;
        }

        if (userSession.getAsyncRemote() == null) {
            Log.error("Unable to send message, async remote is null");
            return;
        }

        writeExecutorService.submit(() -> {
            try {
                userSession.getBasicRemote().sendText(message);
            } catch (IOException e) {
                Log.warn("Failed to send data", e);
            }
        });
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
     * Add a raw data listener
     *
     * @see RawDataHelper
     * @param listener
     */
    public void addRawDataCallback(final Consumer<String> listener) {
        rawDataConsumer.add(listener);
    }

    /**
     * Remove a raw data listener
     *
     * @see RawDataHelper
     * @param listener
     * @return
     */
    public boolean removeRawDataCallback(final Consumer<String> listener) {
        return rawDataConsumer.remove(listener);
    }
    
    /**
     * Close the connection
     */
    @Override
    public void close() {
        if (userSession == null) {
            return;
        }
        
        if (RawDataHelper.isRawDataRecordingEnabled()) {
            rawDataConsumer.forEach(e -> e.accept("close"));
        }

        try {
            userSession.close(new CloseReason(CloseCodes.NORMAL_CLOSURE, "Socket closed"));
        } catch (Throwable e) {
            Log.error("Got exception while closing socket", e);
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