/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 Jan Kristof Nidzwetzki
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License. 
 *
 *******************************************************************************/
package bitfinex;

import bitfinex.callback.channel.ChannelCallbackHandler;
import bitfinex.callback.channel.ExecutedTradeHandler;
import bitfinex.callback.channel.OrderbookHandler;
import bitfinex.callback.channel.RawOrderbookHandler;
import bitfinex.callback.command.CommandCallbackHandler;
import bitfinex.callback.command.DoNothingCommandCallback;
import bitfinex.callback.command.SubscribedCallback;
import bitfinex.callback.command.UnsubscribedCallback;
import bitfinex.commands.*;
import bitfinex.entity.*;
import bitfinex.manager.ExecutedTradesManager;
import bitfinex.manager.OrderbookManager;
import bitfinex.manager.RawOrderbookManager;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import velox.api.layer1.common.Log;

import javax.websocket.DeploymentException;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class BitfinexApiBroker implements Closeable {

    public final static String BITFINEX_URI = "wss://api.bitfinex.com/ws/2";

    private final Consumer<String> apiCallback = ((message) -> websocketCallback(message));

    private WebsocketClientEndpoint websocketEndpoint;

    private final Map<Integer, BitfinexStreamSymbol> channelIdSymbolMap;

    private final OrderbookManager orderbookManager;

    private final ExecutedTradesManager executedTradesManager;

    private final RawOrderbookManager rawOrderbookManager;

    private Map<String, CommandCallbackHandler> commandCallbacks;

    private AtomicLong lastMessageTime = new AtomicLong();

    public BitfinexApiBroker() {
        this.channelIdSymbolMap = new HashMap<>();
        this.orderbookManager = new OrderbookManager(this);
        this.rawOrderbookManager = new RawOrderbookManager(this);
        this.executedTradesManager = new ExecutedTradesManager(this);

        setupCommandCallbacks();
    }

    private void setupCommandCallbacks() {
        commandCallbacks = new HashMap<>();
        commandCallbacks.put("info", new DoNothingCommandCallback());
        commandCallbacks.put("subscribed", new SubscribedCallback());
        commandCallbacks.put("unsubscribed", new UnsubscribedCallback());
    }

    public void connect() throws APIException {
        try {
            final URI bitfinexURI = new URI(BITFINEX_URI);
            websocketEndpoint = new WebsocketClientEndpoint(bitfinexURI);
            websocketEndpoint.addConsumer(apiCallback);
            websocketEndpoint.connect();
        } catch (Exception e) {
            throw new APIException(e);
        }
    }

    @Override
    public void close() {
        if (websocketEndpoint != null) {
            websocketEndpoint.removeConsumer(apiCallback);
            websocketEndpoint.close();
            websocketEndpoint = null;
        }
    }

    public void sendCommand(final AbstractAPICommand apiCommand) {
        try {
            final String command = apiCommand.getCommand(this);
            Log.debug("Sending to server: " + command);
            websocketEndpoint.sendMessage(command);
        } catch (CommandException e) {
            Log.error("Got Exception while sending command", e);
        }
    }

    public WebsocketClientEndpoint getWebsocketEndpoint() {
        return websocketEndpoint;
    }

    private void websocketCallback(final String message) {
        Log.debug("Got message: " + message);
        updateLastMessageTime();
        if (message.startsWith("{")) {
            handleCommandCallback(message);
        } else if (message.startsWith("[")) {
            handleChannelCallback(message);
        } else {
            Log.error("Got unknown callback: " + message);
        }
    }

    private void updateLastMessageTime() {
        lastMessageTime.set(System.currentTimeMillis());
    }

    private void handleCommandCallback(final String message) {
        // JSON callback
        final JsonObject jsonObject = new JsonParser().parse(message).getAsJsonObject();

        final String eventType = jsonObject.get("event").getAsString();

        if (commandCallbacks.containsKey(eventType)) {
            try {
                final CommandCallbackHandler callback = commandCallbacks.get(eventType);
                callback.handleChannelData(this, jsonObject);
            } catch (APIException e) {
                Log.error("Got an exception while handling callback", e);
            }
        }
    }

    public void removeChannel(final int channelId) {
        synchronized (channelIdSymbolMap) {
            channelIdSymbolMap.remove(channelId);
            channelIdSymbolMap.notifyAll();
        }
    }

    public void addToChannelSymbolMap(final int channelId, final BitfinexStreamSymbol symbol) {
        synchronized (channelIdSymbolMap) {
            channelIdSymbolMap.put(channelId, symbol);
            channelIdSymbolMap.notifyAll();
        }
    }

    protected void handleChannelCallback(final String message) {
        // Channel callback
        Log.debug("Channel callback");

        // JSON callback
        final JsonArray jsonArray = new JsonParser().parse(message).getAsJsonArray();

        final int channel = jsonArray.get(0).getAsInt();

        if (channel == 0) {
            Log.debug("signal message: " + message);
        } else {
            handleChannelData(jsonArray);
        }
    }

    private void handleChannelData(final JsonArray jsonArray) {
        final int channel = jsonArray.get(0).getAsInt();
        final BitfinexStreamSymbol channelSymbol = getFromChannelSymbolMap(channel);

        if (channelSymbol == null) {
            Log.debug("Unable to determine symbol for channel " + channel);
            Log.debug("Data is " + jsonArray);
            return;
        }

        try {
            if (jsonArray.get(1).isJsonArray()) {
                handleChannelDataArray(jsonArray, channelSymbol);
            } else {
                handleChannelDataString(jsonArray, channelSymbol);
            }
        } catch (APIException e) {
            Log.error("Got exception while handling callback", e);
        }
    }

    private void handleChannelDataString(final JsonArray jsonArray,
                                         final BitfinexStreamSymbol channelSymbol) throws APIException {

        final String value = jsonArray.get(1).getAsString();

        if ("te".equals(value)) {
            final JsonArray subarray = jsonArray.get(2).getAsJsonArray();
            final ChannelCallbackHandler handler = new ExecutedTradeHandler();
            handler.handleChannelData(this, channelSymbol, subarray);
        } else {
            Log.debug("skipping: " + jsonArray);
        }
    }

    private void handleChannelDataArray(final JsonArray jsonArray, final BitfinexStreamSymbol channelSymbol)
            throws APIException {
        final JsonArray subarray = jsonArray.get(1).getAsJsonArray();

        if (channelSymbol instanceof RawOrderbookConfiguration) {
            final RawOrderbookHandler handler = new RawOrderbookHandler();
            handler.handleChannelData(this, channelSymbol, subarray);
        } else if (channelSymbol instanceof OrderbookConfiguration) {
            final OrderbookHandler handler = new OrderbookHandler();
            handler.handleChannelData(this, channelSymbol, subarray);
        } else if (channelSymbol instanceof BitfinexExecutedTradeSymbol) {
            final ChannelCallbackHandler handler = new ExecutedTradeHandler();
            handler.handleChannelData(this, channelSymbol, subarray);
        } else {
            Log.error("Unknown stream type: " + channelSymbol);
        }
    }

    public BitfinexStreamSymbol getFromChannelSymbolMap(final int channel) {
        synchronized (channelIdSymbolMap) {
            return channelIdSymbolMap.get(channel);
        }
    }

    public int getChannelForSymbol(final BitfinexStreamSymbol symbol) {
        synchronized (channelIdSymbolMap) {
            return channelIdSymbolMap.entrySet()
                    .stream()
                    .filter((v) -> v.getValue().equals(symbol))
                    .map(Map.Entry::getKey)
                    .findAny().orElse(-1);
        }
    }

    public boolean removeChannelForSymbol(final BitfinexStreamSymbol symbol) {
        final int channel = getChannelForSymbol(symbol);

        if (channel != -1) {
            synchronized (channelIdSymbolMap) {
                channelIdSymbolMap.remove(channel);
            }

            return true;
        }

        return false;
    }

    public AtomicLong getLastMessageTime() {
        return lastMessageTime;
    }

    public OrderbookManager getOrderbookManager() {
        return orderbookManager;
    }

    public RawOrderbookManager getRawOrderbookManager() {
        return rawOrderbookManager;
    }

    public ExecutedTradesManager getExecutedTradesManager() {
        return executedTradesManager;
    }

    public synchronized boolean reconnect() {
        try {
            websocketEndpoint.close();
            websocketEndpoint.connect();

            resubscribeChannels();
            updateLastMessageTime();

            return true;
        } catch (DeploymentException | IOException e) {
            Log.error("Websocket connection failed", e);
            websocketEndpoint.close();
            return false;
        }
    }

    private void resubscribeChannels() {
        final Map<Integer, BitfinexStreamSymbol> oldChannelIdSymbolMap;

        synchronized (channelIdSymbolMap) {
            oldChannelIdSymbolMap = new HashMap<>(channelIdSymbolMap);
            channelIdSymbolMap.clear();
            channelIdSymbolMap.notifyAll();
        }

        // Resubscribe channels
        for (BitfinexStreamSymbol symbol : oldChannelIdSymbolMap.values()) {
            if (symbol instanceof BitfinexExecutedTradeSymbol) {
                sendCommand(new SubscribeTradesCommand((BitfinexExecutedTradeSymbol) symbol));
            } else if (symbol instanceof OrderbookConfiguration) {
                sendCommand(new SubscribeOrderbookCommand((OrderbookConfiguration) symbol));
            } else if (symbol instanceof RawOrderbookConfiguration) {
                sendCommand(new SubscribeRawOrderbookCommand((RawOrderbookConfiguration) symbol));
            } else {
                Log.error("Unknown stream symbol: " + symbol);
            }
        }
    }
}
