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
package bitfinex.manager;

import bitfinex.BitfinexApiBroker;
import bitfinex.commands.SubscribeOrderbookCommand;
import bitfinex.commands.UnsubscribeChannelCommand;
import bitfinex.entity.APIException;
import bitfinex.entity.OrderbookConfiguration;
import bitfinex.entity.OrderbookEntry;

import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

public class OrderbookManager {

    private final BiConsumerCallbackManager<OrderbookConfiguration, OrderbookEntry> channelCallbacks;

    private final ExecutorService executorService;

    private final BitfinexApiBroker bitfinexApiBroker;

    public OrderbookManager(final BitfinexApiBroker bitfinexApiBroker) {
        this.bitfinexApiBroker = bitfinexApiBroker;
        this.executorService = bitfinexApiBroker.getExecutorService();
        this.channelCallbacks = new BiConsumerCallbackManager<>(executorService);
    }

    public void registerOrderbookCallback(final OrderbookConfiguration orderbookConfiguration,
                                          final BiConsumer<OrderbookConfiguration, OrderbookEntry> callback) {

        channelCallbacks.registerCallback(orderbookConfiguration, callback);
    }

    public boolean removeOrderbookCallback(final OrderbookConfiguration orderbookConfiguration,
                                           final BiConsumer<OrderbookConfiguration, OrderbookEntry> callback) throws APIException {

        return channelCallbacks.removeCallback(orderbookConfiguration, callback);
    }

    public void subscribeOrderbook(final OrderbookConfiguration orderbookConfiguration) {

        final SubscribeOrderbookCommand subscribeOrderbookCommand
                = new SubscribeOrderbookCommand(orderbookConfiguration);

        bitfinexApiBroker.sendCommand(subscribeOrderbookCommand);
    }

    public void unsubscribeOrderbook(final OrderbookConfiguration orderbookConfiguration) throws APIException {

        final int channel = bitfinexApiBroker.getChannelForSymbol(orderbookConfiguration);

        if (channel == -1) {
            throw new IllegalArgumentException("Unknown symbol: " + orderbookConfiguration);
        }

        final UnsubscribeChannelCommand command = new UnsubscribeChannelCommand(channel);
        bitfinexApiBroker.sendCommand(command);
        bitfinexApiBroker.removeChannelForSymbol(orderbookConfiguration);
        channelCallbacks.clearCallBacks(orderbookConfiguration);
    }

    public void handleNewOrderbookEntry(final OrderbookConfiguration configuration,
                                        final OrderbookEntry entry) {

        channelCallbacks.handleEvent(configuration, entry);
    }
}
