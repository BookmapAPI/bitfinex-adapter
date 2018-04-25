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
import bitfinex.commands.SubscribeRawOrderbookCommand;
import bitfinex.commands.UnsubscribeChannelCommand;
import bitfinex.entity.APIException;
import bitfinex.entity.RawOrderbookConfiguration;
import bitfinex.entity.RawOrderbookEntry;

import java.util.List;
import java.util.function.BiConsumer;

public class RawOrderbookManager {

    private final BiConsumerCallbackManager<RawOrderbookConfiguration, RawOrderbookEntry> channelCallbacks;
    private final BiConsumerCallbackManager<RawOrderbookConfiguration, List<RawOrderbookEntry>> snapshotCallbacks;

    private final BitfinexApiBroker bitfinexApiBroker;

    public RawOrderbookManager(final BitfinexApiBroker bitfinexApiBroker) {
        this.bitfinexApiBroker = bitfinexApiBroker;
        this.channelCallbacks = new BiConsumerCallbackManager<>();
        this.snapshotCallbacks = new BiConsumerCallbackManager<>();
    }

    public void registerOrderbookCallback(final RawOrderbookConfiguration orderbookConfiguration,
                                          final BiConsumer<RawOrderbookConfiguration, RawOrderbookEntry> callback) {
        channelCallbacks.registerCallback(orderbookConfiguration, callback);
    }

    public void registerOrderbookSnapshotCallback(final RawOrderbookConfiguration orderbookConfiguration,
                                                  final BiConsumer<RawOrderbookConfiguration, List<RawOrderbookEntry>> callback) {
        snapshotCallbacks.registerCallback(orderbookConfiguration, callback);
    }

    public boolean removeOrderbookCallback(final RawOrderbookConfiguration orderbookConfiguration,
                                           final BiConsumer<RawOrderbookConfiguration, RawOrderbookEntry> callback) throws APIException {

        return channelCallbacks.removeCallback(orderbookConfiguration, callback);
    }

    public void subscribeOrderbook(final RawOrderbookConfiguration orderbookConfiguration) {

        final SubscribeRawOrderbookCommand subscribeOrderbookCommand
                = new SubscribeRawOrderbookCommand(orderbookConfiguration);

        bitfinexApiBroker.sendCommand(subscribeOrderbookCommand);
    }

    public void unsubscribeOrderbook(final RawOrderbookConfiguration orderbookConfiguration) throws APIException {

        final int channel = bitfinexApiBroker.getChannelForSymbol(orderbookConfiguration);

        if (channel == -1) {
            throw new IllegalArgumentException("Unknown symbol: " + orderbookConfiguration);
        }

        final UnsubscribeChannelCommand command = new UnsubscribeChannelCommand(channel);
        bitfinexApiBroker.sendCommand(command);
        bitfinexApiBroker.removeChannelForSymbol(orderbookConfiguration);
        channelCallbacks.clearCallBacks(orderbookConfiguration);
    }

    public void handleNewOrderbookEntry(final RawOrderbookConfiguration configuration,
                                        final RawOrderbookEntry entry) {
        channelCallbacks.handleEvent(configuration, entry);
    }

    public void handleOrderbookSnapshot(final RawOrderbookConfiguration configuration,
                                        final List<RawOrderbookEntry> entry) {
        snapshotCallbacks.handleEvent(configuration, entry);
    }
}
