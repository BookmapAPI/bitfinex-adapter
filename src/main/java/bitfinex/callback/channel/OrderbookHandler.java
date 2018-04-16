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
package bitfinex.callback.channel;

import bitfinex.BitfinexApiBroker;
import bitfinex.entity.APIException;
import bitfinex.entity.BitfinexStreamSymbol;
import bitfinex.entity.OrderbookConfiguration;
import bitfinex.entity.OrderbookEntry;
import com.google.gson.JsonArray;

import java.math.BigDecimal;

public class OrderbookHandler implements ChannelCallbackHandler {

    @Override
    public void handleChannelData(final BitfinexApiBroker bitfinexApiBroker,
                                  final BitfinexStreamSymbol channelSymbol, final JsonArray jsonArray) throws APIException {

        final OrderbookConfiguration configuration = (OrderbookConfiguration) channelSymbol;

        // Example: [13182,1,-0.1]

        // Snapshots contain multiple Orderbook entries, updates only one
        if (jsonArray.get(0) instanceof JsonArray) {
            for (int pos = 0; pos < jsonArray.size(); pos++) {
                final JsonArray parts = jsonArray.get(pos).getAsJsonArray();
                handleEntry(bitfinexApiBroker, configuration, parts);
            }
        } else {
            handleEntry(bitfinexApiBroker, configuration, jsonArray);
        }


    }

    private void handleEntry(final BitfinexApiBroker bitfinexApiBroker,
                             final OrderbookConfiguration configuration,
                             final JsonArray jsonArray) {

        final BigDecimal price = jsonArray.get(0).getAsBigDecimal();
        final BigDecimal count = jsonArray.get(1).getAsBigDecimal();
        final BigDecimal amount = jsonArray.get(2).getAsBigDecimal();

        final OrderbookEntry orderbookEntry = new OrderbookEntry(price, count, amount);

        bitfinexApiBroker.getOrderbookManager().handleNewOrderbookEntry(configuration, orderbookEntry);
    }

}
