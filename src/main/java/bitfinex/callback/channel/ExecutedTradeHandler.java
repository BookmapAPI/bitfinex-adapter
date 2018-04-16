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
import bitfinex.entity.BitfinexExecutedTradeSymbol;
import bitfinex.entity.BitfinexStreamSymbol;
import bitfinex.entity.ExecutedTrade;
import com.google.gson.JsonArray;

import java.math.BigDecimal;

public class ExecutedTradeHandler implements ChannelCallbackHandler {

    @Override
    public void handleChannelData(final BitfinexApiBroker bitfinexApiBroker,
                                  final BitfinexStreamSymbol channelSymbol, final JsonArray jsonArray) throws APIException {

        final BitfinexExecutedTradeSymbol configuration = (BitfinexExecutedTradeSymbol) channelSymbol;

        // Snapshots contain multiple executes entries, updates only one
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
                             final BitfinexExecutedTradeSymbol symbol,
                             final JsonArray jsonArray) {

        final ExecutedTrade executedTrade = new ExecutedTrade();

        final long id = jsonArray.get(0).getAsLong();
        executedTrade.setId(id);

        final long timestamp = jsonArray.get(1).getAsLong();
        executedTrade.setTimestamp(timestamp);

        final BigDecimal amount = jsonArray.get(2).getAsBigDecimal();
        executedTrade.setAmount(amount);

        // Funding or Currency
        if (jsonArray.size() > 4) {
            final BigDecimal rate = jsonArray.get(3).getAsBigDecimal();
            executedTrade.setRate(rate);

            final int period = jsonArray.get(4).getAsInt();
            executedTrade.setPeriod(period);
        } else {
            final BigDecimal price = jsonArray.get(3).getAsBigDecimal();
            executedTrade.setPrice(price);
        }

        bitfinexApiBroker.getExecutedTradesManager().handleExecutedTradeEntry(symbol, executedTrade);
    }
}
