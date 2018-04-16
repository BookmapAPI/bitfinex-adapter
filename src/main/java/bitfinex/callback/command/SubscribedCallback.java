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
package bitfinex.callback.command;

import bitfinex.BitfinexApiBroker;
import bitfinex.entity.APIException;
import bitfinex.entity.BitfinexExecutedTradeSymbol;
import bitfinex.entity.OrderbookConfiguration;
import bitfinex.entity.RawOrderbookConfiguration;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscribedCallback implements CommandCallbackHandler {

    final static Logger logger = LoggerFactory.getLogger(SubscribedCallback.class);

    @Override
    public void handleChannelData(final BitfinexApiBroker bitfinexApiBroker,
                                  final JsonObject jsonObject) throws APIException {

        final String channel = jsonObject.get("channel").getAsString();
        final int channelId = jsonObject.get("chanId").getAsInt();

        switch (channel) {
            case "trades":
                handleTradesCallback(bitfinexApiBroker, jsonObject, channelId);
                break;
            case "book":
                handleBookCallback(bitfinexApiBroker, jsonObject, channelId);
                break;
            default:
                logger.error("Unknown subscribed callback {}", jsonObject.toString());
        }
    }

    private void handleBookCallback(final BitfinexApiBroker bitfinexApiBroker, final JsonObject jsonObject,
                                    final int channelId) {

        if ("R0".equals(jsonObject.get("prec").getAsString())) {
            final RawOrderbookConfiguration configuration
                    = RawOrderbookConfiguration.fromJSON(jsonObject);
            logger.info("Registering raw book {} on channel {}", jsonObject, channelId);
            bitfinexApiBroker.addToChannelSymbolMap(channelId, configuration);
        } else {
            final OrderbookConfiguration configuration
                    = OrderbookConfiguration.fromJSON(jsonObject);
            logger.info("Registering book {} on channel {}", jsonObject, channelId);
            bitfinexApiBroker.addToChannelSymbolMap(channelId, configuration);
        }
    }

    private void handleTradesCallback(final BitfinexApiBroker bitfinexApiBroker, final JsonObject jsonObject,
                                      final int channelId) {

        final String symbol2 = jsonObject.get("symbol").getAsString();
        final BitfinexExecutedTradeSymbol currencyPair = BitfinexExecutedTradeSymbol.fromBitfinexString(symbol2);
        logger.info("Registering symbol {} on channel {}", currencyPair, channelId);
        bitfinexApiBroker.addToChannelSymbolMap(channelId, currencyPair);
    }
}
