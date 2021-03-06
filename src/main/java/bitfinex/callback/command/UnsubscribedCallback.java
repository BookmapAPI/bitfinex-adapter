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
import bitfinex.entity.BitfinexStreamSymbol;
import com.google.gson.JsonObject;
import velox.api.layer1.common.Log;

public class UnsubscribedCallback implements CommandCallbackHandler {

    @Override
    public void handleChannelData(final BitfinexApiBroker bitfinexApiBroker, final JsonObject jsonObject)
            throws APIException {

        final int channelId = jsonObject.get("chanId").getAsInt();
        final BitfinexStreamSymbol symbol = bitfinexApiBroker.getFromChannelSymbolMap(channelId);
        Log.info("Channel " + channelId + " (" + symbol + ") is unsubscribed");

        bitfinexApiBroker.removeChannel(channelId);
    }
}
