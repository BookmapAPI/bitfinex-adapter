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
package bitfinex.commands;

import bitfinex.BitfinexApiBroker;
import com.google.gson.JsonObject;

public class UnsubscribeChannelCommand extends AbstractAPICommand {

    private final int channel;

    public UnsubscribeChannelCommand(final int channel) {
        this.channel = channel;
    }

    @Override
    public String getCommand(final BitfinexApiBroker bitfinexApiBroker) {
        final JsonObject subscribeJson = new JsonObject();
        subscribeJson.addProperty("event", "unsubscribe");
        subscribeJson.addProperty("chanId", channel);

        return subscribeJson.toString();
    }

}
