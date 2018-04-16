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

import bitfinex.entity.APIException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

public class BiConsumerCallbackManager<S, T> {

    private final Map<S, List<BiConsumer<S, T>>> callbacks;

    private final ExecutorService executorService;

    public BiConsumerCallbackManager(final ExecutorService executorService) {
        this.executorService = executorService;
        this.callbacks = new HashMap<>();
    }

    public void registerCallback(final S symbol, final BiConsumer<S, T> callback) {

        callbacks.putIfAbsent(symbol, new ArrayList<>());

        final List<BiConsumer<S, T>> callbackList = callbacks.get(symbol);

        synchronized (callbackList) {
            callbackList.add(callback);
        }
    }

    public void clearCallBacks(final S symbol) throws APIException {

        if (!callbacks.containsKey(symbol)) {
            throw new APIException("Unknown ticker string: " + symbol);
        }

        final List<BiConsumer<S, T>> callbackList = callbacks.get(symbol);

        synchronized (callbackList) {
            callbackList.clear();
        }
    }

    public boolean removeCallback(final S symbol, final BiConsumer<S, T> callback) throws APIException {

        if (!callbacks.containsKey(symbol)) {
            throw new APIException("Unknown ticker string: " + symbol);
        }

        final List<BiConsumer<S, T>> callbackList = callbacks.get(symbol);

        synchronized (callbackList) {
            return callbackList.remove(callback);
        }
    }

    public void handleEventsList(final S symbol, final List<T> elements) {

        final List<BiConsumer<S, T>> callbackList = callbacks.get(symbol);

        if (callbackList == null) {
            return;
        }

        synchronized (callbackList) {
            if (callbackList.isEmpty()) {
                return;
            }

            // Notify callbacks synchronously, to preserve the order of events
            for (final T element : elements) {
                callbackList.forEach((c) -> {
                    c.accept(symbol, element);
                });
            }
        }
    }

    public void handleEvent(final S symbol, final T element) {

        final List<BiConsumer<S, T>> callbackList = callbacks.get(symbol);

        if (callbackList == null) {
            return;
        }

        synchronized (callbackList) {
            if (callbackList.isEmpty()) {
                return;
            }

            callbackList.forEach((c) -> {
                final Runnable runnable = () -> c.accept(symbol, element);
                executorService.submit(runnable);
            });
        }
    }

}
