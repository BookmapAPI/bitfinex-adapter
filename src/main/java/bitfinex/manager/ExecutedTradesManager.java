package bitfinex.manager;

import bitfinex.BitfinexApiBroker;
import bitfinex.commands.SubscribeTradesCommand;
import bitfinex.commands.UnsubscribeChannelCommand;
import bitfinex.entity.APIException;
import bitfinex.entity.BitfinexExecutedTradeSymbol;
import bitfinex.entity.ExecutedTrade;

import java.util.function.BiConsumer;

public class ExecutedTradesManager {
    private final BiConsumerCallbackManager<BitfinexExecutedTradeSymbol, ExecutedTrade> tradesCallbacks;

    private final BitfinexApiBroker bitfinexApiBroker;

    public ExecutedTradesManager(final BitfinexApiBroker bitfinexApiBroker) {
        this.bitfinexApiBroker = bitfinexApiBroker;
        this.tradesCallbacks = new BiConsumerCallbackManager<>();
    }

    public void registerTradeCallback(final BitfinexExecutedTradeSymbol tradeSymbol,
                                      final BiConsumer<BitfinexExecutedTradeSymbol, ExecutedTrade> callback) {

        tradesCallbacks.registerCallback(tradeSymbol, callback);
    }

    public boolean removeTradeCallback(final BitfinexExecutedTradeSymbol tradeSymbol,
                                       final BiConsumer<BitfinexExecutedTradeSymbol, ExecutedTrade> callback) throws APIException {

        return tradesCallbacks.removeCallback(tradeSymbol, callback);
    }

    public void subscribeExecutedTrades(final BitfinexExecutedTradeSymbol tradeSymbol) {

        final SubscribeTradesCommand subscribeTradesCommand
                = new SubscribeTradesCommand(tradeSymbol);

        bitfinexApiBroker.sendCommand(subscribeTradesCommand);
    }

    public void unsubscribeExecutedTrades(final BitfinexExecutedTradeSymbol tradeSymbol) throws APIException {

        final int channel = bitfinexApiBroker.getChannelForSymbol(tradeSymbol);

        if (channel == -1) {
            throw new IllegalArgumentException("Unknown symbol: " + tradeSymbol);
        }

        final UnsubscribeChannelCommand command = new UnsubscribeChannelCommand(channel);
        bitfinexApiBroker.sendCommand(command);
        bitfinexApiBroker.removeChannelForSymbol(tradeSymbol);
        tradesCallbacks.clearCallBacks(tradeSymbol);
    }

    public void handleExecutedTradeEntry(final BitfinexExecutedTradeSymbol tradeSymbol,
                                         final ExecutedTrade entry) {

        tradesCallbacks.handleEvent(tradeSymbol, entry);
    }
}
