package bookmap;

import bitfinex.BitfinexApiBroker;
import bitfinex.entity.*;
import bitfinex.manager.ExecutedTradesManager;
import velox.api.layer0.live.ExternalLiveBaseProvider;
import velox.api.layer1.Layer1ApiAdminListener;
import velox.api.layer1.data.*;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.BiConsumer;

public class BitfinexRealTimeProvider extends ExternalLiveBaseProvider {

    private BitfinexApiBroker bitfinexApiBroker = new BitfinexApiBroker();

    private final HashSet<String> aliases = new HashSet<>();

    private Map<String, OrderbookConfiguration> orderBookConfigByAlias = new HashMap<>();
    private Map<String, BitfinexExecutedTradeSymbol> tradeSymbolByAlias = new HashMap<>();

    private int AMOUNT_MULTIPLIER = 1000; //bookmap accepts inter amounts, but bitfinex returns float values. we will save 3 digits after coma

    @Override
    public void login(LoginData loginData) {
        try {
            bitfinexApiBroker.connect();
            adminListeners.forEach(Layer1ApiAdminListener::onLoginSuccessful);
        } catch (APIException e) {
            adminListeners.forEach(l -> l.onLoginFailed(LoginFailedReason.FATAL, "Cannot connect to bitfinex API"));
        }
    }

    @Override
    public String formatPrice(String alias, double price) {
        OrderbookConfiguration orderbookConfiguration;
        synchronized (aliases) {
            orderbookConfiguration = orderBookConfigByAlias.get(alias);
        }
        double priceStep = PriceConverter.getPriceStep(orderbookConfiguration);
        return formatPriceDefault(priceStep, price);
    }

    @Override
    public void subscribe(String symbol, String exchange, String type) {
        String alias = createAlias(symbol, exchange, type);
        boolean added;
        synchronized (aliases) {
            added = aliases.add(alias);
            subscribeOrderBook(symbol, exchange, type, alias);
            subscribeExecutedTrades(symbol, exchange, type, alias);
        }
        if (!added) {
            instrumentListeners.forEach(i -> i.onInstrumentAlreadySubscribed(symbol, exchange, type));
        }
    }

    private void subscribeOrderBook(String symbol, String exchange, String type, String alias) {
        OrderbookConfiguration orderbookConfiguration =
                new OrderbookConfiguration(BitfinexCurrencyPair.valueOf(symbol), OrderBookPrecision.P1, OrderBookFrequency.F0, 25);

        double pips = PriceConverter.getPriceStep(orderbookConfiguration);
        final InstrumentInfo instrumentInfo = new InstrumentInfo(symbol, exchange, type, pips, 1, "", false);
        instrumentListeners.forEach(i -> i.onInstrumentAdded(alias, instrumentInfo));

        final BiConsumer<OrderbookConfiguration, OrderbookEntry> orderBookCallback = (orderbookConfig, entry) -> {
            adminListeners.forEach(Layer1ApiAdminListener::onConnectionRestored);
            boolean isBid = entry.getAmount().signum() > 0;
            int multiplier = PriceConverter.getMultiplier(orderbookConfig);
            int price = entry.getPrice().multiply(BigDecimal.valueOf(multiplier)).intValue();
            int amount = entry.getAmount().abs().multiply(BigDecimal.valueOf(AMOUNT_MULTIPLIER)).intValue();
            if (entry.getCount().intValue() != 0) {
                dataListeners.forEach(l -> l.onDepth(alias, isBid, price, amount));
            } else {
                dataListeners.forEach(l -> l.onDepth(alias, isBid, price, 0));
            }
        };
        bitfinexApiBroker.getOrderbookManager().registerOrderbookCallback(orderbookConfiguration, orderBookCallback);
        bitfinexApiBroker.getOrderbookManager().subscribeOrderbook(orderbookConfiguration);
        orderBookConfigByAlias.put(alias, orderbookConfiguration);
    }

    private void subscribeExecutedTrades(String symbol, String exchange, String type, String alias) {
        final BitfinexExecutedTradeSymbol tradeSymbol = new BitfinexExecutedTradeSymbol(BitfinexCurrencyPair.valueOf(symbol));
        final ExecutedTradesManager executedTradesManager = bitfinexApiBroker.getExecutedTradesManager();

        final OrderbookConfiguration orderbookConfiguration = orderBookConfigByAlias.get(alias);

        if (orderbookConfiguration == null) {
            adminListeners.forEach(l -> l.onLoginFailed(LoginFailedReason.FATAL,
                    "Cannot subscribe for trading. Order book config not found."));
            return;
        }

        final BiConsumer<BitfinexExecutedTradeSymbol, ExecutedTrade> tradeCallback = (symb, trade) -> {
            int multiplier = PriceConverter.getMultiplier(orderbookConfiguration);
            double price = trade.getPrice().multiply(BigDecimal.valueOf(multiplier)).doubleValue();
            boolean isOtc = false;
            boolean isBidAgressor = trade.getAmount().signum() > 0;
            int amount = trade.getAmount().abs().multiply(BigDecimal.valueOf(AMOUNT_MULTIPLIER)).intValue();

            dataListeners.forEach(l -> l.onTrade(alias, price, amount, new TradeInfo(isOtc, isBidAgressor)));
        };

        executedTradesManager.registerTradeCallback(tradeSymbol, tradeCallback);
        executedTradesManager.subscribeExecutedTrades(tradeSymbol);

        tradeSymbolByAlias.put(alias, tradeSymbol);
    }

    @Override
    public void unsubscribe(String alias) {
        synchronized (aliases) {
            try {
                bitfinexApiBroker.getOrderbookManager().unsubscribeOrderbook(orderBookConfigByAlias.get(alias));
                bitfinexApiBroker.getExecutedTradesManager().unsubscribeExecutedTrades(tradeSymbolByAlias.get(alias));
                aliases.remove(alias);
                orderBookConfigByAlias.remove(alias);
                tradeSymbolByAlias.remove(alias);
            } catch (APIException e) {
                adminListeners.forEach(l -> l.onLoginFailed(LoginFailedReason.UNKNOWN, e.getMessage()));
            }
        }
    }

    @Override
    public void sendOrder(OrderSendParameters orderSendParameters) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateOrder(OrderUpdateParameters orderUpdateParameters) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSource() {
        return "bitfinex";
    }

    @Override
    public void close() {
        bitfinexApiBroker.close();
    }

    private static String createAlias(String symbol, String exchange, String type) {
        return "Bitfinex/" + symbol;
    }
}
