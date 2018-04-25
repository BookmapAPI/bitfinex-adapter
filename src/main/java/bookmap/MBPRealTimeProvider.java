package bookmap;

import bitfinex.BitfinexApiBroker;
import bitfinex.entity.*;
import bitfinex.manager.ExecutedTradesManager;
import velox.api.layer0.live.ExternalLiveBaseProvider;
import velox.api.layer1.Layer1ApiAdminListener;
import velox.api.layer1.data.*;
import velox.api.layer1.layers.utils.OrderBook;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.function.BiConsumer;

public class MBPRealTimeProvider extends ExternalLiveBaseProvider {

    private BitfinexApiBroker bitfinexApiBroker = new BitfinexApiBroker();

    private final HashSet<String> aliases = new HashSet<>();

    private Map<String, OrderbookConfiguration> orderBookConfigByAlias = new HashMap<>();
    private Map<String, BitfinexExecutedTradeSymbol> tradeSymbolByAlias = new HashMap<>();

    private int AMOUNT_MULTIPLIER = (int) 1e4; // bookmap accepts integer amounts, but bitfinex returns float values. we will save 4 digits after coma
    private int AMOUNT_LIMIT_AFTER_MULTIPLICATION = (int) 1e9; // limit is needed to not cause integer overflows

    private static final HashSet<BitfinexCurrencyPair> supportedPairs = new HashSet<>();

    static {
        supportedPairs.add(BitfinexCurrencyPair.BTC_USD);
        supportedPairs.add(BitfinexCurrencyPair.IOT_USD);
    }

    @Override
    public void login(LoginData loginData) {
        try {
            bitfinexApiBroker.connect();
            Thread heartBeatThread = new Thread(new HeartBeatThread(bitfinexApiBroker));
            heartBeatThread.start();
            adminListeners.forEach(Layer1ApiAdminListener::onLoginSuccessful);
        } catch (APIException e) {
            adminListeners.forEach(l -> l.onLoginFailed(LoginFailedReason.FATAL, "Cannot connect to bitfinex API"));
        }
    }

    /**
     * Price should be passed into onDepth method in integer format.
     * Integer value is used to identify the coordinate on price axis
     * and then it is multiplied by pips (price step) to display the actual price.
     * We maintain the same logic for onTrade, even though it uses double value as coordinate on price axis.
     */
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
        if (!BitfinexCurrencyPair.contains(symbol) || !supportedPairs.contains(BitfinexCurrencyPair.valueOf(symbol))) {
            instrumentListeners.forEach(i -> i.onInstrumentNotFound(symbol, exchange, type));
            return;
        }

        String alias = createAlias(symbol, exchange, type);
        boolean added;
        synchronized (aliases) {
            added = aliases.add(alias);
        }
        if (added) {
            subscribeOrderBook(symbol, exchange, type, alias);
            subscribeExecutedTrades(symbol, exchange, type, alias);
        } else {
            instrumentListeners.forEach(i -> i.onInstrumentAlreadySubscribed(symbol, exchange, type));
        }
    }

    private void subscribeOrderBook(String symbol, String exchange, String type, String alias) {
        // P1 precision is chosen because of optimal aggregation level by price. With P0 there are too many gaps on price axis.
        OrderbookConfiguration orderbookConfiguration =
                new OrderbookConfiguration(BitfinexCurrencyPair.valueOf(symbol), OrderBookPrecision.P1, OrderBookFrequency.F0, 100);

        double pips = PriceConverter.getPriceStep(orderbookConfiguration);

        InstrumentInfoCrypto instrumentInfoCrypto = new InstrumentInfoCrypto(symbol, exchange, type, pips, 1, "", AMOUNT_MULTIPLIER);
        instrumentListeners.forEach(i -> i.onInstrumentAdded(alias, instrumentInfoCrypto));

        OrderBook orderBook = new OrderBook();

        registerOrderBookSnapshotCallback(alias, orderbookConfiguration, orderBook);
        registerOrderBookUpdateCallback(alias, orderbookConfiguration, orderBook);

        bitfinexApiBroker.getOrderbookManager().subscribeOrderbook(orderbookConfiguration);

        orderBookConfigByAlias.put(alias, orderbookConfiguration);
    }

    private void registerOrderBookUpdateCallback(String alias, OrderbookConfiguration orderbookConfiguration, OrderBook orderBook) {
        BiConsumer<OrderbookConfiguration, OrderbookEntry> orderBookCallback = (orderbookConfig, entry) -> {
            notifyOrderBookUpdate(alias, orderbookConfig, entry, orderBook);
        };
        bitfinexApiBroker.getOrderbookManager().registerOrderbookCallback(orderbookConfiguration, orderBookCallback);
    }


    private void registerOrderBookSnapshotCallback(String alias, OrderbookConfiguration orderbookConfiguration, OrderBook orderBook) {
        BiConsumer<OrderbookConfiguration, List<OrderbookEntry>> orderBookSnapshopCallback = (orderbookConfig, entries) -> {
            removeLevelsNotPresentInSnapshot(alias, orderBook, orderbookConfig, entries);

            for (OrderbookEntry entry : entries) {
                notifyOrderBookUpdate(alias, orderbookConfig, entry, orderBook);
            }
        };
        bitfinexApiBroker.getOrderbookManager().registerOrderbookSnapshotCallback(orderbookConfiguration, orderBookSnapshopCallback);
    }

    private void notifyOrderBookUpdate(String alias, OrderbookConfiguration orderbookConfiguration, OrderbookEntry entry, OrderBook orderBook) {
        boolean isBid = entry.getAmount().signum() > 0;
        int price = PriceConverter.convertToInteger(orderbookConfiguration, entry.getPrice().doubleValue());
        int amount = getAmount(entry.getAmount());
        int count = entry.getCount().intValue();
        if (count != 0) {
            dataListeners.forEach(l -> l.onDepth(alias, isBid, price, amount));
            orderBook.onUpdate(isBid, price, amount);
        } else {
            dataListeners.forEach(l -> l.onDepth(alias, isBid, price, 0));
            orderBook.onUpdate(isBid, price, 0);
        }
    }

    private void removeLevelsNotPresentInSnapshot(String alias, OrderBook orderBook, OrderbookConfiguration orderbookConfig, List<OrderbookEntry> entries) {
        HashSet<Integer> bidPricesInSnapshot = new HashSet<>();
        HashSet<Integer> askPricesInSnapshot = new HashSet<>();
        for (OrderbookEntry entry : entries) {
            boolean isBid = entry.getAmount().signum() > 0;
            int price = PriceConverter.convertToInteger(orderbookConfig, entry.getPrice().doubleValue());
            if (isBid) bidPricesInSnapshot.add(price);
            else askPricesInSnapshot.add(price);
        }

        Integer[] bidLevels = orderBook.levels(true);
        Integer[] askLevels = orderBook.levels(false);
        Arrays.sort(bidLevels);
        Arrays.sort(askLevels, Comparator.reverseOrder());

        for (int i = 0; i < bidLevels.length; i++) {
            int idx = i;
            if (!bidPricesInSnapshot.contains(bidLevels[i])) {
                dataListeners.forEach(l -> l.onDepth(alias, true, bidLevels[idx], 0));
            }
        }

        for (int i = 0; i < askLevels.length; i++) {
            int idx = i;
            if (!askPricesInSnapshot.contains(askLevels[i])) {
                dataListeners.forEach(l -> l.onDepth(alias, false, askLevels[idx], 0));
            }
        }
    }

    private void subscribeExecutedTrades(String symbol, String exchange, String type, String alias) {
        BitfinexExecutedTradeSymbol tradeSymbol = new BitfinexExecutedTradeSymbol(BitfinexCurrencyPair.valueOf(symbol));
        ExecutedTradesManager executedTradesManager = bitfinexApiBroker.getExecutedTradesManager();

        OrderbookConfiguration orderbookConfiguration = orderBookConfigByAlias.get(alias);

        if (orderbookConfiguration == null) {
            adminListeners.forEach(l -> l.onLoginFailed(LoginFailedReason.FATAL,
                    "Cannot subscribe for trading. Order book config not found."));
            return;
        }

        BiConsumer<BitfinexExecutedTradeSymbol, ExecutedTrade> tradeCallback = (symb, trade) -> {
            double price = PriceConverter.convertToDouble(orderbookConfiguration, trade.getPrice().doubleValue());
            boolean isOtc = false;
            boolean isBidAgressor = trade.getAmount().signum() > 0;
            int amount = getAmount(trade.getAmount());

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
                adminListeners.forEach(l -> l.onSystemTextMessage(e.getMessage(), SystemTextMessageType.UNCLASSIFIED));
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

    private int getAmount(BigDecimal amount) {
        return amount
                .abs()
                .multiply(BigDecimal.valueOf(AMOUNT_MULTIPLIER))
                .toBigInteger()
                .min(BigInteger.valueOf(AMOUNT_LIMIT_AFTER_MULTIPLICATION))
                .intValue();
    }
}
