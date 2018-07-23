package bookmap;

import bitfinex.BitfinexApiBroker;
import bitfinex.entity.*;
import bitfinex.manager.ExecutedTradesManager;
import velox.api.layer0.annotations.Layer0LiveModule;
import velox.api.layer0.live.ExternalLiveBaseProvider;
import velox.api.layer1.Layer1ApiAdminListener;
import velox.api.layer1.annotations.Layer1ApiVersion;
import velox.api.layer1.annotations.Layer1ApiVersionValue;
import velox.api.layer1.data.*;
import velox.api.layer1.layers.utils.OrderBook;
import velox.api.layer1.providers.helper.RawDataHelper;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Provider accepts the data from bitfinex and pass it into bookmap using dataListeners.
 */
@Layer1ApiVersion(Layer1ApiVersionValue.VERSION1)
@Layer0LiveModule(fullName = "Bitfinex MBP", shortName = "BFp")
public class MBPRealTimeProvider extends ExternalLiveBaseProvider {

    private BitfinexApiBroker bitfinexApiBroker = new BitfinexApiBroker(
            data -> RawDataHelper.sendRawData(data, adminListeners));
    private HeartBeatThread heartBeatThread = new HeartBeatThread(bitfinexApiBroker);

    private final HashSet<String> aliases = new HashSet<>();

    private Map<String, OrderbookConfiguration> orderBookConfigByAlias = new HashMap<>();
    private Map<String, BitfinexExecutedTradeSymbol> tradeSymbolByAlias = new HashMap<>();

    private static final HashMap<BitfinexCurrencyPair, Integer> amountMultiPliers = new HashMap<>();

    private static final HashSet<BitfinexCurrencyPair> supportedPairs = new HashSet<>();

    static {
        // to support a pair it is needed to insert respective price steps into PriceConverter map
        supportedPairs.add(BitfinexCurrencyPair.BTC_USD);
        supportedPairs.add(BitfinexCurrencyPair.IOT_USD);

        amountMultiPliers.put(BitfinexCurrencyPair.BTC_USD, (int) 1e4);
        amountMultiPliers.put(BitfinexCurrencyPair.IOT_USD, (int) 1e1);
    }

    @Override
    public void login(LoginData loginData) {
        try {
            bitfinexApiBroker.connect();
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

    /**
     * Subscribe method is called when new instrument is added.
     */
    @Override
    public void subscribe(SubscribeInfo subscribeInfo) {
        final String symbol = subscribeInfo.symbol;
        final String exchange = subscribeInfo.exchange;
        final String type = subscribeInfo.type;
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

        int amountMultiplier = amountMultiPliers.get(orderbookConfiguration.getCurrencyPair());
        InstrumentInfoCrypto instrumentInfoCrypto = new InstrumentInfoCrypto(symbol, exchange, type, pips, 1, "", amountMultiplier);
        instrumentListeners.forEach(i -> i.onInstrumentAdded(alias, instrumentInfoCrypto));

        OrderBook orderBook = new OrderBook();

        registerOrderBookSnapshotCallback(alias, orderbookConfiguration, orderBook);
        registerOrderBookUpdateCallback(alias, orderbookConfiguration, orderBook);

        bitfinexApiBroker.getOrderbookManager().subscribeOrderbook(orderbookConfiguration);

        orderBookConfigByAlias.put(alias, orderbookConfiguration);
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

    private void removeLevelsNotPresentInSnapshot(String alias, OrderBook orderBook, OrderbookConfiguration orderbookConfig, List<OrderbookEntry> entries) {
        HashSet<Integer> bidPricesInSnapshot = new HashSet<>();
        HashSet<Integer> askPricesInSnapshot = new HashSet<>();
        for (OrderbookEntry entry : entries) {
            boolean isBid = entry.getAmount().signum() > 0;
            int price = PriceConverter.convertToInteger(orderbookConfig, entry.getPrice());
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

    private void registerOrderBookUpdateCallback(String alias, OrderbookConfiguration orderbookConfiguration, OrderBook orderBook) {
        BiConsumer<OrderbookConfiguration, OrderbookEntry> orderBookCallback = (orderbookConfig, entry) -> {
            notifyOrderBookUpdate(alias, orderbookConfig, entry, orderBook);
        };
        bitfinexApiBroker.getOrderbookManager().registerOrderbookCallback(orderbookConfiguration, orderBookCallback);
    }

    private void notifyOrderBookUpdate(String alias, OrderbookConfiguration orderbookConfiguration, OrderbookEntry entry, OrderBook orderBook) {
        boolean isBid = entry.getAmount().signum() > 0;
        int price = PriceConverter.convertToInteger(orderbookConfiguration, entry.getPrice());
        int amount = getAmount(orderbookConfiguration.getCurrencyPair(), entry.getAmount());
        int count = entry.getCount().intValue();
        if (count != 0) {
            dataListeners.forEach(l -> l.onDepth(alias, isBid, price, amount));
            orderBook.onUpdate(isBid, price, amount);
        } else {
            dataListeners.forEach(l -> l.onDepth(alias, isBid, price, 0));
            orderBook.onUpdate(isBid, price, 0);
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
            double price = PriceConverter.convertToDouble(orderbookConfiguration, trade.getPrice());
            boolean isOtc = false;
            boolean isBidAgressor = trade.getAmount().signum() > 0;
            int amount = getAmount(symb.getBitfinexCurrencyPair(), trade.getAmount());

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
    public Layer1ApiProviderSupportedFeatures getSupportedFeatures() {
        return super.getSupportedFeatures().toBuilder()
            .setKnownInstruments(
                    supportedPairs.stream()
                    .map(p -> new SubscribeInfo(p.name(), null, null))
                    .collect(Collectors.toList())
            )
            .setExchangeUsedForSubscription(false)
            .setTypeUsedForSubscription(false)
            .setHistoricalDataInfo(new BmSimpleHistoricalDataInfo(
                    "http://bitfinex.historicaldata.bookmap.com:28080/historical-data-server-1.0/"))
            .build();
    }

    @Override
    public void close() {
        heartBeatThread.shutDown();
        bitfinexApiBroker.close();
    }

    private static String createAlias(String symbol, String exchange, String type) {
        return symbol;
    }

    private int getAmount(BitfinexCurrencyPair pair, BigDecimal amount) {
        return amount
                .abs()
                .multiply(BigDecimal.valueOf(amountMultiPliers.get(pair)))
                .toBigInteger()
                .intValue();
    }
}
