package bookmap;

import bitfinex.BitfinexApiBroker;
import bitfinex.entity.*;
import bitfinex.manager.ExecutedTradesManager;
import velox.api.layer0.live.ExternalLiveBaseProvider;
import velox.api.layer1.Layer1ApiAdminListener;
import velox.api.layer1.common.Log;
import velox.api.layer1.data.*;
import velox.api.layer1.layers.utils.OrderBook;
import velox.api.layer1.layers.utils.OrderByOrderBook;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.function.BiConsumer;

public class MBORealTimeProvider extends ExternalLiveBaseProvider {

    private BitfinexApiBroker bitfinexApiBroker = new BitfinexApiBroker();

    private final HashSet<String> aliases = new HashSet<>();

    private Map<String, RawOrderbookConfiguration> orderBookConfigByAlias = new HashMap<>();
    private Map<String, BitfinexExecutedTradeSymbol> tradeSymbolByAlias = new HashMap<>();

    private int AMOUNT_MULTIPLIER = (int) 1e4; // bookmap accepts integer amounts, but bitfinex returns float values. we will save 4 digits after coma
    private int AMOUNT_LIMIT_AFTER_MULTIPLICATION = (int) 1e9; // limit is needed to not cause integer overflows
    private OrderBookPrecision DEFAULT_RAW_ORDER_BOOK_PRICE_PRECISION = OrderBookPrecision.P1;

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
        RawOrderbookConfiguration orderbookConfiguration;
        synchronized (aliases) {
            orderbookConfiguration = orderBookConfigByAlias.get(alias);
        }
        double priceStep = PriceConverter.getPriceStep(orderbookConfiguration.getCurrencyPair(), DEFAULT_RAW_ORDER_BOOK_PRICE_PRECISION);
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
        RawOrderbookConfiguration orderbookConfiguration = new RawOrderbookConfiguration(BitfinexCurrencyPair.valueOf(symbol));

        double pips = PriceConverter.getPriceStep(orderbookConfiguration.getCurrencyPair(), DEFAULT_RAW_ORDER_BOOK_PRICE_PRECISION);

        InstrumentInfoCrypto instrumentInfoCrypto = new InstrumentInfoCrypto(symbol, exchange, type, pips, 1, "", AMOUNT_MULTIPLIER);
        instrumentListeners.forEach(i -> i.onInstrumentAdded(alias, instrumentInfoCrypto));

        OrderByOrderBook orderByOrderBook = new OrderByOrderBook();

        registerOrderBookSnapshotCallback(alias, orderbookConfiguration, orderByOrderBook);
        registerOrderBookUpdateCallback(alias, orderbookConfiguration, orderByOrderBook);

        bitfinexApiBroker.getRawOrderbookManager().subscribeOrderbook(orderbookConfiguration);

        orderBookConfigByAlias.put(alias, orderbookConfiguration);
    }

    private void registerOrderBookUpdateCallback(String alias, RawOrderbookConfiguration orderbookConfiguration, OrderByOrderBook orderBook) {
        BiConsumer<RawOrderbookConfiguration, RawOrderbookEntry> orderBookCallback = (orderbookConfig, entry) -> {
            notifyOrderBookUpdate(alias, orderbookConfig, entry, orderBook);
        };
        bitfinexApiBroker.getRawOrderbookManager().registerOrderbookCallback(orderbookConfiguration, orderBookCallback);
    }


    private void registerOrderBookSnapshotCallback(String alias, RawOrderbookConfiguration orderbookConfiguration, OrderByOrderBook orderBook) {
        BiConsumer<RawOrderbookConfiguration, List<RawOrderbookEntry>> orderBookSnapshopCallback = (orderbookConfig, entries) -> {
            Log.info("snapshot start");
            clearLevels(alias, orderBook);

            for (RawOrderbookEntry entry : entries) {
                notifyOrderBookUpdate(alias, orderbookConfig, entry, orderBook);
            }
            Log.info("snapshot end");
        };
        bitfinexApiBroker.getRawOrderbookManager().registerOrderbookSnapshotCallback(orderbookConfiguration, orderBookSnapshopCallback);
    }

    private void notifyOrderBookUpdate(String alias, RawOrderbookConfiguration orderbookConfiguration, RawOrderbookEntry entry, OrderByOrderBook orderBook) {
        long orderId = entry.getOrderId();
        boolean isBid = entry.getAmount().signum() > 0;
        int price = PriceConverter.roundToInteger(orderbookConfiguration.getCurrencyPair(), DEFAULT_RAW_ORDER_BOOK_PRICE_PRECISION, entry.getPrice(), isBid);
        int amount = getAmount(entry.getAmount());
        Log.info("Processing entry. price = " + price + " orderId = " + orderId + " amount = " + amount + " isbid = " + isBid);
//        Log.info(entry.toString());
        if (price != 0) {
            if (orderBook.hasOrder(orderId)) {
                Log.info("updatecase");
                OrderByOrderBook.OrderUpdateResult orderUpdateResult = orderBook.updateOrder(orderId, price, amount);
                dataListeners.forEach(l -> l.onDepth(alias, isBid, orderUpdateResult.fromPrice, (int) orderUpdateResult.fromSize));
                Log.info("fromPrice = " + orderUpdateResult.fromPrice + " fromsize = " + orderUpdateResult.fromSize);
                dataListeners.forEach(l -> l.onDepth(alias, isBid, price, (int) orderUpdateResult.toSize));
                Log.info("tozite = " + orderUpdateResult.toSize);
            } else {
                Log.info("addcase");
                long newAmount = orderBook.addOrder(orderId, isBid, price, amount);
                dataListeners.forEach(l -> l.onDepth(alias, isBid, orderBook.getLastPriceOfOrder(orderId), (int) newAmount));
                Log.info("newamount = " + newAmount);
            }
        } else {
            Log.info("removecase");
            int removedOrderPrice = orderBook.getLastPriceOfOrder(orderId);
            long newAmount = orderBook.removeOrder(orderId);
            dataListeners.forEach(l -> l.onDepth(alias, isBid, removedOrderPrice, (int) newAmount));
            Log.info("newamount = " + newAmount);
        }
    }

    private void clearLevels(String alias, OrderByOrderBook orderByOrderBook) {
        OrderBook orderBook = orderByOrderBook.getOrderBook();

        Integer[] bidLevels = orderBook.levels(true);
        Integer[] askLevels = orderBook.levels(false);
        Arrays.sort(bidLevels);
        Arrays.sort(askLevels, Comparator.reverseOrder());

        for (int i = 0; i < bidLevels.length; i++) {
            int idx = i;
            dataListeners.forEach(l -> l.onDepth(alias, true, bidLevels[idx], 0));
            Log.info("something removed");
        }

        for (int i = 0; i < askLevels.length; i++) {
            int idx = i;
            dataListeners.forEach(l -> l.onDepth(alias, false, askLevels[idx], 0));
            Log.info("something removed1");
        }

    }

    private void subscribeExecutedTrades(String symbol, String exchange, String type, String alias) {
        BitfinexExecutedTradeSymbol tradeSymbol = new BitfinexExecutedTradeSymbol(BitfinexCurrencyPair.valueOf(symbol));
        ExecutedTradesManager executedTradesManager = bitfinexApiBroker.getExecutedTradesManager();

        BiConsumer<BitfinexExecutedTradeSymbol, ExecutedTrade> tradeCallback = (symb, trade) -> {
            double price = PriceConverter.convertToDouble(symb.getBitfinexCurrencyPair(), DEFAULT_RAW_ORDER_BOOK_PRICE_PRECISION, trade.getPrice().doubleValue());
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
                bitfinexApiBroker.getRawOrderbookManager().unsubscribeOrderbook(orderBookConfigByAlias.get(alias));
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
