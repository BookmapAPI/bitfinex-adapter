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
import velox.api.layer1.layers.utils.OrderByOrderBook;
import velox.api.layer1.providers.helper.RawDataHelper;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.BiConsumer;

/**
 * Provider accepts the data from bitfinex and pass it into bookmap using dataListeners.
 */
@Layer1ApiVersion(Layer1ApiVersionValue.VERSION1)
@Layer0LiveModule(fullName = "Bitfinex MBO", shortName = "BFo")
public class MBORealTimeProvider extends ExternalLiveBaseProvider {

    private BitfinexApiBroker bitfinexApiBroker = new BitfinexApiBroker(
            data -> RawDataHelper.sendRawData(data, adminListeners));
    private HeartBeatThread heartBeatThread = new HeartBeatThread(bitfinexApiBroker);


    private final HashSet<String> aliases = new HashSet<>();

    private Map<String, RawOrderbookConfiguration> orderBookConfigByAlias = new HashMap<>();
    private Map<String, BitfinexExecutedTradeSymbol> tradeSymbolByAlias = new HashMap<>();

    private static final OrderBookPrecision DEFAULT_RAW_ORDER_BOOK_PRICE_PRECISION = OrderBookPrecision.P1;

    private static final HashSet<BitfinexCurrencyPair> supportedPairs = new HashSet<>();
    private static final HashMap<BitfinexCurrencyPair, Integer> amountMultipliers = new HashMap<>();

    static {
        // to support a pair it is needed to insert respective price steps into PriceConverter map
        // values are generated using PriceSizeMultiplierStepsCollector
        amountMultipliers.put(BitfinexCurrencyPair.BTC_USD, 10000);
        amountMultipliers.put(BitfinexCurrencyPair.LTC_USD, 10);
        amountMultipliers.put(BitfinexCurrencyPair.LTC_BTC, 100);
        amountMultipliers.put(BitfinexCurrencyPair.ETH_USD, 1000);
        amountMultipliers.put(BitfinexCurrencyPair.ETH_BTC, 1000);
        amountMultipliers.put(BitfinexCurrencyPair.ETC_BTC, 10);
        amountMultipliers.put(BitfinexCurrencyPair.ETC_USD, 10);
        amountMultipliers.put(BitfinexCurrencyPair.RRT_USD, 1);
        amountMultipliers.put(BitfinexCurrencyPair.RRT_BTC, 1);
        amountMultipliers.put(BitfinexCurrencyPair.ZEC_USD, 100);
        amountMultipliers.put(BitfinexCurrencyPair.ZEC_BTC, 100);
        amountMultipliers.put(BitfinexCurrencyPair.XMR_USD, 100000);
        amountMultipliers.put(BitfinexCurrencyPair.XMR_BTC, 100);
        amountMultipliers.put(BitfinexCurrencyPair.DSH_USD, 100);
        amountMultipliers.put(BitfinexCurrencyPair.DSH_BTC, 10000);
        amountMultipliers.put(BitfinexCurrencyPair.BTC_EUR, 10000);
        amountMultipliers.put(BitfinexCurrencyPair.XRP_USD, 1);
        amountMultipliers.put(BitfinexCurrencyPair.XRP_BTC, 1);
        amountMultipliers.put(BitfinexCurrencyPair.IOT_USD, 1);
        amountMultipliers.put(BitfinexCurrencyPair.IOT_BTC, 1);
        amountMultipliers.put(BitfinexCurrencyPair.IOT_ETH, 1);
        amountMultipliers.put(BitfinexCurrencyPair.EOS_USD, 10);
        amountMultipliers.put(BitfinexCurrencyPair.EOS_BTC, 1);
        amountMultipliers.put(BitfinexCurrencyPair.EOS_ETH, 10);
        amountMultipliers.put(BitfinexCurrencyPair.SAN_USD, 10);
        amountMultipliers.put(BitfinexCurrencyPair.SAN_BTC, 1);
        amountMultipliers.put(BitfinexCurrencyPair.SAN_ETH, 1);
        amountMultipliers.put(BitfinexCurrencyPair.OMG_USD, 10);
        amountMultipliers.put(BitfinexCurrencyPair.OMG_BTC, 10);
        amountMultipliers.put(BitfinexCurrencyPair.OMG_ETH, 10);
        amountMultipliers.put(BitfinexCurrencyPair.BCH_USD, 1000);
        amountMultipliers.put(BitfinexCurrencyPair.BCH_BTC, 1000);
        amountMultipliers.put(BitfinexCurrencyPair.BCH_ETH, 1000);
        amountMultipliers.put(BitfinexCurrencyPair.NEO_USD, 10);
        amountMultipliers.put(BitfinexCurrencyPair.NEO_BTC, 10);
        amountMultipliers.put(BitfinexCurrencyPair.NEO_ETH, 100);
        amountMultipliers.put(BitfinexCurrencyPair.ETP_USD, 1);
        amountMultipliers.put(BitfinexCurrencyPair.ETP_BTC, 1);
        amountMultipliers.put(BitfinexCurrencyPair.ETP_ETH, 1);
        amountMultipliers.put(BitfinexCurrencyPair.QTM_USD, 10);
        amountMultipliers.put(BitfinexCurrencyPair.QTM_BTC, 10);
        amountMultipliers.put(BitfinexCurrencyPair.QTM_ETH, 10);
        amountMultipliers.put(BitfinexCurrencyPair.AVT_USD, 1);
        amountMultipliers.put(BitfinexCurrencyPair.AVT_BTC, 1000);
        amountMultipliers.put(BitfinexCurrencyPair.AVT_ETH, 1);
        amountMultipliers.put(BitfinexCurrencyPair.EDO_USD, 1);
        amountMultipliers.put(BitfinexCurrencyPair.EDO_BTC, 1);
        amountMultipliers.put(BitfinexCurrencyPair.EDO_ETH, 1);
        amountMultipliers.put(BitfinexCurrencyPair.BTG_USD, 10);
        amountMultipliers.put(BitfinexCurrencyPair.BTG_BTC, 100);
        amountMultipliers.put(BitfinexCurrencyPair.DAT_USD, 1);
        amountMultipliers.put(BitfinexCurrencyPair.DAT_BTC, 1);
        amountMultipliers.put(BitfinexCurrencyPair.DAT_ETH, 1000);
        amountMultipliers.put(BitfinexCurrencyPair.QSH_USD, 1);
        amountMultipliers.put(BitfinexCurrencyPair.QSH_BTC, 1000000);
        amountMultipliers.put(BitfinexCurrencyPair.QSH_ETH, 1);
        amountMultipliers.put(BitfinexCurrencyPair.YYW_USD, 1);
        amountMultipliers.put(BitfinexCurrencyPair.YYW_BTC, 1);
        amountMultipliers.put(BitfinexCurrencyPair.YYW_ETH, 1);
        amountMultipliers.put(BitfinexCurrencyPair.GNT_USD, 1);
        amountMultipliers.put(BitfinexCurrencyPair.GNT_BTC, 10);
        amountMultipliers.put(BitfinexCurrencyPair.GNT_ETH, 1);
        amountMultipliers.put(BitfinexCurrencyPair.SNT_USD, 1);
        amountMultipliers.put(BitfinexCurrencyPair.SNT_BTC, 1);
        amountMultipliers.put(BitfinexCurrencyPair.SNT_ETH, 1);
        amountMultipliers.put(BitfinexCurrencyPair.IOT_EUR, 1);
        amountMultipliers.put(BitfinexCurrencyPair.BAT_USD, 1);
        amountMultipliers.put(BitfinexCurrencyPair.BAT_BTC, 1);
        amountMultipliers.put(BitfinexCurrencyPair.BAT_ETH, 1);
        amountMultipliers.put(BitfinexCurrencyPair.MNA_USD, 10);
        amountMultipliers.put(BitfinexCurrencyPair.MNA_BTC, 1);
        amountMultipliers.put(BitfinexCurrencyPair.MNA_ETH, 1);
        amountMultipliers.put(BitfinexCurrencyPair.FUN_USD, 1);
        amountMultipliers.put(BitfinexCurrencyPair.FUN_BTC, 1);
        amountMultipliers.put(BitfinexCurrencyPair.FUN_ETH, 1);
        amountMultipliers.put(BitfinexCurrencyPair.ZRX_USD, 1);
        amountMultipliers.put(BitfinexCurrencyPair.ZRX_BTC, 1);
        amountMultipliers.put(BitfinexCurrencyPair.ZRX_ETH, 1);
        amountMultipliers.put(BitfinexCurrencyPair.TNB_USD, 1);
        amountMultipliers.put(BitfinexCurrencyPair.TNB_BTC, 1);
        amountMultipliers.put(BitfinexCurrencyPair.TNB_ETH, 1);
        amountMultipliers.put(BitfinexCurrencyPair.SPK_USD, 1000000);
        amountMultipliers.put(BitfinexCurrencyPair.SPK_BTC, 1000000);
        amountMultipliers.put(BitfinexCurrencyPair.SPK_ETH, 1);
        amountMultipliers.put(BitfinexCurrencyPair.TRX_BTC, 1);
        amountMultipliers.put(BitfinexCurrencyPair.TRX_ETH, 1);
        amountMultipliers.put(BitfinexCurrencyPair.TRX_USD, 1);
        amountMultipliers.put(BitfinexCurrencyPair.RCN_BTC, 1);
        amountMultipliers.put(BitfinexCurrencyPair.RCN_ETH, 1);
        amountMultipliers.put(BitfinexCurrencyPair.RCN_USD, 1);
        amountMultipliers.put(BitfinexCurrencyPair.RLC_BTC, 10);
        amountMultipliers.put(BitfinexCurrencyPair.RLC_ETH, 1);
        amountMultipliers.put(BitfinexCurrencyPair.RLC_USD, 1);
        amountMultipliers.put(BitfinexCurrencyPair.AID_BTC, 1);
        amountMultipliers.put(BitfinexCurrencyPair.AID_USD, 1);
        amountMultipliers.put(BitfinexCurrencyPair.AID_ETH, 1);
        amountMultipliers.put(BitfinexCurrencyPair.SNG_BTC, 1);
        amountMultipliers.put(BitfinexCurrencyPair.SNG_ETH, 1);
        amountMultipliers.put(BitfinexCurrencyPair.SNG_USD, 1);
        amountMultipliers.put(BitfinexCurrencyPair.REP_BTC, 100);
        amountMultipliers.put(BitfinexCurrencyPair.REP_ETH, 1000);
        amountMultipliers.put(BitfinexCurrencyPair.REP_USD, 100);
        amountMultipliers.put(BitfinexCurrencyPair.ELF_BTC, 1);
        amountMultipliers.put(BitfinexCurrencyPair.ELF_ETH, 10000);
        amountMultipliers.put(BitfinexCurrencyPair.ELF_USD, 1);
        
        supportedPairs.addAll(amountMultipliers.keySet());
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
        RawOrderbookConfiguration orderbookConfiguration;
        synchronized (aliases) {
            orderbookConfiguration = orderBookConfigByAlias.get(alias);
        }
        double priceStep = PriceConverter.getPriceStep(orderbookConfiguration.getCurrencyPair(), DEFAULT_RAW_ORDER_BOOK_PRICE_PRECISION);
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
        RawOrderbookConfiguration orderbookConfiguration = new RawOrderbookConfiguration(BitfinexCurrencyPair.valueOf(symbol));

        double pips = PriceConverter.getPriceStep(orderbookConfiguration.getCurrencyPair(), DEFAULT_RAW_ORDER_BOOK_PRICE_PRECISION);

        int amountMultiplier = amountMultipliers.get(orderbookConfiguration.getCurrencyPair());
        InstrumentInfoCrypto instrumentInfoCrypto = new InstrumentInfoCrypto(symbol, exchange, type, pips, 1, "", amountMultiplier);
        instrumentListeners.forEach(i -> i.onInstrumentAdded(alias, instrumentInfoCrypto));

        OrderByOrderBook orderByOrderBook = new OrderByOrderBook();

        registerOrderBookSnapshotCallback(alias, orderbookConfiguration, orderByOrderBook);
        registerOrderBookUpdateCallback(alias, orderbookConfiguration, orderByOrderBook);

        bitfinexApiBroker.getRawOrderbookManager().subscribeOrderbook(orderbookConfiguration);

        orderBookConfigByAlias.put(alias, orderbookConfiguration);
    }

    private void registerOrderBookUpdateCallback(String alias, RawOrderbookConfiguration orderbookConfiguration, OrderByOrderBook orderBook) {
        BiConsumer<RawOrderbookConfiguration, RawOrderbookEntry> orderBookCallback =
                (orderbookConfig, entry) -> notifyOrderBookUpdate(alias, orderbookConfig, entry, orderBook);
        bitfinexApiBroker.getRawOrderbookManager().registerOrderbookCallback(orderbookConfiguration, orderBookCallback);
    }

    /**
     * We handle snapshot separately to remove levels that are not present in snapshot after reconnect.
     * @param alias
     * @param orderbookConfiguration
     * @param orderBook
     */
    private void registerOrderBookSnapshotCallback(String alias, RawOrderbookConfiguration orderbookConfiguration, OrderByOrderBook orderBook) {
        BiConsumer<RawOrderbookConfiguration, List<RawOrderbookEntry>> orderBookSnapshopCallback = (orderbookConfig, entries) -> {
            clearLevels(alias, orderBook, entries, orderbookConfig);

            for (RawOrderbookEntry entry : entries) {
                notifyOrderBookUpdate(alias, orderbookConfig, entry, orderBook);
            }
        };
        bitfinexApiBroker.getRawOrderbookManager().registerOrderbookSnapshotCallback(orderbookConfiguration, orderBookSnapshopCallback);
    }

    private void notifyOrderBookUpdate(String alias, RawOrderbookConfiguration orderbookConfiguration, RawOrderbookEntry entry, OrderByOrderBook orderBook) {
        long orderId = entry.getOrderId();
        boolean isBid = entry.getAmount().signum() > 0;
        int price = PriceConverter.roundToInteger(orderbookConfiguration.getCurrencyPair(), DEFAULT_RAW_ORDER_BOOK_PRICE_PRECISION, entry.getPrice(), isBid);
        int amount = getAmount(orderbookConfiguration.getCurrencyPair(), entry.getAmount());
        if (price != 0) {
            if (orderBook.hasOrder(orderId)) {
                OrderByOrderBook.OrderUpdateResult orderUpdateResult = orderBook.updateOrder(orderId, price, amount);
                dataListeners.forEach(l -> l.onDepth(alias, isBid, orderUpdateResult.fromPrice, (int) orderUpdateResult.fromSize));
                dataListeners.forEach(l -> l.onDepth(alias, isBid, price, (int) orderUpdateResult.toSize));
            } else {
                long newAmount = orderBook.addOrder(orderId, isBid, price, amount);
                dataListeners.forEach(l -> l.onDepth(alias, isBid, orderBook.getLastPriceOfOrder(orderId), (int) newAmount));
            }
        } else if (orderBook.hasOrder(orderId)) {
            int removedOrderPrice = orderBook.getLastPriceOfOrder(orderId);
            long newAmount = orderBook.removeOrder(orderId);
            dataListeners.forEach(l -> l.onDepth(alias, isBid, removedOrderPrice, (int) newAmount));
        }
    }

    /**
     * Removes levels that are not present in snapshot from current bookmap state. Needed to support reconnect.
     * @param alias
     * @param orderByOrderBook
     * @param entries
     * @param orderbookConfig
     */
    private void clearLevels(String alias, OrderByOrderBook orderByOrderBook, List<RawOrderbookEntry> entries, RawOrderbookConfiguration orderbookConfig) {
        OrderBook orderBook = orderByOrderBook.getOrderBook();

        Integer[] oldBidLevels = orderBook.levels(true);
        Integer[] oldAskLevels = orderBook.levels(false);

        Arrays.sort(oldBidLevels);
        Arrays.sort(oldAskLevels, Comparator.reverseOrder());

        HashSet<Integer> newBidLevels = new HashSet<>();
        HashSet<Integer> newAskLevels = new HashSet<>();

        for (RawOrderbookEntry entry : entries) {
            boolean isBid = entry.getAmount().signum() > 0;
            int price = PriceConverter.roundToInteger(orderbookConfig.getCurrencyPair(), DEFAULT_RAW_ORDER_BOOK_PRICE_PRECISION, entry.getPrice(), isBid);
            if (price != 0) {
                if (isBid) newBidLevels.add(price);
                else newAskLevels.add(price);
            }
        }


        for (int i = 0; i < oldBidLevels.length; i++) {
            if (!newBidLevels.contains(oldBidLevels[i])) {
                int idx = i;
                dataListeners.forEach(l -> l.onDepth(alias, true, oldBidLevels[idx], 0));
            }
        }

        for (int i = 0; i < oldAskLevels.length; i++) {
            if (!newAskLevels.contains(oldAskLevels[i])) {
                int idx = i;
                dataListeners.forEach(l -> l.onDepth(alias, false, oldAskLevels[idx], 0));
            }
        }

        orderByOrderBook.getAllIds().forEach(orderByOrderBook::removeOrder);

    }

    private void subscribeExecutedTrades(String symbol, String exchange, String type, String alias) {
        BitfinexExecutedTradeSymbol tradeSymbol = new BitfinexExecutedTradeSymbol(BitfinexCurrencyPair.valueOf(symbol));
        ExecutedTradesManager executedTradesManager = bitfinexApiBroker.getExecutedTradesManager();

        BiConsumer<BitfinexExecutedTradeSymbol, ExecutedTrade> tradeCallback = (symb, trade) -> {
            double price = PriceConverter.convertToDouble(symb.getBitfinexCurrencyPair(), DEFAULT_RAW_ORDER_BOOK_PRICE_PRECISION, trade.getPrice());
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
        heartBeatThread.shutDown();
        bitfinexApiBroker.close();
    }

    private static String createAlias(String symbol, String exchange, String type) {
        return symbol;
    }

    private int getAmount(BitfinexCurrencyPair pair, BigDecimal amount) {
        return amount
                .abs()
                .multiply(BigDecimal.valueOf(amountMultipliers.get(pair)))
                .toBigInteger()
                .intValue();
    }

}
