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
import java.math.BigInteger;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;

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

    private static final HashMap<BitfinexCurrencyPair, Integer> amountMultipliers = new HashMap<>();

    private static final HashSet<BitfinexCurrencyPair> supportedPairs = new HashSet<>();

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
        // If Bookmap sent us preferred price step - used it. If not - use default one.
        final Double priceStep = subscribeInfo instanceof SubscribeInfoCrypto 
                ? ((SubscribeInfoCrypto)subscribeInfo).pips : null;
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
            subscribeOrderBook(symbol, exchange, type, alias, priceStep);
            subscribeExecutedTrades(symbol, exchange, type, alias);
        } else {
            instrumentListeners.forEach(i -> i.onInstrumentAlreadySubscribed(symbol, exchange, type));
        }
    }

    private void subscribeOrderBook(String symbol, String exchange, String type, String alias, Double priceStep) {
        BitfinexCurrencyPair currencyPair = BitfinexCurrencyPair.valueOf(symbol);
        OrderBookPrecision precision = priceStep == null ? OrderBookPrecision.P1
                : PriceConverter.getClosestOrderBookPrecision(currencyPair, priceStep);
        OrderbookConfiguration orderbookConfiguration =
                new OrderbookConfiguration(currencyPair, precision, OrderBookFrequency.F0, 100);

        double pips = PriceConverter.getPriceStep(orderbookConfiguration);

        int amountMultiplier = amountMultipliers.get(orderbookConfiguration.getCurrencyPair());
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
            .setPipsFunction(s -> {
                try {
                    BitfinexCurrencyPair bitfinexCurrencyPair = BitfinexCurrencyPair.valueOf(s.symbol);
                    double[] priceStepPrimitives = PriceConverter.getPriceSteps(bitfinexCurrencyPair);
                    Double[] priceSteps = ArrayUtils.toObject(priceStepPrimitives);
                    // Selecting item in the middle or right before the middle as reasonable default
                    Double defaultValue = priceSteps[(priceSteps.length - 1) / 2];
                    return new DefaultAndList<Double>(
                            defaultValue, Arrays.asList(priceSteps));
                } catch (IllegalArgumentException | NullPointerException e) {
                    return null;
                }
            })
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
                .multiply(BigDecimal.valueOf(amountMultipliers.get(pair)))
                .toBigInteger()
                .min(BigInteger.valueOf(1000_000_000)) // Workaround - values close to integer maximum might cause issues
                .intValue();
    }
}
