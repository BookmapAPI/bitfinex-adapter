package bookmap;

import bitfinex.entity.BitfinexCurrencyPair;
import bitfinex.entity.OrderBookPrecision;
import bitfinex.entity.OrderbookConfiguration;

import java.util.HashMap;

public class PriceConverter {
    //multipliers to convert price with precision type P0, P1, P2, P3 to integer value
    private static HashMap<BitfinexCurrencyPair, int[]> multipliers = new HashMap<>();

    private static HashMap<BitfinexCurrencyPair, double[]> priceStep = new HashMap<>();

    //values are derived from websocket playground https://bitfinex.readme.io/v2/reference#ws-public-order-books
    static {
        priceStep.put(BitfinexCurrencyPair.BTC_USD, new double[]{0.1, 1, 10, 100});
        priceStep.put(BitfinexCurrencyPair.IOT_USD, new double[]{0.0001, 0.001, 0.01, 0.1});
    }

    static {
        multipliers.put(BitfinexCurrencyPair.BTC_USD, new int[]{10, 1, 1, 1});
        multipliers.put(BitfinexCurrencyPair.IOT_USD, new int[]{10000, 1000, 100, 10});
    }

    public static int getMultiplier(OrderbookConfiguration orderbookConfiguration) {
        return getMultiplier(orderbookConfiguration.getCurrencyPair(), orderbookConfiguration.getOrderBookPrecision());
    }

    public static double getPriceStep(OrderbookConfiguration orderbookConfiguration) {
        return getPriceStep(orderbookConfiguration.getCurrencyPair(), orderbookConfiguration.getOrderBookPrecision());
    }

    public static int getMultiplier(BitfinexCurrencyPair currencyPair, OrderBookPrecision precision) {
        int index = Integer.valueOf(precision.toString().substring(1));
        return multipliers.get(currencyPair)[index];
    }

    public static double getPriceStep(BitfinexCurrencyPair currencyPair, OrderBookPrecision precision) {
        int index = Integer.valueOf(precision.toString().substring(1));
        return priceStep.get(currencyPair)[index];
    }
}
