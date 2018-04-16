package bookmap;

import bitfinex.entity.BitfinexCurrencyPair;
import bitfinex.entity.OrderBookPrecision;
import bitfinex.entity.OrderbookConfiguration;

import java.util.HashMap;

public class PriceConverter {
    private static HashMap<BitfinexCurrencyPair, double[]> priceStep = new HashMap<>();

    //values are derived from websocket playground https://bitfinex.readme.io/v2/reference#ws-public-order-books
    static {
        priceStep.put(BitfinexCurrencyPair.BTC_USD, new double[]{0.1, 1, 10, 100});
        priceStep.put(BitfinexCurrencyPair.IOT_USD, new double[]{0.0001, 0.001, 0.01, 0.1});
    }

    //onDepth accets integer prices
    public static int convertToInteger(OrderbookConfiguration orderbookConfiguration, double price) {
        double step = getPriceStep(orderbookConfiguration);
        return (int)(price/step);
    }

    //onTrade accepts double prices
    public static double convertToDouble(OrderbookConfiguration orderbookConfiguration, double price) {
        double step = getPriceStep(orderbookConfiguration);
        return price/step;
    }

    public static double getPriceStep(OrderbookConfiguration orderbookConfiguration) {
        return getPriceStep(orderbookConfiguration.getCurrencyPair(), orderbookConfiguration.getOrderBookPrecision());
    }

    public static double getPriceStep(BitfinexCurrencyPair currencyPair, OrderBookPrecision precision) {
        int index = Integer.valueOf(precision.toString().substring(1));
        return priceStep.get(currencyPair)[index];
    }
}
