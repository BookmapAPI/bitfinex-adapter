package bookmap;

import bitfinex.entity.BitfinexCurrencyPair;
import bitfinex.entity.OrderBookPrecision;
import bitfinex.entity.OrderbookConfiguration;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;

public class PriceConverter {
    private static HashMap<BitfinexCurrencyPair, double[]> priceStep = new HashMap<>();

    // values are derived from websocket playground https://bitfinex.readme.io/v2/reference#ws-public-order-books
    static {
        priceStep.put(BitfinexCurrencyPair.BTC_USD, new double[]{0.1, 1, 10, 100});
        priceStep.put(BitfinexCurrencyPair.IOT_USD, new double[]{0.0001, 0.001, 0.01, 0.1});
    }

    // onDepth accepts integer prices
    public static int convertToInteger(OrderbookConfiguration orderbookConfiguration, double price) {
        return convertToInteger(orderbookConfiguration.getCurrencyPair(), orderbookConfiguration.getOrderBookPrecision(), price);
    }

    public static int convertToInteger(BitfinexCurrencyPair currencyPair, OrderBookPrecision precision, double price) {
        double step = getPriceStep(currencyPair, precision);
        return (int) (price / step);
    }

    // onTrade accepts double prices
    public static double convertToDouble(OrderbookConfiguration orderbookConfiguration, double price) {
        return convertToDouble(orderbookConfiguration.getCurrencyPair(), orderbookConfiguration.getOrderBookPrecision(), price);
    }

    public static double convertToDouble(BitfinexCurrencyPair currencyPair, OrderBookPrecision precision, double price) {
        double step = getPriceStep(currencyPair, precision);
        return price / step;
    }

    public static double getPriceStep(OrderbookConfiguration orderbookConfiguration) {
        return getPriceStep(orderbookConfiguration.getCurrencyPair(), orderbookConfiguration.getOrderBookPrecision());
    }

    public static double getPriceStep(BitfinexCurrencyPair currencyPair, OrderBookPrecision precision) {
        int index = Integer.valueOf(precision.toString().substring(1));
        return priceStep.get(currencyPair)[index];
    }

    public static int roundToInteger(BitfinexCurrencyPair currencyPair, OrderBookPrecision precision, BigDecimal price, boolean isBid) {
        double step = getPriceStep(currencyPair, precision);
        int figuresAfterComa = 0;
        int order = (int) (1 / step);
        while (order >= 10) {
            figuresAfterComa++;
            order /= 10;
        }
        order = (int) step;
        while (order >= 10) {
            figuresAfterComa--;
            order /= 10;
        }
        RoundingMode roundingMode;
        if (isBid) {
            roundingMode = RoundingMode.FLOOR;
        } else {
            roundingMode = RoundingMode.CEILING;
        }
        price = price.abs().setScale(figuresAfterComa, roundingMode);
        return convertToInteger(currencyPair, precision, price.doubleValue());
    }
}
