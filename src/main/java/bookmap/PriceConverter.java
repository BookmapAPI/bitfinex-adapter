package bookmap;

import bitfinex.entity.BitfinexCurrencyPair;
import bitfinex.entity.OrderBookPrecision;
import bitfinex.entity.OrderbookConfiguration;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;

public class PriceConverter {
    private static HashMap<BitfinexCurrencyPair, double[]> priceStep = new HashMap<>();

    // values are generated using PriceSizeMultiplierStepsCollector
    static {
        priceStep.put(BitfinexCurrencyPair.BTC_USD, new double[]{0.1, 1.0, 10.0, 100.0});
        priceStep.put(BitfinexCurrencyPair.LTC_USD, new double[]{0.001, 0.01, 0.1, 1.0});
        priceStep.put(BitfinexCurrencyPair.LTC_BTC, new double[]{1.0E-6, 1.0E-5, 1.0E-5, 1.0E-4});
        priceStep.put(BitfinexCurrencyPair.ETH_USD, new double[]{0.01, 0.1, 1.0, 10.0});
        priceStep.put(BitfinexCurrencyPair.ETH_BTC, new double[]{1.0E-6, 1.0E-5, 1.0E-4, 0.001});
        priceStep.put(BitfinexCurrencyPair.ETC_BTC, new double[]{1.0E-7, 1.0E-6, 1.0E-5, 1.0E-5});
        priceStep.put(BitfinexCurrencyPair.ETC_USD, new double[]{0.001, 0.01, 0.1, 0.1});
        priceStep.put(BitfinexCurrencyPair.RRT_USD, new double[]{1.0E-6, 1.0E-5, 1.0E-4, 0.001});
        priceStep.put(BitfinexCurrencyPair.RRT_BTC, new double[]{1.0E-8, 1.0E-8, 1.0E-8, 1.0E-7});
        priceStep.put(BitfinexCurrencyPair.ZEC_USD, new double[]{0.01, 0.1, 1.0, 1.0});
        priceStep.put(BitfinexCurrencyPair.ZEC_BTC, new double[]{1.0E-6, 1.0E-5, 1.0E-4, 1.0E-4});
        priceStep.put(BitfinexCurrencyPair.XMR_USD, new double[]{0.01, 0.1, 1.0, 1.0});
        priceStep.put(BitfinexCurrencyPair.XMR_BTC, new double[]{1.0E-6, 1.0E-5, 1.0E-4, 1.0E-4});
        priceStep.put(BitfinexCurrencyPair.DSH_USD, new double[]{0.01, 0.1, 1.0, 1.0});
        priceStep.put(BitfinexCurrencyPair.DSH_BTC, new double[]{1.0E-6, 1.0E-5, 1.0E-4, 1.0E-4});
        priceStep.put(BitfinexCurrencyPair.BTC_EUR, new double[]{0.1, 1.0, 10.0, 100.0});
        priceStep.put(BitfinexCurrencyPair.XRP_USD, new double[]{1.0E-5, 1.0E-4, 0.001, 0.01});
        priceStep.put(BitfinexCurrencyPair.XRP_BTC, new double[]{1.0E-8, 1.0E-8, 1.0E-7, 1.0E-6});
        priceStep.put(BitfinexCurrencyPair.IOT_USD, new double[]{1.0E-5, 1.0E-4, 0.001, 0.01});
        priceStep.put(BitfinexCurrencyPair.IOT_BTC, new double[]{1.0E-8, 1.0E-7, 1.0E-7, 1.0E-6});
        priceStep.put(BitfinexCurrencyPair.IOT_ETH, new double[]{1.0E-7, 1.0E-6, 1.0E-5, 1.0E-5});
        priceStep.put(BitfinexCurrencyPair.EOS_USD, new double[]{1.0E-4, 0.001, 0.01, 0.1});
        priceStep.put(BitfinexCurrencyPair.EOS_BTC, new double[]{1.0E-8, 1.0E-7, 1.0E-6, 1.0E-5});
        priceStep.put(BitfinexCurrencyPair.EOS_ETH, new double[]{1.0E-6, 1.0E-5, 1.0E-4, 1.0E-4});
        priceStep.put(BitfinexCurrencyPair.SAN_USD, new double[]{1.0E-4, 0.001, 0.001, 0.01});
        priceStep.put(BitfinexCurrencyPair.SAN_BTC, new double[]{1.0E-8, 1.0E-7, 1.0E-6, 1.0E-6});
        priceStep.put(BitfinexCurrencyPair.SAN_ETH, new double[]{1.0E-7, 2.0E-7, 1.0E-6, 1.0E-5});
        priceStep.put(BitfinexCurrencyPair.OMG_USD, new double[]{1.0E-4, 0.001, 0.01, 0.1});
        priceStep.put(BitfinexCurrencyPair.OMG_BTC, new double[]{1.0E-8, 1.0E-7, 1.0E-6, 1.0E-5});
        priceStep.put(BitfinexCurrencyPair.OMG_ETH, new double[]{1.0E-6, 1.0E-5, 1.0E-4, 1.0E-4});
        priceStep.put(BitfinexCurrencyPair.BCH_USD, new double[]{0.01, 0.1, 1.0, 10.0});
        priceStep.put(BitfinexCurrencyPair.BCH_BTC, new double[]{1.0E-5, 1.0E-4, 1.0E-4, 0.001});
        priceStep.put(BitfinexCurrencyPair.BCH_ETH, new double[]{1.0E-4, 0.001, 0.01, 0.01});
        priceStep.put(BitfinexCurrencyPair.NEO_USD, new double[]{0.001, 0.01, 0.1, 0.1});
        priceStep.put(BitfinexCurrencyPair.NEO_BTC, new double[]{1.0E-7, 1.0E-6, 1.0E-5, 1.0E-4});
        priceStep.put(BitfinexCurrencyPair.NEO_ETH, new double[]{1.0E-6, 1.0E-5, 1.0E-4, 0.001});
        priceStep.put(BitfinexCurrencyPair.ETP_USD, new double[]{1.0E-4, 0.001, 0.01, 0.01});
        priceStep.put(BitfinexCurrencyPair.ETP_BTC, new double[]{1.0E-8, 1.0E-7, 1.0E-6, 1.0E-6});
        priceStep.put(BitfinexCurrencyPair.ETP_ETH, new double[]{1.0E-7, 1.0E-6, 1.0E-5, 1.0E-4});
        priceStep.put(BitfinexCurrencyPair.QTM_USD, new double[]{1.0E-4, 0.001, 0.01, 0.1});
        priceStep.put(BitfinexCurrencyPair.QTM_BTC, new double[]{1.0E-8, 1.0E-7, 1.0E-6, 1.0E-5});
        priceStep.put(BitfinexCurrencyPair.QTM_ETH, new double[]{1.0E-6, 1.0E-5, 1.0E-5, 1.0E-6});
        priceStep.put(BitfinexCurrencyPair.AVT_USD, new double[]{1.0E-5, 1.0E-4, 0.001, 0.01});
        priceStep.put(BitfinexCurrencyPair.AVT_BTC, new double[]{1.0E-8, 7.0E-8, 1.0E-7, 1.0E-6});
        priceStep.put(BitfinexCurrencyPair.AVT_ETH, new double[]{1.0E-7, 1.0E-6, 4.0E-6, 1.0E-6});
        priceStep.put(BitfinexCurrencyPair.EDO_USD, new double[]{1.0E-5, 1.0E-4, 0.001, 0.01});
        priceStep.put(BitfinexCurrencyPair.EDO_BTC, new double[]{1.0E-8, 1.0E-7, 5.0E-7, 1.0E-6});
        priceStep.put(BitfinexCurrencyPair.EDO_ETH, new double[]{1.0E-7, 1.0E-6, 1.0E-5, 1.0E-5});
        priceStep.put(BitfinexCurrencyPair.BTG_USD, new double[]{0.001, 0.01, 0.1, 0.4});
        priceStep.put(BitfinexCurrencyPair.BTG_BTC, new double[]{1.0E-7, 1.0E-6, 1.0E-5, 9.0E-5});
        priceStep.put(BitfinexCurrencyPair.DAT_USD, new double[]{1.0E-6, 1.0E-5, 1.0E-4, 0.001});
        priceStep.put(BitfinexCurrencyPair.DAT_BTC, new double[]{1.0E-8, 1.0E-8, 1.0E-8, 1.0E-7});
        priceStep.put(BitfinexCurrencyPair.DAT_ETH, new double[]{1.0E-8, 1.0E-7, 1.0E-6, 1.0E-7});
        priceStep.put(BitfinexCurrencyPair.QSH_USD, new double[]{1.0E-5, 1.0E-4, 0.001, 0.001});
        priceStep.put(BitfinexCurrencyPair.QSH_BTC, new double[]{1.0E-8, 1.0E-8, 1.0E-7, 1.0E-7});
        priceStep.put(BitfinexCurrencyPair.QSH_ETH, new double[]{1.0E-8, 1.0E-7, 1.0E-6, 1.0E-6});
        priceStep.put(BitfinexCurrencyPair.YYW_USD, new double[]{1.0E-6, 1.0E-5, 1.0E-4, 0.001});
        priceStep.put(BitfinexCurrencyPair.YYW_BTC, new double[]{1.0E-8, 1.0E-8, 1.0E-8, 1.0E-8});
        priceStep.put(BitfinexCurrencyPair.YYW_ETH, new double[]{1.0E-8, 1.0E-8, 1.0E-8, 1.0E-8});
        priceStep.put(BitfinexCurrencyPair.GNT_USD, new double[]{1.0E-5, 1.0E-4, 0.001, 0.001});
        priceStep.put(BitfinexCurrencyPair.GNT_BTC, new double[]{1.0E-8, 1.0E-8, 1.0E-7, 9.0E-7});
        priceStep.put(BitfinexCurrencyPair.GNT_ETH, new double[]{1.0E-8, 1.0E-7, 1.0E-6, 1.0E-5});
        priceStep.put(BitfinexCurrencyPair.SNT_USD, new double[]{1.0E-6, 1.0E-5, 1.0E-4, 0.001});
        priceStep.put(BitfinexCurrencyPair.SNT_BTC, new double[]{1.0E-8, 1.0E-8, 1.0E-8, 1.0E-7});
        priceStep.put(BitfinexCurrencyPair.SNT_ETH, new double[]{1.0E-8, 1.0E-7, 3.0E-8, 1.0E-8});
        priceStep.put(BitfinexCurrencyPair.IOT_EUR, new double[]{1.0E-5, 1.0E-4, 0.001, 0.01});
        priceStep.put(BitfinexCurrencyPair.BAT_USD, new double[]{1.0E-5, 1.0E-4, 0.001, 0.002});
        priceStep.put(BitfinexCurrencyPair.BAT_BTC, new double[]{1.0E-8, 1.0E-8, 1.0E-7, 1.0E-6});
        priceStep.put(BitfinexCurrencyPair.BAT_ETH, new double[]{1.0E-8, 1.0E-7, 1.0E-6, 1.0E-5});
        priceStep.put(BitfinexCurrencyPair.MNA_USD, new double[]{1.0E-5, 1.0E-4, 1.0E-4, 0.001});
        priceStep.put(BitfinexCurrencyPair.MNA_BTC, new double[]{1.0E-8, 1.0E-8, 2.0E-8, 1.0E-7});
        priceStep.put(BitfinexCurrencyPair.MNA_ETH, new double[]{1.0E-8, 1.0E-7, 1.0E-6, 1.0E-6});
        priceStep.put(BitfinexCurrencyPair.FUN_USD, new double[]{1.0E-6, 1.0E-5, 1.0E-4, 1.0E-4});
        priceStep.put(BitfinexCurrencyPair.FUN_BTC, new double[]{1.0E-8, 1.0E-8, 1.0E-8, 1.0E-8});
        priceStep.put(BitfinexCurrencyPair.FUN_ETH, new double[]{1.0E-8, 1.0E-8, 1.0E-7, 1.0E-6});
        priceStep.put(BitfinexCurrencyPair.ZRX_USD, new double[]{1.0E-4, 0.001, 0.001, 0.01});
        priceStep.put(BitfinexCurrencyPair.ZRX_BTC, new double[]{1.0E-8, 1.0E-7, 1.0E-6, 1.0E-6});
        priceStep.put(BitfinexCurrencyPair.ZRX_ETH, new double[]{1.0E-7, 1.0E-6, 1.0E-5, 1.0E-5});
        priceStep.put(BitfinexCurrencyPair.TNB_USD, new double[]{1.0E-6, 1.0E-5, 1.0E-4, 1.0E-4});
        priceStep.put(BitfinexCurrencyPair.TNB_BTC, new double[]{1.0E-8, 1.0E-8, 1.0E-8, 1.0E-8});
        priceStep.put(BitfinexCurrencyPair.TNB_ETH, new double[]{1.0E-8, 1.0E-8, 1.0E-8, 1.0E-8});
        priceStep.put(BitfinexCurrencyPair.SPK_USD, new double[]{1.0E-6, 1.0E-5, 1.0E-4, 0.001});
        priceStep.put(BitfinexCurrencyPair.SPK_BTC, new double[]{1.0E-8, 1.0E-8, 1.0E-8, 1.0E-8});
        priceStep.put(BitfinexCurrencyPair.SPK_ETH, new double[]{1.0E-8, 1.0E-8, 1.0E-8, 1.0E-8});
        priceStep.put(BitfinexCurrencyPair.TRX_BTC, new double[]{1.0E-8, 1.0E-8, 1.0E-8, 1.0E-7});
        priceStep.put(BitfinexCurrencyPair.TRX_ETH, new double[]{1.0E-8, 1.0E-8, 1.0E-7, 9.0E-7});
        priceStep.put(BitfinexCurrencyPair.TRX_USD, new double[]{1.0E-6, 1.0E-5, 1.0E-4, 0.001});
        priceStep.put(BitfinexCurrencyPair.RCN_BTC, new double[]{1.0E-8, 1.0E-8, 1.0E-8, 1.0E-8});
        priceStep.put(BitfinexCurrencyPair.RCN_ETH, new double[]{1.0E-8, 1.0E-7, 1.0E-7, 1.0E-8});
        priceStep.put(BitfinexCurrencyPair.RCN_USD, new double[]{1.0E-6, 1.0E-5, 1.0E-4, 0.001});
        priceStep.put(BitfinexCurrencyPair.RLC_BTC, new double[]{1.0E-8, 1.0E-8, 1.0E-7, 1.0E-7});
        priceStep.put(BitfinexCurrencyPair.RLC_ETH, new double[]{1.0E-7, 1.0E-7, 1.0E-7, 1.0E-7});
        priceStep.put(BitfinexCurrencyPair.RLC_USD, new double[]{1.0E-5, 4.0E-4, 0.001, 0.01});
        priceStep.put(BitfinexCurrencyPair.AID_BTC, new double[]{1.0E-8, 1.0E-8, 7.0E-8, 1.0E-7});
        priceStep.put(BitfinexCurrencyPair.AID_USD, new double[]{1.0E-6, 1.0E-4, 2.0E-4, 0.001});
        priceStep.put(BitfinexCurrencyPair.AID_ETH, new double[]{1.0E-8, 1.0E-8, 1.0E-7, 1.0E-6});
        priceStep.put(BitfinexCurrencyPair.SNG_BTC, new double[]{1.0E-8, 1.0E-8, 1.0E-8, 1.0E-8});
        priceStep.put(BitfinexCurrencyPair.SNG_ETH, new double[]{1.0E-8, 1.0E-8, 1.0E-8, 1.0E-8});
        priceStep.put(BitfinexCurrencyPair.SNG_USD, new double[]{1.0E-6, 1.0E-5, 1.0E-4, 4.0E-4});
        priceStep.put(BitfinexCurrencyPair.REP_BTC, new double[]{1.0E-7, 1.0E-6, 1.0E-5, 1.0E-5});
        priceStep.put(BitfinexCurrencyPair.REP_ETH, new double[]{1.0E-6, 1.0E-5, 1.0E-4, 5.0E-6});
        priceStep.put(BitfinexCurrencyPair.REP_USD, new double[]{0.001, 0.02, 0.1, 0.1});
        priceStep.put(BitfinexCurrencyPair.ELF_BTC, new double[]{1.0E-8, 1.0E-8, 1.0E-7, 1.0E-6});
        priceStep.put(BitfinexCurrencyPair.ELF_ETH, new double[]{1.0E-7, 8.0E-8, 8.0E-8, 1.0E-8});
        priceStep.put(BitfinexCurrencyPair.ELF_USD, new double[]{1.0E-5, 1.0E-4, 0.001, 0.01});
    }

    // onDepth accepts integer prices
    public static int convertToInteger(OrderbookConfiguration orderbookConfiguration, BigDecimal price) {
        return convertToInteger(orderbookConfiguration.getCurrencyPair(), orderbookConfiguration.getOrderBookPrecision(), price);
    }

    public static int convertToInteger(BitfinexCurrencyPair currencyPair, OrderBookPrecision precision, BigDecimal price) {
        double step = getPriceStep(currencyPair, precision);
        int figuresAfterComa = (int) Math.round(1.0 / step);
        return price.multiply(BigDecimal.valueOf(figuresAfterComa)).intValue();
    }

    // onTrade accepts double prices
    public static double convertToDouble(OrderbookConfiguration orderbookConfiguration, BigDecimal price) {
        return convertToDouble(orderbookConfiguration.getCurrencyPair(), orderbookConfiguration.getOrderBookPrecision(), price);
    }

    public static double convertToDouble(BitfinexCurrencyPair currencyPair, OrderBookPrecision precision, BigDecimal price) {
        double step = getPriceStep(currencyPair, precision);
        int figuresAfterComa = (int) Math.round(1.0 / step);
        return price.multiply(BigDecimal.valueOf(figuresAfterComa)).doubleValue();
    }

    public static double getPriceStep(OrderbookConfiguration orderbookConfiguration) {
        return getPriceStep(orderbookConfiguration.getCurrencyPair(), orderbookConfiguration.getOrderBookPrecision());
    }

    public static double getPriceStep(BitfinexCurrencyPair currencyPair, OrderBookPrecision precision) {
        int index = Integer.valueOf(precision.toString().substring(1));
        return priceStep.get(currencyPair)[index];
    }
    
    public static OrderBookPrecision getClosestOrderBookPrecision(BitfinexCurrencyPair currencyPair, double priceStep) {
        double minDistance = Double.POSITIVE_INFINITY;
        OrderBookPrecision bestPrecision = null;

        for (OrderBookPrecision precision : OrderBookPrecision.values()) {
            double priceStepForPrecision = getPriceStep(currencyPair, precision);
            
            double currentDistance = Math.abs(priceStepForPrecision - priceStep);
            if (currentDistance < minDistance) {
                minDistance = currentDistance;
                bestPrecision = precision;
            }
        }
        return bestPrecision;
    }
    
    public static double[] getPriceSteps(BitfinexCurrencyPair currencyPair) {
        return priceStep.get(currencyPair);
    }

    public static int roundToInteger(BitfinexCurrencyPair currencyPair, OrderBookPrecision precision, BigDecimal price, boolean isBid) {
        double step = getPriceStep(currencyPair, precision);
        int figuresAfterComa = 0;
        int order = (int) Math.round(1.0 / step);
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
        return convertToInteger(currencyPair, precision, price);
    }
}
