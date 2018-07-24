package bookmap;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.google.gson.Gson;

import bitfinex.entity.BitfinexCurrencyPair;
import bitfinex.entity.OrderBookPrecision;

/**
 * Utility class collecting list of precisions and price multipliers. It's not
 * fully reliable, and generated data does not seem to change too often, that's
 * why it isn't embedded into adapter to be ran automatically.
 */
public class PriceSizeMultiplierStepsCollector {
    
    private static double[][] getOrderBook(BitfinexCurrencyPair symbol, OrderBookPrecision precision) {
        try {
            URL url = new URL("https://api.bitfinex.com/v2/book/" + symbol.toBitfinexString() + "/" + precision);
            try (InputStream inputStream = url.openStream()) {
                return new Gson().fromJson(new InputStreamReader(inputStream), double[][].class);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static Pair<Double, List<Double>> getPipsAndSizeMultipliers(BitfinexCurrencyPair symbol)  {
        
        try {
            // Sleep for 10 seconds to avoid triggering DoS protection
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }
        
        Double sizeMultiplier = null;
        List<Double> priceSteps = new ArrayList<>();
        
        for (OrderBookPrecision precision : OrderBookPrecision.values()) {
            double[][] orderBook = getOrderBook(symbol, precision);
            
            List<Double> sizes = Arrays.stream(orderBook)
                    .map(r -> Math.abs(r[2]))
                    .filter(r -> r != 0)
                    .collect(Collectors.toList());
            double minDistance = Double.POSITIVE_INFINITY;
            for (int i = 1; i < orderBook.length; ++i) {
                double distance = Math.abs(orderBook[i - 1][0] - orderBook[i][0]);
                minDistance = Math.min(distance, minDistance);
            }
            double minSize = sizes.stream().mapToDouble(a -> a).min().getAsDouble();

            System.out.println(minSize);
            System.out.println(minDistance);
            
            if (sizeMultiplier == null) {
                sizeMultiplier = 5 / minSize;
                // Rounding sizeMultiplier to next power of 10
                sizeMultiplier = Math.pow(10, Math.ceil(Math.log10(sizeMultiplier)));
                sizeMultiplier = Math.max(sizeMultiplier, 1);
            }
            
            // Rounding minDistance to get rid of precision errors
            minDistance = Math.round(minDistance * 1e9) / 1e9;
           
            
            priceSteps.add(minDistance);
        }
        return new ImmutablePair<>(sizeMultiplier, priceSteps);
    }

    public static void main(String[] args) throws IOException {
        
        List<ImmutablePair<BitfinexCurrencyPair, Pair<Double, List<Double>>>> 
            dataAboutSymbols = Arrays.stream(BitfinexCurrencyPair.values())
                .map(p -> new ImmutablePair<>(p, getPipsAndSizeMultipliers(p)))
                .collect(Collectors.toList());
        
        
        
        for (ImmutablePair<BitfinexCurrencyPair, Pair<Double, List<Double>>> symbolData : dataAboutSymbols) {
            BitfinexCurrencyPair symbol = symbolData.getLeft();
            Pair<Double, List<Double>> parameters = symbolData.getRight();
            System.out.println(String.format(
                    "amountMultipliers.put(BitfinexCurrencyPair.%s, %d);",
                        symbol.name(), Math.round(parameters.getLeft())));
        }
        for (ImmutablePair<BitfinexCurrencyPair, Pair<Double, List<Double>>> symbolData : dataAboutSymbols) {
            BitfinexCurrencyPair symbol = symbolData.getLeft();
            Pair<Double, List<Double>> parameters = symbolData.getRight();
            
            String arrayOfSteps = parameters.getRight().stream()
                .map(v -> v.toString())
                .collect(Collectors.joining(", "));
            
            System.out.println(String.format(
                    "priceStep.put(BitfinexCurrencyPair.%s, new double[]{%s});",
                        symbol.name(), arrayOfSteps));
        }
    }
}
