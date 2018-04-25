/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 Jan Kristof Nidzwetzki
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License. 
 *
 *******************************************************************************/
package bitfinex.entity;

public enum BitfinexCurrencyPair {

    BTC_USD,
    LTC_USD,
    LTC_BTC,
    ETH_USD,
    ETH_BTC,
    ETC_BTC,
    ETC_USD,
    RRT_USD,
    RRT_BTC,
    ZEC_USD,
    ZEC_BTC,
    XMR_USD,
    XMR_BTC,
    DSH_USD,
    DSH_BTC,
    BTC_EUR,
    XRP_USD,
    XRP_BTC,
    IOT_USD,
    IOT_BTC,
    IOT_ETH,
    EOS_USD,
    EOS_BTC,
    EOS_ETH,
    SAN_USD,
    SAN_BTC,
    SAN_ETH,
    OMG_USD,
    OMG_BTC,
    OMG_ETH,
    BCH_USD,
    BCH_BTC,
    BCH_ETH,
    NEO_USD,
    NEO_BTC,
    NEO_ETH,
    ETP_USD,
    ETP_BTC,
    ETP_ETH,
    QTM_USD,
    QTM_BTC,
    QTM_ETH,
    AVT_USD,
    AVT_BTC,
    AVT_ETH,
    EDO_USD,
    EDO_BTC,
    EDO_ETH,
    BTG_USD,
    BTG_BTC,
    DAT_USD,
    DAT_BTC,
    DAT_ETH,
    QSH_USD,
    QSH_BTC,
    QSH_ETH,
    YYW_USD,
    YYW_BTC,
    YYW_ETH,
    GNT_USD,
    GNT_BTC,
    GNT_ETH,
    SNT_USD,
    SNT_BTC,
    SNT_ETH,
    IOT_EUR,
    BAT_USD,
    BAT_BTC,
    BAT_ETH,
    MNA_USD,
    MNA_BTC,
    MNA_ETH,
    FUN_USD,
    FUN_BTC,
    FUN_ETH,
    ZRX_USD,
    ZRX_BTC,
    ZRX_ETH,
    TNB_USD,
    TNB_BTC,
    TNB_ETH,
    SPK_USD,
    SPK_BTC,
    SPK_ETH,
    TRX_BTC,
    TRX_ETH,
    TRX_USD,
    RCN_BTC,
    RCN_ETH,
    RCN_USD,
    RLC_BTC,
    RLC_ETH,
    RLC_USD,
    AID_BTC,
    AID_USD,
    AID_ETH,
    SNG_BTC,
    SNG_ETH,
    SNG_USD,
    REP_BTC,
    REP_ETH,
    REP_USD,
    ELF_BTC,
    ELF_ETH,
    ELF_USD;

    public static BitfinexCurrencyPair fromSymbolString(final String symbolString) {
        for (BitfinexCurrencyPair curency : BitfinexCurrencyPair.values()) {
            if (curency.toBitfinexString().equalsIgnoreCase(symbolString)) {
                return curency;
            }
        }
        throw new IllegalArgumentException("Unable to find currency pair for: " + symbolString);
    }

    public static boolean contains(String s) {
        for (BitfinexCurrencyPair pair : values()) {
            if (pair.name().equals(s)) return true;
        }
        return false;
    }

    public String toBitfinexString() {
        return "t" + toString().replace("_", "");
    }
}
