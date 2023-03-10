/*
 * Copyright © 2023 Brinvex (dev@brinvex.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.brinvex.util.fiobank.impl.broker.parser;

import com.brinvex.util.fiobank.api.model.Lang;
import com.brinvex.util.fiobank.api.model.RawBrokerTransaction;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;


@SuppressWarnings("SpellCheckingInspection")
public enum TranColumnDef {

    TRADE_DATE("Dátum obchodu", "Datum obchodu", "Trade Date", ParsingUtil::toFioDateTime, RawBrokerTransaction::setTradeDate),
    DIRECTION("Smer", "Směr", "Direction", ParsingUtil::toDirection, RawBrokerTransaction::setDirection),
    SYMBOL("Symbol", "Symbol", "Symbol", ParsingUtil::stripToNull, TranColumnDef::fillSymbol),
    PRICE("Cena", "Cena", "Price", ParsingUtil::toDecimal, RawBrokerTransaction::setPrice),
    SHARES("Počet", "Počet", "Shares", ParsingUtil::toDecimal, RawBrokerTransaction::setShares),
    CURRENCY("Mena", "Měna", "Currency", ParsingUtil::stripToNull, TranColumnDef::fillCurrency),
    VOLUME_CZK("Objem v CZK", "Objem v CZK", "Volume (CZK)", ParsingUtil::toDecimal, RawBrokerTransaction::setVolumeCzk),
    FEES_CZK("Poplatky v CZK", "Poplatky v CZK", "Fees", ParsingUtil::toDecimal, RawBrokerTransaction::setFeesCzk),
    VOLUME_USD("Objem v USD", "Objem v USD", "Volume in USD", ParsingUtil::toDecimal, RawBrokerTransaction::setVolumeUsd),
    FEES_USD("Poplatky v USD", "Poplatky v USD", "Fees (USD)", ParsingUtil::toDecimal, RawBrokerTransaction::setFeesUsd),
    VOLUME_EUR("Objem v EUR", "Objem v EUR", "Volume (EUR)", ParsingUtil::toDecimal, RawBrokerTransaction::setVolumeEur),
    FEES_EUR("Poplatky v EUR", "Poplatky v EUR", "Fees (EUR)", ParsingUtil::toDecimal, RawBrokerTransaction::setFeesEur),
    MARKET("Trh", "Trh", "Market", ParsingUtil::stripToNull, RawBrokerTransaction::setMarket),
    INSTRUMENT_NAME("Názov FN", "Název CP", "Title", ParsingUtil::stripToNull, RawBrokerTransaction::setInstrumentName),
    SETTLEMENT_DATE("Dátum vysporiadania", "Datum vypořádání", "Settlement Date", ParsingUtil::toFioDate, RawBrokerTransaction::setSettlementDate),
    STATUS("Stav", "Stav", "Status", ParsingUtil::stripToNull, RawBrokerTransaction::setStatus),
    ORDER_ID("Pokyn ID", "Pokyn ID", "Order ID", ParsingUtil::stripToNull, RawBrokerTransaction::setOrderId),
    TEXT("Text FIO", "Text FIO", "Text FIO", ParsingUtil::stripToNull, RawBrokerTransaction::setText),
    USER_COMMENTS("Užívateľská identifikácia", "Uživatelská identifikace", "User Comments", ParsingUtil::stripToNull, RawBrokerTransaction::setUserComments),
    ;

    public static final Set<TranColumnDef> STANDARD_COLUMNS = Set.of(
            TRADE_DATE,
            DIRECTION,
            SYMBOL,
            PRICE,
            SHARES,
            CURRENCY,
            VOLUME_CZK,
            FEES_CZK,
            VOLUME_USD,
            FEES_USD,
            VOLUME_EUR,
            FEES_EUR,
            TEXT
    );

    private final String titleSK;

    private final String titleCZ;

    private final String titleEN;

    private final BiFunction<Lang, String, ?> mapper;

    private final BiConsumer<RawBrokerTransaction, ?> filler;

    <T> TranColumnDef(
            String titleSK,
            String titleCZ,
            String titleEN,
            BiFunction<Lang, String, T> mapper,
            BiConsumer<RawBrokerTransaction, T> filler
    ) {
        this.titleSK = titleSK;
        this.titleCZ = titleCZ;
        this.titleEN = titleEN;
        this.mapper = mapper;
        this.filler = filler;
    }

    <T> TranColumnDef(
            String titleSK,
            String titleCZ,
            String titleEN,
            Function<String, T> mapper,
            BiConsumer<RawBrokerTransaction, T> filler
    ) {
        this(titleSK, titleCZ, titleEN, (lang, s) -> mapper.apply(s), filler);
    }

    public static TranColumnDef ofTitle(String title, Lang lang) {
        for (TranColumnDef value : values()) {
            if (value.getTitle(lang).equals(title)) {
                return value;
            }
        }
        return null;
    }

    public String getTitle(Lang lang) {
        switch (lang) {
            case CZ:
                return titleCZ;
            case EN:
                return titleEN;
            case SK:
                return titleSK;
        }
        throw new IllegalArgumentException("Unexpected value: " + lang);
    }

    private static void fillSymbol(RawBrokerTransaction rawTransaction, String rawSymbol) {
        if (rawSymbol != null && !rawSymbol.isBlank()) {
            rawTransaction.setRawSymbol(rawSymbol);
            rawTransaction.setSymbol(rawSymbol.endsWith("*") ? rawSymbol.substring(0, rawSymbol.length() - 1) : rawSymbol);
        }
    }

    private static void fillCurrency(RawBrokerTransaction rawTransaction, String rawCurrency) {
        rawTransaction.setRawCurrency(rawCurrency);
        rawTransaction.setCcy(ParsingUtil.toCurrency(rawCurrency));
    }

    public void fill(RawBrokerTransaction rawTransaction, String cell, Lang lang) {
        Object value = mapper.apply(lang, cell);
        @SuppressWarnings("unchecked")
        BiConsumer<RawBrokerTransaction, Object> filler = (BiConsumer<RawBrokerTransaction, Object>) this.filler;
        filler.accept(rawTransaction, value);
    }
}
