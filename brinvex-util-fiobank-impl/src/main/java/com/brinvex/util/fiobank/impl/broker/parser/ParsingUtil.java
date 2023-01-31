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

import com.brinvex.util.fiobank.api.model.Currency;
import com.brinvex.util.fiobank.api.model.RawBrokerTranDirection;
import com.brinvex.util.fiobank.api.model.Lang;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@SuppressWarnings({"SameParameterValue", "SpellCheckingInspection"})
public class ParsingUtil {

    private static class DateFormat {
        private static final DateTimeFormatter DAY_HOUR_MINUTE = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm");
        private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("d.M.yyyy");
    }

    public static LocalDateTime toLocalDateTime(String s, DateTimeFormatter dtf) {
        return s == null || s.isBlank() ? null : LocalDateTime.parse(s, dtf);
    }

    public static LocalDateTime toFioDateTime(String s) {
        return toLocalDateTime(s, DateFormat.DAY_HOUR_MINUTE);
    }

    public static LocalDate toLocalDate(String s, DateTimeFormatter dtf) {
        return s == null || s.isBlank() ? null : LocalDate.parse(s, dtf);
    }

    public static LocalDate toFioDate(String s) {
        return toLocalDate(s, DateFormat.DAY);
    }

    public static BigDecimal toDecimal(String s) {
        return s == null || s.isBlank() ? null : new BigDecimal(s.replace(" ", "").replace(',', '.'));
    }

    public static Currency toCurrency(String s) {
        if (s == null || s.isBlank()) {
            return null;
        }
        s = s.trim();
        for (Currency ccy : Currency.values()) {
            if (ccy.name().equalsIgnoreCase(s)) {
                return ccy;
            }
        }
        return null;
    }

    public static RawBrokerTranDirection toDirection(Lang lang, String direction) {
        if (direction == null || direction.isBlank()) {
            return null;
        }
        if (lang.equals(Lang.CZ)) {
            if (direction.equalsIgnoreCase(("Nákup"))) {
                return RawBrokerTranDirection.BUY;
            }
            if (direction.equalsIgnoreCase(("Prodej"))) {
                return RawBrokerTranDirection.SELL;
            }
            if (direction.equalsIgnoreCase("Bankovní převod")) {
                return RawBrokerTranDirection.BANK_TRANSFER;
            }
            if (direction.equalsIgnoreCase("Převod mezi měnami")) {
                return RawBrokerTranDirection.CURRENCY_CONVERSION;
            }
        }
        if (lang.equals(Lang.SK)) {
            if (direction.equalsIgnoreCase(("Nákup"))) {
                return RawBrokerTranDirection.BUY;
            }
            if (direction.equalsIgnoreCase(("Predaj"))) {
                return RawBrokerTranDirection.SELL;
            }
            if (direction.equalsIgnoreCase("Bankový prevod")) {
                return RawBrokerTranDirection.BANK_TRANSFER;
            }
            if (direction.equalsIgnoreCase("Prevod mezi menami")) {
                return RawBrokerTranDirection.CURRENCY_CONVERSION;
            }
        }
        if (lang.equals(Lang.EN)) {
            if (direction.equalsIgnoreCase(("Buy"))) {
                return RawBrokerTranDirection.BUY;
            }
            if (direction.equalsIgnoreCase(("Sell"))) {
                return RawBrokerTranDirection.SELL;
            }
            if (direction.equalsIgnoreCase("Bank transfer")) {
                return RawBrokerTranDirection.BANK_TRANSFER;
            }
            if (direction.equalsIgnoreCase("Currency Conversion")) {
                return RawBrokerTranDirection.CURRENCY_CONVERSION;
            }
        }
        throw new IllegalArgumentException(String.format("Unexpected %s value: '%s'", lang, direction));
    }

    public static String stripToNull(String s) {
        if (s == null) {
            return null;
        }
        s = s.trim();
        if (s.isEmpty()) {
            return null;
        }
        return s;
    }
}
