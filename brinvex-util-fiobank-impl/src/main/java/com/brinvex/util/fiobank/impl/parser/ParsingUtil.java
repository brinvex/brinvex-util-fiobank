package com.brinvex.util.fiobank.impl.parser;

import com.brinvex.util.fiobank.api.model.Currency;
import com.brinvex.util.fiobank.api.model.RawDirection;
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

    public static RawDirection toDirection(Lang lang, String direction) {
        if (direction == null || direction.isBlank()) {
            return null;
        }
        if (lang.equals(Lang.CZ)) {
            if (direction.equalsIgnoreCase(("Nákup"))) {
                return RawDirection.BUY;
            }
            if (direction.equalsIgnoreCase(("Prodej"))) {
                return RawDirection.SELL;
            }
            if (direction.equalsIgnoreCase("Bankovní převod")) {
                return RawDirection.BANK_TRANSFER;
            }
            if (direction.equalsIgnoreCase("Převod mezi měnami")) {
                return RawDirection.CURRENCY_CONVERSION;
            }
        }
        if (lang.equals(Lang.SK)) {
            if (direction.equalsIgnoreCase(("Nákup"))) {
                return RawDirection.BUY;
            }
            if (direction.equalsIgnoreCase(("Predaj"))) {
                return RawDirection.SELL;
            }
            if (direction.equalsIgnoreCase("Bankový prevod")) {
                return RawDirection.BANK_TRANSFER;
            }
            if (direction.equalsIgnoreCase("Prevod mezi menami")) {
                return RawDirection.CURRENCY_CONVERSION;
            }
        }
        if (lang.equals(Lang.EN)) {
            if (direction.equalsIgnoreCase(("Buy"))) {
                return RawDirection.BUY;
            }
            if (direction.equalsIgnoreCase(("Sell"))) {
                return RawDirection.SELL;
            }
            if (direction.equalsIgnoreCase("Bank transfer")) {
                return RawDirection.BANK_TRANSFER;
            }
            if (direction.equalsIgnoreCase("Currency Conversion")) {
                return RawDirection.CURRENCY_CONVERSION;
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