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
package com.brinvex.util.fiobank.impl.broker;

import com.brinvex.util.fiobank.api.model.Country;
import com.brinvex.util.fiobank.api.model.Currency;
import com.brinvex.util.fiobank.api.model.Lang;
import com.brinvex.util.fiobank.api.model.RawBrokerTranDirection;
import com.brinvex.util.fiobank.api.model.RawBrokerTransaction;
import com.brinvex.util.fiobank.api.model.Transaction;
import com.brinvex.util.fiobank.api.model.TransactionType;
import com.brinvex.util.fiobank.api.service.exception.FiobankServiceException;
import com.brinvex.util.fiobank.impl.broker.parser.ParsingUtil;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.brinvex.util.fiobank.impl.util.ValidationUtil.assertEqual;
import static com.brinvex.util.fiobank.impl.util.ValidationUtil.assertIsNegative;
import static com.brinvex.util.fiobank.impl.util.ValidationUtil.assertIsPositive;
import static com.brinvex.util.fiobank.impl.util.ValidationUtil.assertIsZero;
import static com.brinvex.util.fiobank.impl.util.ValidationUtil.assertIsZeroOrNegative;
import static com.brinvex.util.fiobank.impl.util.ValidationUtil.assertIsZeroOrPositive;
import static com.brinvex.util.fiobank.impl.util.ValidationUtil.assertNotNull;
import static com.brinvex.util.fiobank.impl.util.ValidationUtil.assertNull;
import static com.brinvex.util.fiobank.impl.util.ValidationUtil.assertTrue;
import static java.lang.String.format;
import static java.math.BigDecimal.ZERO;
import static java.util.Objects.requireNonNullElse;
import static java.util.Optional.ofNullable;

@SuppressWarnings({"DuplicatedCode", "SpellCheckingInspection"})
public class FioBrokerTransactionMapper {

    private static class LazyHolder {
        private static final ZoneId FIO_TIME_ZONE = ZoneId.of("Europe/Prague");

        private static final Pattern DIVIDEND_TAX_RATE_PATTERN = Pattern.compile("\\(((čistá)|(po\\s+zdanění)),\\s+daň\\s(?<taxRate>\\d+(,\\d+)?)\\s*%\\)");

        private static final DateTimeFormatter ID_DATE_FORMAT = DateTimeFormatter.ofPattern("yyMMddhhmmss");
    }

    public List<Transaction> mapTransactions(
            Transaction prevTran,
            List<RawBrokerTransaction> rawTransToProcess,
            Function<String, Country> symbolCountryProvider
    ) {
        RawBrokerTransaction rawTran = rawTransToProcess.remove(0);

        LinkedList<Transaction> resultTrans = new LinkedList<>();

        LocalDateTime tranDate = rawTran.getTradeDate();
        ZonedDateTime tranZonedDate = tranDate.atZone(LazyHolder.FIO_TIME_ZONE);

        TransactionType tranType = detectTranType(rawTran);
        Country country = detectCountry(rawTran);
        BigDecimal rawValue = getValue(rawTran);
        BigDecimal fees = getFees(rawTran);
        BigDecimal qty = rawTran.getShares();
        RawBrokerTranDirection direction = rawTran.getDirection();
        String symbol = rawTran.getSymbol();
        Currency ccy = rawTran.getCcy();
        String rawCcy = rawTran.getRawCurrency();
        BigDecimal price = rawTran.getPrice();
        String text = rawTran.getText();
        LocalDate settlDate = rawTran.getSettlementDate();

        TransactionType nextTranType;
        Country nextCountry;
        BigDecimal nextRawValue;
        BigDecimal nextFees;
        BigDecimal nextQty;
        RawBrokerTranDirection nextDirection;
        String nextSymbol;
        Currency nextCcy;
        BigDecimal nextPrice;
        String nextText;
        {
            RawBrokerTransaction nextRawTran = rawTransToProcess.isEmpty() ? null : rawTransToProcess.get(0);
            if (nextRawTran != null
                && tranDate.isEqual(nextRawTran.getTradeDate())
                && settlDate.isEqual(nextRawTran.getSettlementDate())
            ) {
                nextTranType = detectTranType(nextRawTran);
                nextCountry = detectCountry(nextRawTran);
                nextRawValue = getValue(nextRawTran);
                nextFees = getFees(nextRawTran);
                nextQty = nextRawTran.getShares();
                nextDirection = nextRawTran.getDirection();
                nextSymbol = nextRawTran.getSymbol();
                nextCcy = nextRawTran.getCcy();
                nextPrice = nextRawTran.getPrice();
                nextText = nextRawTran.getText();
            } else {
                nextTranType = null;
                nextCountry = null;
                nextRawValue = null;
                nextFees = null;
                nextQty = null;
                nextDirection = null;
                nextSymbol = null;
                nextCcy = null;
                nextPrice = null;
                nextText = null;
            }
        }

        Supplier<Transaction> tranInitializer = () -> {
            Transaction t = new Transaction();
            t.setDate(tranZonedDate);
            t.setSettlementDate(settlDate);
            t.setCcy(ccy);
            t.setNote(text);
            t.setQty(BigDecimal.ZERO);
            t.setFees(BigDecimal.ZERO);
            resultTrans.add(t);
            return t;
        };

        if (tranType == TransactionType.DEPOSIT) {
            assertTrue(direction == null || RawBrokerTranDirection.BANK_TRANSFER.equals(direction));
            assertNull(symbol);
            assertNull(price);
            assertIsZero(qty);
            assertIsPositive(rawValue);
            assertIsZeroOrNegative(fees);

            Transaction t = tranInitializer.get();
            t.setType(TransactionType.DEPOSIT);
            t.setGrossValue(rawValue);
            t.setNetValue(rawValue.add(fees));
            t.setCcy(ccy);
            t.setQty(BigDecimal.ZERO);
            t.setFees(fees);

        } else if (tranType == TransactionType.WITHDRAWAL) {
            assertTrue(direction == null || RawBrokerTranDirection.BANK_TRANSFER.equals(direction));
            assertNull(symbol);
            assertNull(price);
            assertIsZero(qty);
            assertIsNegative(rawValue);
            assertIsZero(fees);

            if (TransactionType.FEE.equals(nextTranType) && "Poplatek za převod peněz".equals(nextText)) {
                assertEqual(nextFees, nextRawValue);
                assertIsNegative(nextFees);
                fees = nextFees;
                rawTransToProcess.remove(0);
            }
            Transaction t = tranInitializer.get();
            t.setType(TransactionType.WITHDRAWAL);
            t.setGrossValue(rawValue);
            t.setNetValue(rawValue.add(fees));
            t.setFees(fees);

        } else if (tranType == TransactionType.BUY) {
            assertTrue(direction.equals(RawBrokerTranDirection.BUY));
            assertNotNull(country);
            assertNotNull(symbol);
            assertIsPositive(price);
            assertIsPositive(qty);
            assertNotNull(ccy);
            assertIsNegative(rawValue);
            assertIsZeroOrNegative(fees);

            Transaction t = tranInitializer.get();
            t.setType(TransactionType.BUY);
            t.setGrossValue(rawValue.subtract(fees));
            t.setNetValue(rawValue);
            t.setCountry(country);
            t.setSymbol(symbol);
            t.setQty(qty);
            t.setPrice(price);
            t.setFees(fees);

        } else if (tranType == TransactionType.SELL) {
            assertTrue(direction.equals(RawBrokerTranDirection.SELL));
            assertNotNull(country);
            assertNotNull(symbol);
            assertIsPositive(price);
            assertIsZeroOrPositive(qty);
            assertNotNull(ccy);
            assertIsPositive(rawValue);
            assertIsZeroOrNegative(fees);

            Transaction t = tranInitializer.get();
            t.setType(TransactionType.SELL);
            t.setCountry(country);
            t.setSymbol(symbol);
            t.setQty(qty.negate());
            t.setPrice(price);
            t.setGrossValue(rawValue.subtract(fees));
            t.setNetValue(rawValue);
            t.setFees(fees);

        } else if (tranType == TransactionType.FX_BUY) {
            assertTrue(direction.equals(RawBrokerTranDirection.CURRENCY_CONVERSION));
            assertNotNull(symbol);
            assertIsPositive(price);
            assertIsPositive(qty);
            assertNotNull(ccy);
            assertIsNegative(rawValue);
            assertIsZero(fees);

            Transaction t = tranInitializer.get();
            t.setType(TransactionType.FX_BUY);
            t.setGrossValue(rawValue);
            t.setNetValue(rawValue);
            t.setSymbol(symbol);
            t.setQty(qty);
            t.setPrice(price);
            t.setFees(fees);

        } else if (tranType == TransactionType.FX_SELL) {
            assertTrue(direction.equals(RawBrokerTranDirection.CURRENCY_CONVERSION));
            assertNotNull(symbol);
            assertIsPositive(price);
            assertIsPositive(qty);
            assertNotNull(ccy);
            assertIsPositive(rawValue);
            assertIsZero(fees);

            Transaction t = tranInitializer.get();
            t.setType(TransactionType.FX_SELL);
            t.setGrossValue(rawValue);
            t.setNetValue(rawValue);
            t.setSymbol(symbol);
            t.setQty(qty.negate());
            t.setPrice(price);
            t.setFees(fees);

        } else if (tranType == TransactionType.CASH_DIVIDEND) {
            assertNull(direction);
            assertNotNull(symbol);
            assertNotNull(ccy);
            assertEqual(BigDecimal.ONE, price);
            assertIsPositive(qty);
            assertIsPositive(rawValue);
            assertIsZero(fees);

            BigDecimal tax = null;
            BigDecimal grossValue = rawValue;
            if (TransactionType.TAX.equals(nextTranType) && symbol.equals(nextSymbol)) {
                assertEqual(ccy, nextCcy);
                assertEqual(nextPrice, BigDecimal.ONE);
                assertIsZero(nextFees);
                assertIsNegative(nextRawValue);
                assertEqual(nextRawValue, nextQty);
                tax = nextRawValue;
                rawTransToProcess.remove(0);
            } else {
                Matcher m = LazyHolder.DIVIDEND_TAX_RATE_PATTERN.matcher(text);
                if (m.find()) {
                    BigDecimal taxRate = ParsingUtil.toDecimal(m.group("taxRate"))
                            .divide(new BigDecimal("100.00"), 6, RoundingMode.HALF_UP);
                    assertIsPositive(taxRate);
                    assertEqual(rawValue, qty);
                    tax = rawValue
                            .divide(BigDecimal.ONE.subtract(taxRate), 6, RoundingMode.HALF_UP)
                            .multiply(taxRate)
                            .negate()
                            .setScale(2, RoundingMode.HALF_UP);
                    grossValue = rawValue.subtract(tax);
                }
            }
            if (TransactionType.FEE.equals(nextTranType) && "Poplatek za připsání dividend".equals(nextText)) {
                assertEqual(nextCcy, ccy);
                assertEqual(nextRawValue, nextFees);
                fees = nextRawValue;
                assertIsNegative(fees);
                rawTransToProcess.remove(0);
            }

            Transaction t = tranInitializer.get();
            t.setType(TransactionType.CASH_DIVIDEND);
            t.setGrossValue(grossValue);
            t.setNetValue(grossValue.add(Objects.requireNonNullElse(tax, BigDecimal.ZERO)).add(fees));
            t.setCountry(country);
            t.setSymbol(symbol);
            t.setFees(fees);
            t.setTax(tax);

        } else if (tranType == TransactionType.CAPITAL_DIVIDEND) {
            assertTrue(symbol != null);
            assertTrue(ccy != null);
            assertTrue(price.compareTo(BigDecimal.ONE) == 0);
            assertIsPositive(qty);
            assertIsPositive(rawValue);
            assertIsZero(fees);

            Transaction t = tranInitializer.get();
            t.setType(TransactionType.CAPITAL_DIVIDEND);
            t.setCountry(country);
            t.setSymbol(symbol);
            t.setCcy(ccy);
            t.setGrossValue(rawValue);
            t.setNetValue(rawValue);

        } else if (tranType == TransactionType.STOCK_DIVIDEND) {
            assertNull(direction);
            assertNotNull(symbol);
            assertEqual(BigDecimal.ONE, price);
            assertIsPositive(qty);
            assertIsZeroOrNegative(fees);

            if (String.format("%s - Stock Dividend", symbol).equals(text)) {
                assertIsZero(fees);
                assertNull(country);
                country = symbolCountryProvider.apply(symbol);
                Transaction t = tranInitializer.get();
                t.setType(TransactionType.STOCK_DIVIDEND);
                t.setCountry(country);
                t.setSymbol(symbol);
                t.setQty(qty);
                t.setNetValue(ZERO);
                t.setGrossValue(ZERO);
                t.setCcy(country.getCcy());
                t.setFees(fees);
            } else if (String.format("%s - Finanční kompenzace - Stock Dividend", symbol).equals(text)) {
                assertIsZero(fees);
                assertEqual(rawValue, qty);
                assertNotNull(country);
                Transaction t = tranInitializer.get();
                t.setType(TransactionType.STOCK_DIVIDEND);
                t.setGrossValue(rawValue);
                t.setNetValue(rawValue);
                t.setCountry(country);
                t.setSymbol(symbol);
                t.setQty(BigDecimal.ZERO);
            } else {
                throw new FiobankServiceException("Unsupported: " + text);
            }

        } else if (tranType == TransactionType.DIVIDEND_REVERSAL) {
            assertNull(direction);
            assertNotNull(country);
            assertNotNull(symbol);
            assertNotNull(ccy);
            assertEqual(BigDecimal.ONE, price);
            assertIsNegative(qty);
            assertIsNegative(rawValue);
            assertIsZeroOrNegative(fees);

            BigDecimal taxRefund;
            {
                assertTrue(TransactionType.TAX_REFUND.equals(nextTranType));
                assertTrue(symbol.equals(nextSymbol));
                assertTrue(ccy.equals(nextCcy));
                assertTrue(nextPrice.compareTo(BigDecimal.ONE) == 0);
                assertIsZero(nextFees);
                assertIsPositive(nextRawValue);
                assertEqual(nextRawValue, nextQty);
                taxRefund = nextRawValue;
                rawTransToProcess.remove(0);
            }
            Transaction t = tranInitializer.get();
            t.setType(TransactionType.DIVIDEND_REVERSAL);
            t.setGrossValue(rawValue);
            t.setNetValue(rawValue.add(taxRefund).add(fees));
            t.setCountry(country);
            t.setSymbol(symbol);
            t.setFees(fees);
            t.setTax(taxRefund);

        } else if (tranType == TransactionType.FEE) {
            assertNull(direction);
            assertTrue(symbol != null || qty.compareTo(BigDecimal.ZERO) == 0);
            assertNotNull(ccy);
            assertIsZeroOrNegative(rawValue);
            assertIsZeroOrNegative(fees);
            assertTrue(price == null || price.compareTo(BigDecimal.ONE) == 0);

            BigDecimal value;
            if (fees.compareTo(BigDecimal.ZERO) == 0) {
                value = rawValue;
            } else if (rawValue.compareTo(BigDecimal.ZERO) == 0) {
                value = fees;
            } else if (rawValue.equals(fees)) {
                value = rawValue;
            } else {
                throw new FiobankServiceException(String.format("fees=%s, rawValue=%s", fees, rawValue));
            }
            if (value.compareTo(BigDecimal.ZERO) != 0) {
                Transaction t = tranInitializer.get();
                t.setType(TransactionType.FEE);
                t.setGrossValue(BigDecimal.ZERO);
                t.setNetValue(value);
                t.setFees(value);
                t.setSymbol(symbol);
                t.setCountry(country);
            }

        } else if (tranType == TransactionType.RECLAMATION) {
            assertNull(direction);
            assertNull(symbol);
            assertTrue(qty == null || qty.compareTo(BigDecimal.ZERO) == 0);
            assertNotNull(ccy);
            assertIsZeroOrPositive(rawValue);
            assertTrue(fees.compareTo(BigDecimal.ZERO) == 0 || fees.compareTo(rawValue) == 0);

            Transaction t = tranInitializer.get();
            t.setType(TransactionType.RECLAMATION);
            t.setPrice(price);
            t.setGrossValue(rawValue);
            t.setNetValue(rawValue);

        } else if (tranType == TransactionType.INSTRUMENT_CHANGE_CHILD) {
            assertTrue(nextTranType == TransactionType.INSTRUMENT_CHANGE_PARENT);
            assertTrue(text.equals(nextText));
            assertIsPositive(qty);
            assertEqual(qty, nextQty);
            assertIsZero(price);
            assertIsZero(nextPrice);
            assertIsZero(rawValue);
            assertIsZero(nextRawValue);
            assertIsZero(fees);
            assertIsZero(nextFees);
            assertNull(country);
            assertNull(nextCountry);
            assertNull(ccy);
            assertNull(nextCcy);

            Country positionCountry = symbolCountryProvider.apply(nextSymbol);
            Currency countryCcy = positionCountry == null ? null : positionCountry.getCcy();
            Transaction t1;
            {
                assertTrue(RawBrokerTranDirection.SELL.equals(nextDirection));
                t1 = tranInitializer.get();
                t1.setType(TransactionType.INSTRUMENT_CHANGE_PARENT);
                t1.setNetValue(BigDecimal.ZERO);
                t1.setGrossValue(BigDecimal.ZERO);
                t1.setCountry(positionCountry);
                t1.setCcy(countryCcy);
                t1.setSymbol(nextSymbol);
                t1.setQty(qty.negate());
                t1.setId(generateTranId(t1, prevTran));
                t1.setBunchId(t1.getId());
            }
            {
                assertTrue(RawBrokerTranDirection.BUY.equals(direction));
                Transaction t2 = tranInitializer.get();
                t2.setType(TransactionType.INSTRUMENT_CHANGE_CHILD);
                t2.setNetValue(BigDecimal.ZERO);
                t2.setGrossValue(BigDecimal.ZERO);
                t2.setCountry(positionCountry);
                t2.setCcy(countryCcy);
                t2.setSymbol(symbol);
                t2.setQty(qty);
                t2.setBunchId(t1.getId());
            }
            rawTransToProcess.remove(0);
        } else if (tranType == TransactionType.INSTRUMENT_CHANGE_PARENT) {
            assertTrue(nextTranType == TransactionType.INSTRUMENT_CHANGE_CHILD);
            assertTrue(text.equals(nextText));
            assertIsPositive(qty);
            assertEqual(qty, nextQty);
            assertIsZero(price);
            assertIsZero(nextPrice);
            assertIsZero(rawValue);
            assertIsZero(nextRawValue);
            assertIsZero(fees);
            assertIsZero(nextFees);
            assertNull(ccy);
            assertNull(nextCcy);

            country = symbolCountryProvider.apply(symbol);
            Currency countryCcy = country == null ? null : country.getCcy();

            if (nextCountry == null) {
                nextCountry = country;
            }

            Transaction t1;
            {
                assertTrue(RawBrokerTranDirection.SELL.equals(direction));
                t1 = tranInitializer.get();
                t1.setType(TransactionType.INSTRUMENT_CHANGE_PARENT);
                t1.setNetValue(BigDecimal.ZERO);
                t1.setGrossValue(BigDecimal.ZERO);
                t1.setCountry(country);
                t1.setCcy(countryCcy);
                t1.setSymbol(symbol);
                t1.setQty(qty.negate());
                t1.setId(generateTranId(t1, prevTran));
                t1.setBunchId(t1.getId());
            }
            {
                assertTrue(RawBrokerTranDirection.BUY.equals(nextDirection));
                Transaction t2 = tranInitializer.get();
                t2.setType(TransactionType.INSTRUMENT_CHANGE_CHILD);
                t2.setNetValue(BigDecimal.ZERO);
                t2.setGrossValue(BigDecimal.ZERO);
                t2.setCountry(nextCountry);
                t2.setCcy(countryCcy);
                t2.setSymbol(nextSymbol);
                t2.setQty(qty);
                t2.setBunchId(t1.getId());
            }
            rawTransToProcess.remove(0);
        } else if (tranType == TransactionType.TAX_REFUND) {
            assertTrue(symbol != null);
            assertTrue(ccy != null);
            assertEqual(price, BigDecimal.ONE);
            assertIsPositive(qty);
            assertEqual(rawValue, qty);
            assertIsZero(fees);
            {
                Transaction t = tranInitializer.get();
                t.setType(TransactionType.TAX_REFUND);
                t.setCountry(country);
                t.setSymbol(symbol);
                t.setGrossValue(BigDecimal.ZERO);
                t.setNetValue(rawValue);
                t.setTax(rawValue);
            }
        } else if (tranType == TransactionType.TAX) {
            assertNull(direction);
            assertNotNull(symbol);
            assertNotNull(ccy);
            assertEqual(price, BigDecimal.ONE);
            assertIsZeroOrNegative(qty);
            assertTrue(rawValue.compareTo(qty) <= 0);
            assertIsZero(fees);
            {
                Transaction t = tranInitializer.get();
                t.setType(TransactionType.TAX);
                t.setCountry(country);
                t.setSymbol(symbol);
                t.setGrossValue(BigDecimal.ZERO);
                t.setNetValue(rawValue);
                t.setTax(rawValue);
            }
        } else if (tranType == TransactionType.LIQUIDATION) {
            assertNotNull(symbol);
            assertTrue(price.compareTo(BigDecimal.ONE) == 0 || price.compareTo(BigDecimal.ZERO) == 0);
            assertIsPositive(qty);
            assertTrue(rawValue.compareTo(BigDecimal.ZERO) == 0 || rawValue.compareTo(qty) == 0);
            assertIsZero(fees);

            if (TransactionType.LIQUIDATION.equals(nextTranType)) {
                assertNotNull(country);
                assertTrue(nextDirection.equals(RawBrokerTranDirection.SELL));
                assertTrue(nextSymbol.equals(symbol));
                assertTrue(nextCcy.equals(ccy));
                assertIsZero(nextPrice);
                assertIsPositive(nextQty);
                assertIsZero(nextRawValue);
                assertIsZero(nextFees);
                rawTransToProcess.remove(0);

                Transaction t = tranInitializer.get();
                t.setType(TransactionType.LIQUIDATION);
                t.setCountry(country);
                t.setSymbol(symbol);
                t.setCcy(ccy);
                t.setGrossValue(qty);
                t.setNetValue(qty);
                t.setQty(nextQty.negate());
                t.setPrice(BigDecimal.ZERO);
            } else {
                assertNull(country);
                assertNull(ccy);
                country = symbolCountryProvider.apply(symbol);
                Currency countryCcy = country == null ? null : country.getCcy();

                Transaction t = tranInitializer.get();
                t.setType(TransactionType.LIQUIDATION);
                t.setCountry(country);
                t.setSymbol(symbol);
                t.setCcy(countryCcy);
                t.setGrossValue(BigDecimal.ZERO);
                t.setNetValue(BigDecimal.ZERO);
                t.setQty(qty.negate());
                t.setPrice(BigDecimal.ZERO);

            }
        } else if (tranType == TransactionType.SPINOFF_PARENT) {
            assertNull(direction);
            assertNull(country);
            assertNotNull(symbol);
            assertNull(ccy);
            assertNotNull(rawCcy);
            assertEqual(BigDecimal.ONE, price);
            assertIsPositive(qty);
            assertIsZero(rawValue);
            assertIsZero(fees);

            country = symbolCountryProvider.apply(symbol);
            Currency positionCcy = country == null ? null : country.getCcy();

            Transaction t1;
            {
                t1 = tranInitializer.get();
                t1.setType(TransactionType.SPINOFF_PARENT);
                t1.setCcy(positionCcy);
                t1.setCountry(country);
                t1.setSymbol(symbol);
                t1.setId(generateTranId(t1, prevTran));
                t1.setBunchId(t1.getId());
            }
            {
                Transaction t2 = tranInitializer.get();
                t2.setType(TransactionType.SPINOFF_CHILD);
                t2.setCcy(positionCcy);
                t2.setCountry(country);
                t2.setSymbol(rawCcy);
                t2.setQty(qty);
                t2.setBunchId(t1.getId());
            }
        } else if (tranType == TransactionType.MERGER_CHILD) {
            assertTrue(direction == RawBrokerTranDirection.BUY);
            assertNotNull(symbol);
            assertNull(ccy);
            assertNull(rawCcy);
            assertIsZero(price);
            assertIsPositive(qty);
            assertIsZero(rawValue);
            assertIsZero(fees);

            assertTrue(TransactionType.MERGER_PARENT.equals(nextTranType));
            assertTrue(nextDirection == RawBrokerTranDirection.SELL);
            assertIsZero(nextRawValue);
            assertIsZero(nextFees);
            assertIsZero(nextPrice);
            assertIsPositive(nextQty);

            assertNull(country);
            country = symbolCountryProvider.apply(nextSymbol);
            Currency countryCcy = country == null ? null : country.getCcy();
            Transaction t1;
            {
                t1 = tranInitializer.get();
                t1.setType(TransactionType.MERGER_PARENT);
                t1.setCcy(countryCcy);
                t1.setCountry(country);
                t1.setSymbol(nextSymbol);
                t1.setQty(nextQty.negate());
                t1.setId(generateTranId(t1, prevTran));
                t1.setBunchId(t1.getId());
            }
            {
                Transaction t2 = tranInitializer.get();
                t2.setType(TransactionType.MERGER_CHILD);
                t2.setCcy(countryCcy);
                t2.setCountry(country);
                t2.setSymbol(symbol);
                t2.setQty(qty);
                t2.setBunchId(t1.getId());
            }
            rawTransToProcess.remove(0);
        } else if (tranType == TransactionType.SPINOFF_VALUE) {
            assertNull(direction);
            assertNotNull(symbol);
            assertNotNull(ccy);
            assertNotNull(rawCcy);
            assertEqual(BigDecimal.ONE, price);
            assertEqual(rawValue, qty);
            assertIsPositive(rawValue);
            assertIsZero(fees);

            assertTrue(TransactionType.SPINOFF_VALUE.equals(nextTranType));
            assertTrue(nextDirection == null);
            assertTrue(nextSymbol.equals(symbol));
            assertTrue(nextRawValue.negate().equals(rawValue));
            assertTrue(nextQty.negate().equals(qty));
            assertIsZero(nextFees);
            assertTrue(price.compareTo(BigDecimal.ONE) == 0);
            rawTransToProcess.remove(0);
            Transaction t1;
            {
                t1 = tranInitializer.get();
                t1.setType(TransactionType.SPINOFF_VALUE);
                t1.setCountry(country);
                t1.setSymbol(symbol);
                t1.setGrossValue(rawValue);
                t1.setNetValue(rawValue);
                t1.setId(generateTranId(t1, prevTran));
                t1.setBunchId(t1.getId());
            }
            {
                Transaction t2 = tranInitializer.get();
                t2.setType(TransactionType.SPINOFF_VALUE);
                t2.setCountry(country);
                t2.setSymbol(symbol);
                t2.setGrossValue(rawValue.negate());
                t2.setNetValue(rawValue.negate());
                t2.setBunchId(t1.getId());
            }
        } else if (tranType == TransactionType.SPLIT) {
            assertNull(country);
            assertNull(ccy);
            country = symbolCountryProvider.apply(symbol);
            Currency countryCcy = country == null ? null : country.getCcy();

            if (TransactionType.SPLIT.equals(nextTranType) && nextSymbol.equals(symbol)) {
                assertTrue(direction == RawBrokerTranDirection.SELL);
                assertTrue(nextDirection == RawBrokerTranDirection.BUY);
                assertNull(nextCcy);
                assertIsZero(nextPrice);
                assertIsPositive(nextQty);
                assertIsZero(nextRawValue);
                assertIsZero(nextFees);
                qty = qty.negate().add(nextQty);
                rawTransToProcess.remove(0);
            }
            {
                Transaction t = tranInitializer.get();
                t.setType(TransactionType.SPLIT);
                t.setCcy(countryCcy);
                t.setGrossValue(BigDecimal.ZERO);
                t.setNetValue(BigDecimal.ZERO);
                t.setCountry(country);
                t.setSymbol(symbol);
                t.setSymbol(symbol);
                t.setQty(qty);
            }
        } else {
            throw new FiobankServiceException(String.format("Unsupported transaction: %s", rawTran));
        }

        for (Transaction resultTran : resultTrans) {
            if (resultTran.getId() == null) {
                resultTran.setId(generateTranId(resultTran, prevTran));
                prevTran = resultTran;
            }
        }

        return resultTrans;
    }

    @SuppressWarnings("DuplicatedCode")
    protected BigDecimal getValue(RawBrokerTransaction rawTran) {
        Currency ccy = rawTran.getCcy();
        BigDecimal rawValue = null;
        if (ccy != null) {
            BigDecimal volUsd = rawTran.getVolumeUsd();
            BigDecimal volEur = rawTran.getVolumeEur();
            BigDecimal volCzk = rawTran.getVolumeCzk();
            if (ccy == Currency.USD) {
                assertTrue(volEur == null);
                assertTrue(volCzk == null);
                rawValue = volUsd;
            } else if (ccy == Currency.EUR) {
                assertTrue(volUsd == null);
                assertTrue(volCzk == null);
                rawValue = volEur;
            } else if (ccy == Currency.CZK) {
                assertTrue(volUsd == null);
                assertTrue(volEur == null);
                rawValue = volCzk;
            } else {
                throw new IllegalStateException("Unexpected value: " + ccy);
            }
        }
        return requireNonNullElse(rawValue, ZERO);
    }

    @SuppressWarnings("DuplicatedCode")
    protected BigDecimal getFees(RawBrokerTransaction rawTran) {
        Currency ccy = rawTran.getCcy();
        BigDecimal rawFees = null;
        if (ccy != null) {
            BigDecimal feesUsd = rawTran.getFeesUsd();
            BigDecimal feesEur = rawTran.getFeesEur();
            BigDecimal feesCzk = rawTran.getFeesCzk();
            if (ccy == Currency.USD) {
                assertTrue(feesEur == null);
                assertTrue(feesCzk == null);
                rawFees = feesUsd;
            } else if (ccy == Currency.EUR) {
                assertTrue(feesUsd == null);
                assertTrue(feesCzk == null);
                rawFees = feesEur;
            } else if (ccy == Currency.CZK) {
                assertTrue(feesUsd == null);
                assertTrue(feesEur == null);
                rawFees = feesCzk;
            } else {
                throw new IllegalStateException("Unexpected value: " + ccy);
            }
        }
        return ofNullable(rawFees).map(BigDecimal::negate).orElse(ZERO);
    }

    protected Country detectCountry(RawBrokerTransaction rawTransaction) {
        Currency ccy = rawTransaction.getCcy();
        if (ccy == null) {
            return null;
        }
        switch (ccy) {
            case USD:
                return Country.US;
            case EUR:
                return Country.DE;
            case CZK:
                return Country.CZ;
            default:
                throw new IllegalArgumentException("Unexpected value: " + ccy);
        }
    }

    @SuppressWarnings({"SpellCheckingInspection", "DuplicatedCode", "RedundantIfStatement"})
    protected TransactionType detectTranType(RawBrokerTransaction tran) {
        Lang lang = tran.getLang();
        String symbol = tran.getSymbol();
        RawBrokerTranDirection direction = tran.getDirection();
        String text = tran.getText();
        String market = tran.getMarket();
        if (direction == null && text.startsWith("Vloženo na účet z") && text.endsWith("Bezhotovostní vklad")) {
            return TransactionType.DEPOSIT;
        }
        if (direction == null && text.equals("Vklad Bezhotovostní vklad")) {
            return TransactionType.DEPOSIT;
        }
        if (direction == null && text.equals("v Bezhotovostní vklad")) {
            return TransactionType.DEPOSIT;
        }
        if (direction == null && text.startsWith("Převod z účtu")) {
            return TransactionType.DEPOSIT;
        }
        if (RawBrokerTranDirection.BANK_TRANSFER.equals(direction) && text.startsWith("Převod na účet")) {
            return TransactionType.WITHDRAWAL;
        }

        if (RawBrokerTranDirection.CURRENCY_CONVERSION.equals(direction) && text.equals("Nákup")) {
            return TransactionType.FX_BUY;
        }
        if (RawBrokerTranDirection.CURRENCY_CONVERSION.equals(direction) && text.equals("Prodej")) {
            return TransactionType.FX_SELL;
        }

        if (RawBrokerTranDirection.BUY.equals(direction) && text.equals("Nákup")) {
            return TransactionType.BUY;
        }
        if (RawBrokerTranDirection.SELL.equals(direction) && text.equals("Prodej")) {
            return TransactionType.SELL;
        }
        if (direction == null && text.startsWith(format("%s - Dividenda", symbol))) {
            return TransactionType.CASH_DIVIDEND;
        }
        if (direction == null && text.startsWith(format("%s - Daň z divid. zaplacená", symbol))) {
            return TransactionType.TAX;
        }
        if (direction == null && text.startsWith(format("%s - Daň z dividend zaplacená", symbol))) {
            return TransactionType.TAX;
        }
        if (direction == null && text.startsWith(format("%s - Divi.", symbol))) {
            return TransactionType.CASH_DIVIDEND;
        }
        if (direction == null && text.startsWith(format("%s - Return of Principal", symbol))) {
            return TransactionType.CAPITAL_DIVIDEND;
        }
        if (direction == null && text.startsWith(format("%s - Stock Dividend", symbol))) {
            return TransactionType.STOCK_DIVIDEND;
        }
        if (direction == null && text.startsWith(format("%s - Finanční kompenzace - Stock Dividend", symbol))) {
            return TransactionType.STOCK_DIVIDEND;
        }
        if (direction == null && text.startsWith(format("%s - Oprava dividendy z", symbol))) {
            return TransactionType.DIVIDEND_REVERSAL;
        }
        if (direction == null && text.startsWith(format("%s - Tax Refund", symbol))) {
            return TransactionType.TAX_REFUND;
        }
        if (direction == null && text.startsWith(format("%s - Oprava daně z dividendy", symbol))) {
            return TransactionType.TAX_REFUND;
        }
        if (direction == null && text.startsWith(format("%s - Refundable U.S. Fed Tax Reclassified By Issuer", symbol))) {
            return TransactionType.TAX_REFUND;
        }

        if (direction == null && text.startsWith(format("%s - ADR Fee", symbol))) {
            return TransactionType.FEE;
        }
        if (direction == null && text.startsWith(format("%s - Spin-off Fair Market Value", symbol))) {
            return TransactionType.SPINOFF_VALUE;
        }
        if (direction == null && text.startsWith(format("%s - Spin-off - daň zaplacená", symbol))) {
            return TransactionType.TAX;
        }
        if (direction == null &&
            text.startsWith(format("%s - Spin-off", symbol)) &&
            !text.contains("Fair Market Value") &&
            !text.contains("daň zaplacená")
        ) {
            return TransactionType.SPINOFF_PARENT;
        }
        if (direction == null && text.startsWith(format("%s - Security Liquidated", symbol))) {
            return TransactionType.LIQUIDATION;
        }
        if (direction == RawBrokerTranDirection.SELL && text.startsWith("Security Liquidated")) {
            return TransactionType.LIQUIDATION;
        }
        if (direction == null && text.startsWith("Poplatek za on-line data")) {
            return TransactionType.FEE;
        }
        if (direction == null && text.startsWith("Reklamace ")) {
            return TransactionType.RECLAMATION;
        }

        if (lang.equals(Lang.CZ) && market.equals("Poplatek")) {
            return TransactionType.FEE;
        }
        if (lang.equals(Lang.EN) && market.equals("Fee")) {
            return TransactionType.FEE;
        }
        if (lang.equals(Lang.SK) && market.equals("Poplatok")) {
            return TransactionType.FEE;
        }

        boolean transformation = false;
        if (lang.equals(Lang.CZ) && market.equals("Transformace")) {
            transformation = true;
        }
        if (lang.equals(Lang.EN) && market.equals("Transformation")) {
            transformation = true;
        }
        if (lang.equals(Lang.SK) && market.equals("Transformácia")) {
            transformation = true;
        }
        if (transformation) {
            if (text.contains("Ticker Change: ")
                || text.contains("Change of Listing: ")
                || text.contains("Change in Security ID (ISIN Change)")
            ) {
                if (RawBrokerTranDirection.SELL.equals(direction)) {
                    return TransactionType.INSTRUMENT_CHANGE_PARENT;
                } else if (RawBrokerTranDirection.BUY.equals(direction)) {
                    return TransactionType.INSTRUMENT_CHANGE_CHILD;
                }
            }
            if (text.contains("Split ")) {
                return TransactionType.SPLIT;
            }
            if (text.contains("Stock Merger ")) {
                if (RawBrokerTranDirection.SELL.equals(direction)) {
                    return TransactionType.MERGER_PARENT;
                } else if (RawBrokerTranDirection.BUY.equals(direction)) {
                    return TransactionType.MERGER_CHILD;
                }
            }
            if (text.contains("Security Deleted As Worthless")) {
                return TransactionType.LIQUIDATION;
            }

            if (text.equals(String.format("%s - Reorganization", symbol))) {
                String rawSymbol = tran.getRawSymbol();
                if (RawBrokerTranDirection.SELL.equals(direction) && (symbol + "*").equals(rawSymbol)) {
                    return TransactionType.INSTRUMENT_CHANGE_PARENT;
                } else if (RawBrokerTranDirection.BUY.equals(direction) && symbol.equals(rawSymbol)) {
                    return TransactionType.INSTRUMENT_CHANGE_CHILD;
                }
            }
        }

        throw new FiobankServiceException(format("Could not detect transaction type: %s", tran));
    }

    protected String generateTranId(Transaction tran, Transaction prevTran) {
        ZonedDateTime date = tran.getDate();
        BigDecimal qty = tran.getQty();
        BigDecimal price = tran.getPrice();
        BigDecimal grossValue = tran.getGrossValue();
        TransactionType type = tran.getType();
        Currency ccy = tran.getCcy();
        Country country = tran.getCountry();
        String symbol = tran.getSymbol();
        String id = format("%s/%s/%s/%s/%s/%s/%s/%s/%s",
                LazyHolder.ID_DATE_FORMAT.format(date),
                type,
                ccy,
                country,
                symbol,
                grossValue == null ? "" : grossValue.unscaledValue(),
                qty == null ? "" : qty.unscaledValue(),
                price == null ? "" : price.unscaledValue(),
                Objects.hashCode(tran.getNote())
        );
        if (prevTran != null) {
            String prevId = prevTran.getId();
            ZonedDateTime prevDate = prevTran.getDate();
            TransactionType prevType = prevTran.getType();
            Currency prevCcy = prevTran.getCcy();
            Country prevCountry = prevTran.getCountry();
            String prevSymbol = prevTran.getSymbol();

            if (id.equals(prevId) ||
                (date.isEqual(prevDate)
                 && Objects.equals(type, prevType)
                 && Objects.equals(ccy, prevCcy)
                 && Objects.equals(country, prevCountry)
                 && Objects.equals(symbol, prevSymbol)
                )
            ) {
                id = id + "/" + Objects.hash(prevId);
            }
        }
        return id;
    }
}