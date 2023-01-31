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
import com.brinvex.util.fiobank.api.model.Portfolio;
import com.brinvex.util.fiobank.api.model.Position;
import com.brinvex.util.fiobank.api.model.RawBrokerTranDirection;
import com.brinvex.util.fiobank.api.model.RawBrokerTransaction;
import com.brinvex.util.fiobank.api.model.RawBrokerTransactionList;
import com.brinvex.util.fiobank.api.model.Transaction;
import com.brinvex.util.fiobank.api.model.TransactionType;
import com.brinvex.util.fiobank.api.service.FioBrokerService;
import com.brinvex.util.fiobank.api.service.exception.FiobankServiceException;
import com.brinvex.util.fiobank.impl.broker.parser.ParsingUtil;
import com.brinvex.util.fiobank.impl.broker.parser.BrokerStatementParser;
import com.brinvex.util.fiobank.impl.util.IOUtil;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.util.Objects.requireNonNullElse;
import static java.util.Optional.ofNullable;

@SuppressWarnings("SpellCheckingInspection")
public class FioBrokerServiceImpl implements FioBrokerService {

    private static class LazyHolder {
        private static final Charset DEFAULT_CHARSET = Charset.forName("windows-1250");

        private static final ZoneId FIO_TIME_ZONE = ZoneId.of("Europe/Prague");

        private static final Pattern DIVIDEND_TAX_RATE_PATTERN = Pattern.compile("\\(((čistá)|(po\\s+zdanění)),\\s+daň\\s(?<taxRate>\\d+(,\\d+)?)\\s*%\\)");
    }

    private final BrokerStatementParser brokerStatementParser = new BrokerStatementParser();

    private final PortfolioManager ptfManager = new PortfolioManager();

    @Override
    public RawBrokerTransactionList parseStatements(Collection<String> statementFilePaths) {
        return parseStatements(statementFilePaths
                .stream()
                .map(Path::of)
                .map(filePath -> IOUtil.readTextFileContent(filePath, LazyHolder.DEFAULT_CHARSET, StandardCharsets.UTF_8))
        );
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public RawBrokerTransactionList parseStatements(Stream<String> statementContents) {
        List<RawBrokerTransactionList> rawTranLists = statementContents
                .map(brokerStatementParser::parseStatement)
                .sorted(Comparator.comparing(RawBrokerTransactionList::getPeriodFrom).thenComparing(RawBrokerTransactionList::getPeriodTo))
                .collect(Collectors.toList());

        if (rawTranLists.isEmpty()) {
            throw new IllegalArgumentException("Expected non-empty stream of statements");
        }

        RawBrokerTransactionList result = new RawBrokerTransactionList();
        String accountNumber0;
        {
            RawBrokerTransactionList rawTranList0 = rawTranLists.get(0);
            accountNumber0 = rawTranList0.getAccountNumber();

            result.setAccountNumber(accountNumber0);
            result.setPeriodFrom(rawTranList0.getPeriodFrom());
            result.setPeriodTo(rawTranList0.getPeriodTo());
        }

        Set<RawBrokerTransaction> rawTrans = new LinkedHashSet<>();
        Set<Object> rawTranKeys = new LinkedHashSet<>();
        for (RawBrokerTransactionList rawTranList : rawTranLists) {
            LocalDate periodFrom = rawTranList.getPeriodFrom();
            LocalDate periodTo = rawTranList.getPeriodTo();

            String accountNumber = rawTranList.getAccountNumber();
            if (!accountNumber0.equals(accountNumber)) {
                throw new FiobankServiceException(format("Unexpected multiple accounts: %s, %s",
                        accountNumber0,
                        accountNumber
                ));
            }

            LocalDate nextPeriodFrom = result.getPeriodTo().plusDays(1);
            if (nextPeriodFrom.isBefore(periodFrom)) {
                throw new FiobankServiceException(format("Missing period: '%s - %s', accountNumber=%s",
                        nextPeriodFrom, periodFrom.minusDays(1), accountNumber0));
            }
            if (periodTo.isAfter(result.getPeriodTo())) {
                result.setPeriodTo(periodTo);
            }

            List<RawBrokerTransaction> tranListTrans = new ArrayList<>(rawTranList.getTransactions());
            Collections.reverse(tranListTrans);
            Set<Object> tranListTranKeys = new LinkedHashSet<>();
            for (RawBrokerTransaction tranListTran : tranListTrans) {
                Object tranKey = constructRawTransactionKey(tranListTran);
                if (!rawTranKeys.contains(tranKey)) {
                    rawTrans.add(tranListTran);
                }
                tranListTranKeys.add(tranKey);
            }
            rawTranKeys.addAll(tranListTranKeys);
        }

        result.setTransactions(rawTrans
                .stream()
                .sorted(Comparator.comparing(RawBrokerTransaction::getTradeDate))
                .collect(Collectors.toCollection(ArrayList::new))
        );

        return result;
    }

    @Override
    @SuppressWarnings("DuplicatedCode")
    public Portfolio processStatements(Portfolio ptf, Stream<String> statementContents) {
        RawBrokerTransactionList rawTranList = parseStatements(statementContents);
        List<RawBrokerTransaction> rawTrans = rawTranList.getTransactions();

        String accountNumber = rawTranList.getAccountNumber();
        LocalDate periodFrom = rawTranList.getPeriodFrom();
        LocalDate periodTo = rawTranList.getPeriodTo();
        if (ptf == null) {
            ptf = ptfManager.initPortfolio(accountNumber, periodFrom, periodTo);
        } else {
            if (!accountNumber.equals(ptf.getAccountNumber())) {
                throw new FiobankServiceException(format("Unexpected multiple accounts: %s, %s",
                        ptf.getAccountNumber(),
                        accountNumber
                ));
            }
            LocalDate nextPeriodFrom = ptf.getPeriodTo().plusDays(1);
            if (nextPeriodFrom.isBefore(periodFrom)) {
                throw new FiobankServiceException(format("Missing period: '%s - %s', accountNumber=%s",
                        nextPeriodFrom, periodFrom.minusDays(1), accountNumber));
            }
            if (periodTo.isAfter(ptf.getPeriodTo())) {
                ptf.setPeriodTo(periodTo);
            }
        }
        List<Transaction> ptfTrans = ptf.getTransactions();
        if (!ptfTrans.isEmpty()) {
            Transaction lastPtfTran = ptfTrans.get(ptfTrans.size() - 1);
            LocalDateTime lastPtfTranDate = lastPtfTran.getDate().toLocalDateTime();
            rawTrans.removeIf(t -> !t.getTradeDate().isAfter(lastPtfTranDate));
        }

        LinkedList<Transaction> newTrans = new LinkedList<>();
        for (int i = 0, rawTransSize = rawTrans.size(); i < rawTransSize; i++) {
            RawBrokerTransaction rawTran = rawTrans.get(i);

            LocalDateTime tranDate = rawTran.getTradeDate();
            ZonedDateTime tranZonedDate = tranDate.atZone(LazyHolder.FIO_TIME_ZONE);

            try {
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
                    RawBrokerTransaction nextRawTran = i < rawTransSize - 1 ? rawTrans.get(i + 1) : null;
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

                Function<TransactionType, Transaction> tranInitializer = tType -> {
                    Transaction t = new Transaction();
                    t.setType(tType);
                    t.setDate(tranZonedDate);
                    t.setText(text);
                    t.setSettlementDate(settlDate);
                    t.setCurrency(ccy);
                    t.setGrossValue(null);
                    t.setNetValue(null);
                    t.setQty(ZERO);
                    t.setIncome(ZERO);
                    t.setFees(ZERO);
                    t.setTax(null);
                    ptfTrans.add(t);
                    newTrans.add(t);
                    return t;
                };
                BiFunction<TransactionType, Transaction, Transaction> bunchTranInitializer = (tType, bunchedTran) -> {
                    String bunchId = UUID.randomUUID().toString();
                    Transaction t = tranInitializer.apply(tType);
                    t.setBunchId(bunchId);
                    bunchedTran.setBunchId(bunchId);
                    return t;
                };

                if (tranType == TransactionType.CASH_TOP_UP) {
                    assertTrue(direction == null || RawBrokerTranDirection.BANK_TRANSFER.equals(direction));
                    assertNull(symbol);
                    assertNull(price);
                    assertIsZero(qty);
                    assertIsPositive(rawValue);
                    assertIsZeroOrNegative(fees);

                    Transaction t = tranInitializer.apply(TransactionType.CASH_TOP_UP);
                    t.setGrossValue(rawValue);
                    t.setNetValue(rawValue.add(fees));
                    t.setCurrency(ccy);
                    t.setQty(ZERO);
                    t.setIncome(rawValue);
                    t.setFees(fees);

                } else if (tranType == TransactionType.CASH_WITHDRAWAL) {
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
                        i++;
                    }
                    Transaction t = tranInitializer.apply(TransactionType.CASH_WITHDRAWAL);
                    t.setGrossValue(rawValue);
                    t.setNetValue(rawValue.add(fees));
                    t.setIncome(rawValue);
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

                    Transaction t = tranInitializer.apply(TransactionType.BUY);
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

                    Transaction t = tranInitializer.apply(TransactionType.SELL);
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

                    Transaction t = tranInitializer.apply(TransactionType.FX_BUY);
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

                    Transaction t = tranInitializer.apply(TransactionType.FX_SELL);
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
                    assertEqual(ONE, price);
                    assertIsPositive(qty);
                    assertIsPositive(rawValue);
                    assertIsZero(fees);

                    BigDecimal tax = null;
                    BigDecimal grossValue = rawValue;
                    if (TransactionType.TAX.equals(nextTranType) && symbol.equals(nextSymbol)) {
                        assertEqual(ccy, nextCcy);
                        assertEqual(nextPrice, ONE);
                        assertIsZero(nextFees);
                        assertIsNegative(nextRawValue);
                        assertEqual(nextRawValue, nextQty);
                        tax = nextRawValue;
                        i++;
                    } else {
                        Matcher m = LazyHolder.DIVIDEND_TAX_RATE_PATTERN.matcher(text);
                        if (m.find()) {
                            BigDecimal taxRate = ParsingUtil.toDecimal(m.group("taxRate"))
                                    .divide(new BigDecimal("100.00"), 6, RoundingMode.HALF_UP);
                            assertIsPositive(taxRate);
                            assertEqual(rawValue, qty);
                            tax = rawValue
                                    .divide(ONE.subtract(taxRate), 6, RoundingMode.HALF_UP)
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
                        i++;
                    }

                    Transaction t = tranInitializer.apply(TransactionType.CASH_DIVIDEND);
                    t.setGrossValue(grossValue);
                    t.setNetValue(grossValue.add(requireNonNullElse(tax, ZERO)).add(fees));
                    t.setCountry(country);
                    t.setSymbol(symbol);
                    t.setIncome(grossValue);
                    t.setFees(fees);
                    t.setTax(tax);

                } else if (tranType == TransactionType.CAPITAL_DIVIDEND) {
                    assertTrue(symbol != null);
                    assertTrue(ccy != null);
                    assertTrue(price.compareTo(ONE) == 0);
                    assertIsPositive(qty);
                    assertIsPositive(rawValue);
                    assertIsZero(fees);

                    Transaction t = tranInitializer.apply(TransactionType.CAPITAL_DIVIDEND);
                    t.setCountry(country);
                    t.setSymbol(symbol);
                    t.setCurrency(ccy);
                    t.setIncome(rawValue);
                    t.setGrossValue(rawValue);
                    t.setNetValue(rawValue);

                } else if (tranType == TransactionType.STOCK_DIVIDEND) {
                    assertNull(direction);
                    assertNotNull(symbol);
                    assertEqual(ONE, price);
                    assertIsPositive(qty);
                    assertIsZeroOrNegative(fees);

                    if (String.format("%s - Stock Dividend", symbol).equals(text)) {
                        assertIsZero(fees);
                        assertNull(country);
                        country = ptfManager.findPosition(ptf, symbol).getCountry();
                        Transaction t = tranInitializer.apply(TransactionType.STOCK_DIVIDEND);
                        t.setCountry(country);
                        t.setSymbol(symbol);
                        t.setQty(qty);
                        t.setFees(fees);
                    } else if (String.format("%s - Finanční kompenzace - Stock Dividend", symbol).equals(text)) {
                        assertIsZero(fees);
                        assertEqual(rawValue, qty);
                        assertNotNull(country);
                        Transaction t = tranInitializer.apply(TransactionType.STOCK_DIVIDEND);
                        t.setGrossValue(rawValue);
                        t.setNetValue(rawValue);
                        t.setIncome(rawValue);
                        t.setCountry(country);
                        t.setSymbol(symbol);
                        t.setQty(ZERO);
                    } else {
                        throw new FiobankServiceException("Unsupported: " + text);
                    }

                } else if (tranType == TransactionType.DIVIDEND_REVERSAL) {
                    assertNull(direction);
                    assertNotNull(country);
                    assertNotNull(symbol);
                    assertNotNull(ccy);
                    assertEqual(ONE, price);
                    assertIsNegative(qty);
                    assertIsNegative(rawValue);
                    assertIsZeroOrNegative(fees);

                    BigDecimal taxRefund;
                    {
                        assertTrue(TransactionType.TAX_REFUND.equals(nextTranType));
                        assertTrue(symbol.equals(nextSymbol));
                        assertTrue(ccy.equals(nextCcy));
                        assertTrue(nextPrice.compareTo(ONE) == 0);
                        assertIsZero(nextFees);
                        assertIsPositive(nextRawValue);
                        assertEqual(nextRawValue, nextQty);
                        taxRefund = nextRawValue;
                        i++;
                    }
                    Transaction t = tranInitializer.apply(TransactionType.DIVIDEND_REVERSAL);
                    t.setGrossValue(rawValue);
                    t.setNetValue(rawValue.add(taxRefund).add(fees));
                    t.setCountry(country);
                    t.setSymbol(symbol);
                    t.setIncome(rawValue);
                    t.setFees(fees);
                    t.setTax(taxRefund);

                } else if (tranType == TransactionType.FEE) {
                    assertNull(direction);
                    assertTrue(symbol != null || qty.compareTo(ZERO) == 0);
                    assertNotNull(ccy);
                    assertIsZeroOrNegative(rawValue);
                    assertIsZeroOrNegative(fees);
                    assertTrue(price == null || price.compareTo(ONE) == 0);

                    BigDecimal value;
                    if (fees.compareTo(ZERO) == 0) {
                        value = rawValue;
                    } else if (rawValue.compareTo(ZERO) == 0) {
                        value = fees;
                    } else if (rawValue.equals(fees)) {
                        value = rawValue;
                    } else {
                        throw new FiobankServiceException(String.format("fees=%s, rawValue=%s", fees, rawValue));
                    }
                    if (value.compareTo(ZERO) == 0) {
                        continue;
                    }

                    Transaction t = tranInitializer.apply(TransactionType.FEE);
                    t.setGrossValue(ZERO);
                    t.setNetValue(value);
                    t.setFees(value);
                    t.setSymbol(symbol);
                    t.setCountry(country);
                } else if (tranType == TransactionType.RECLAMATION) {
                    assertNull(direction);
                    assertNull(symbol);
                    assertTrue(qty == null || qty.compareTo(ZERO) == 0);
                    assertNotNull(ccy);
                    assertIsZeroOrPositive(rawValue);
                    assertTrue(fees.compareTo(ZERO) == 0 || fees.compareTo(rawValue) == 0);

                    Transaction t = tranInitializer.apply(TransactionType.RECLAMATION);
                    t.setPrice(price);
                    t.setGrossValue(rawValue);
                    t.setNetValue(rawValue);
                    t.setIncome(rawValue);

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

                    Position position = ptfManager.findPosition(ptf, nextSymbol);
                    Country positionCountry = position.getCountry();
                    Currency countryCcy = getCcyByCountry(positionCountry);
                    Transaction t1;
                    {
                        assertTrue(RawBrokerTranDirection.SELL.equals(nextDirection));
                        t1 = tranInitializer.apply(TransactionType.INSTRUMENT_CHANGE_PARENT);
                        t1.setNetValue(ZERO);
                        t1.setGrossValue(ZERO);
                        t1.setCountry(positionCountry);
                        t1.setCurrency(countryCcy);
                        t1.setSymbol(nextSymbol);
                        t1.setQty(qty.negate());
                    }
                    {
                        assertTrue(RawBrokerTranDirection.BUY.equals(direction));
                        Transaction t2 = bunchTranInitializer.apply(TransactionType.INSTRUMENT_CHANGE_CHILD, t1);
                        t2.setNetValue(ZERO);
                        t2.setGrossValue(ZERO);
                        t2.setCountry(positionCountry);
                        t2.setCurrency(countryCcy);
                        t2.setSymbol(symbol);
                        t2.setQty(qty);
                    }
                    i++;
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

                    Position position = ptfManager.findPosition(ptf, symbol);
                    country = position.getCountry();
                    Currency countryCcy = getCcyByCountry(country);

                    Transaction t1;
                    {
                        assertTrue(RawBrokerTranDirection.SELL.equals(direction));
                        t1 = tranInitializer.apply(TransactionType.INSTRUMENT_CHANGE_PARENT);
                        t1.setNetValue(ZERO);
                        t1.setGrossValue(ZERO);
                        t1.setCountry(country);
                        t1.setCurrency(countryCcy);
                        t1.setSymbol(symbol);
                        t1.setQty(qty.negate());
                    }
                    {
                        assertTrue(RawBrokerTranDirection.BUY.equals(nextDirection));
                        Transaction t2 = bunchTranInitializer.apply(TransactionType.INSTRUMENT_CHANGE_CHILD, t1);
                        t2.setNetValue(ZERO);
                        t2.setGrossValue(ZERO);
                        t2.setCountry(country);
                        t2.setCurrency(countryCcy);
                        t2.setSymbol(nextSymbol);
                        t2.setQty(qty);
                    }
                    i++;
                } else if (tranType == TransactionType.TAX_REFUND) {

                    assertTrue(symbol != null);
                    assertTrue(ccy != null);
                    assertEqual(price, ONE);
                    assertIsPositive(qty);
                    assertEqual(rawValue, qty);
                    assertIsZero(fees);
                    {
                        Transaction t = tranInitializer.apply(TransactionType.TAX_REFUND);
                        t.setCountry(country);
                        t.setSymbol(symbol);
                        t.setGrossValue(ZERO);
                        t.setNetValue(rawValue);
                        t.setTax(rawValue);
                    }
                } else if (tranType == TransactionType.TAX) {
                    assertNull(direction);
                    assertNotNull(symbol);
                    assertNotNull(ccy);
                    assertEqual(price, ONE);
                    assertIsZeroOrNegative(qty);
                    assertTrue(rawValue.compareTo(qty) <= 0);
                    assertIsZero(fees);

                    {
                        Transaction t = tranInitializer.apply(TransactionType.TAX);
                        t.setCountry(country);
                        t.setSymbol(symbol);
                        t.setGrossValue(ZERO);
                        t.setNetValue(rawValue);
                        t.setTax(rawValue);
                    }
                } else if (tranType == TransactionType.LIQUIDATION) {
                    assertNotNull(symbol);
                    assertTrue(price.compareTo(ONE) == 0 || price.compareTo(ZERO) == 0);
                    assertIsPositive(qty);
                    assertTrue(rawValue.compareTo(ZERO) == 0 || rawValue.compareTo(qty) == 0);
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
                        i++;

                        Transaction t = tranInitializer.apply(TransactionType.LIQUIDATION);
                        t.setCountry(country);
                        t.setSymbol(symbol);
                        t.setCurrency(ccy);
                        t.setGrossValue(qty);
                        t.setNetValue(qty);
                        t.setIncome(qty);
                        t.setQty(nextQty.negate());
                        t.setPrice(ZERO);
                    } else {
                        assertNull(country);
                        assertNull(ccy);
                        country = ptfManager.findPosition(ptf, symbol).getCountry();
                        Currency countryCcy = getCcyByCountry(country);

                        Transaction t = tranInitializer.apply(TransactionType.LIQUIDATION);
                        t.setCountry(country);
                        t.setSymbol(symbol);
                        t.setCurrency(countryCcy);
                        t.setGrossValue(ZERO);
                        t.setNetValue(ZERO);
                        t.setIncome(ZERO);
                        t.setQty(qty.negate());
                        t.setPrice(ZERO);

                    }
                } else if (tranType == TransactionType.SPINOFF_PARENT) {
                    assertNull(direction);
                    assertNull(country);
                    assertNotNull(symbol);
                    assertNull(ccy);
                    assertNotNull(rawCcy);
                    assertEqual(ONE, price);
                    assertIsPositive(qty);
                    assertIsZero(rawValue);
                    assertIsZero(fees);

                    Position position = ptfManager.findPosition(ptf, symbol);
                    country = position.getCountry();
                    Currency positionCcy = getCcyByCountry(country);

                    Transaction t1;
                    {
                        t1 = tranInitializer.apply(TransactionType.SPINOFF_PARENT);
                        t1.setCurrency(positionCcy);
                        t1.setCountry(country);
                        t1.setSymbol(symbol);
                    }
                    {
                        Transaction t2 = bunchTranInitializer.apply(TransactionType.SPINOFF_CHILD, t1);
                        t2.setCurrency(positionCcy);
                        t2.setCountry(country);
                        t2.setSymbol(rawCcy);
                        t2.setQty(qty);
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
                    country = ptfManager.findPosition(ptf, nextSymbol).getCountry();
                    Currency countryCcy = getCcyByCountry(country);
                    Transaction t1;
                    {
                        t1 = tranInitializer.apply(TransactionType.MERGER_PARENT);
                        t1.setCurrency(countryCcy);
                        t1.setCountry(country);
                        t1.setSymbol(nextSymbol);
                        t1.setQty(nextQty.negate());
                    }
                    {
                        Transaction t2 = bunchTranInitializer.apply(TransactionType.MERGER_CHILD, t1);
                        t2.setCurrency(countryCcy);
                        t2.setCountry(country);
                        t2.setSymbol(symbol);
                        t2.setQty(qty);
                    }
                    i++;
                } else if (tranType == TransactionType.SPINOFF_VALUE) {
                    assertNull(direction);
                    assertNotNull(symbol);
                    assertNotNull(ccy);
                    assertNotNull(rawCcy);
                    assertEqual(ONE, price);
                    assertEqual(rawValue, qty);
                    assertIsPositive(rawValue);
                    assertIsZero(fees);

                    assertTrue(TransactionType.SPINOFF_VALUE.equals(nextTranType));
                    assertTrue(nextDirection == null);
                    assertTrue(nextSymbol.equals(symbol));
                    assertTrue(nextRawValue.negate().equals(rawValue));
                    assertTrue(nextQty.negate().equals(qty));
                    assertIsZero(nextFees);
                    assertTrue(price.compareTo(ONE) == 0);
                    i++;
                    Transaction t1;
                    {
                        t1 = tranInitializer.apply(TransactionType.SPINOFF_VALUE);
                        t1.setCountry(country);
                        t1.setSymbol(symbol);
                        t1.setIncome(rawValue);
                        t1.setGrossValue(rawValue);
                        t1.setNetValue(rawValue);
                    }
                    {
                        Transaction t2 = bunchTranInitializer.apply(TransactionType.SPINOFF_VALUE, t1);
                        t2.setCountry(country);
                        t2.setSymbol(symbol);
                        t2.setIncome(rawValue.negate());
                        t2.setGrossValue(rawValue.negate());
                        t2.setNetValue(rawValue.negate());
                    }
                } else if (tranType == TransactionType.SPLIT) {
                    assertNull(country);
                    assertNull(ccy);
                    Position position = ptfManager.findPosition(ptf, symbol);
                    country = position.getCountry();
                    Currency countryCcy = getCcyByCountry(country);

                    if (TransactionType.SPLIT.equals(nextTranType) && nextSymbol.equals(symbol)) {
                        assertTrue(direction == RawBrokerTranDirection.SELL);
                        assertTrue(nextDirection == RawBrokerTranDirection.BUY);
                        assertNull(nextCcy);
                        assertIsZero(nextPrice);
                        assertIsPositive(nextQty);
                        assertIsZero(nextRawValue);
                        assertIsZero(nextFees);
                        qty = qty.negate().add(nextQty);
                        i++;
                    }
                    {
                        Transaction t = tranInitializer.apply(TransactionType.SPLIT);
                        t.setCurrency(countryCcy);
                        t.setGrossValue(ZERO);
                        t.setNetValue(ZERO);
                        t.setCountry(country);
                        t.setSymbol(symbol);
                        t.setSymbol(symbol);
                        t.setQty(qty);
                    }
                } else {
                    throw new FiobankServiceException(format("%s - rawTran=%s", i + 1, rawTran));
                }

                while (!newTrans.isEmpty()) {
                    Transaction newTran = newTrans.removeFirst();
                    try {
                        ptfManager.applyTransaction(ptf, newTran);
                    } catch (Exception e) {
                        throw new FiobankServiceException(format("%s - newTran=%s, ptf=%s", i + 1, newTran, ptf), e);
                    }
                }

            } catch (Exception e) {
                throw new FiobankServiceException(format("%s - rawTran=%s", i + 1, rawTran), e);
            }
        }

        return ptf;
    }

    @Override
    public Portfolio processStatements(Portfolio ptf, Collection<String> statementFilePaths) {
        Stream<String> statementContentStream = statementFilePaths
                .stream()
                .map(Path::of)
                .map(filePath -> IOUtil.readTextFileContent(filePath, LazyHolder.DEFAULT_CHARSET, StandardCharsets.UTF_8));
        return processStatements(ptf, statementContentStream);
    }

    @Override
    public Portfolio processStatements(Collection<String> statementFilePaths) {
        Stream<String> statementContentStream = statementFilePaths
                .stream()
                .map(Path::of)
                .map(filePath -> IOUtil.readTextFileContent(filePath, LazyHolder.DEFAULT_CHARSET, StandardCharsets.UTF_8));
        return processStatements(statementContentStream);
    }

    @Override
    public Portfolio processStatements(Stream<String> statementContents) {
        return processStatements(null, statementContents);
    }

    @SuppressWarnings({"SpellCheckingInspection", "DuplicatedCode", "RedundantIfStatement"})
    private TransactionType detectTranType(RawBrokerTransaction tran) {
        Lang lang = tran.getLang();
        String symbol = tran.getSymbol();
        RawBrokerTranDirection direction = tran.getDirection();
        String text = tran.getText();
        String market = tran.getMarket();
        if (direction == null && text.startsWith("Vloženo na účet z") && text.endsWith("Bezhotovostní vklad")) {
            return TransactionType.CASH_TOP_UP;
        }
        if (direction == null && text.equals("Vklad Bezhotovostní vklad")) {
            return TransactionType.CASH_TOP_UP;
        }
        if (direction == null && text.startsWith("Převod z účtu")) {
            return TransactionType.CASH_TOP_UP;
        }
        if (RawBrokerTranDirection.BANK_TRANSFER.equals(direction) && text.startsWith("Převod na účet")) {
            return TransactionType.CASH_WITHDRAWAL;
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
        }

        throw new FiobankServiceException(format("Could not detect transaction type: %s", tran));
    }

    private Country detectCountry(RawBrokerTransaction rawTransaction) {
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

    private Currency getCcyByCountry(Country country) {
        if (country == null) {
            return null;
        }
        switch (country) {
            case US:
                return Currency.USD;
            case DE:
                return Currency.EUR;
            case CZ:
                return Currency.CZK;
            default:
                throw new IllegalArgumentException("Unexpected value: " + country);
        }
    }

    private Object constructRawTransactionKey(RawBrokerTransaction tran) {
        return Arrays.asList(
                tran.getTradeDate(),
                tran.getDirection(),
                tran.getSymbol(),
                tran.getPrice(),
                tran.getShares(),
                tran.getCcy(),
                tran.getVolumeCzk(),
                tran.getFeesCzk(),
                tran.getVolumeUsd(),
                tran.getFeesUsd(),
                tran.getVolumeEur(),
                tran.getFeesEur()
        );
    }

    @SuppressWarnings("DuplicatedCode")
    private BigDecimal getFees(RawBrokerTransaction rawTran) {
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

    @SuppressWarnings("DuplicatedCode")
    private BigDecimal getValue(RawBrokerTransaction rawTran) {
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

}
