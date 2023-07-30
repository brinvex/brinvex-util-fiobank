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
import com.brinvex.util.fiobank.api.model.Portfolio;
import com.brinvex.util.fiobank.api.model.PortfolioValue;
import com.brinvex.util.fiobank.api.model.RawBrokerTransaction;
import com.brinvex.util.fiobank.api.model.RawBrokerTransactionList;
import com.brinvex.util.fiobank.api.model.Transaction;
import com.brinvex.util.fiobank.api.service.FioBrokerService;
import com.brinvex.util.fiobank.api.service.exception.FiobankServiceException;
import com.brinvex.util.fiobank.impl.broker.parser.BrokerStatementParser;
import com.brinvex.util.fiobank.impl.util.IOUtil;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.brinvex.util.fiobank.impl.util.ValidationUtil.assertTrue;
import static java.lang.String.format;

public class FioBrokerServiceImpl implements FioBrokerService {

    private static class LazyHolder {
        private static final Charset DEFAULT_CHARSET = Charset.forName("windows-1250");
    }

    protected final BrokerStatementParser brokerStatementParser = new BrokerStatementParser();

    protected final PortfolioManager ptfManager = new PortfolioManager();

    protected final FioBrokerTransactionMapper transactionMapper = new FioBrokerTransactionMapper();

    @Override
    public RawBrokerTransactionList parseTransactionStatements(Collection<Path> transactionStatementFilePaths) {
        return parseTransactionStatements(transactionStatementFilePaths
                .stream()
                .map(filePath -> IOUtil.readTextFileContent(filePath, LazyHolder.DEFAULT_CHARSET, StandardCharsets.UTF_8))
        );
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public RawBrokerTransactionList parseTransactionStatements(Stream<String> transactionStatementContents) {
        List<RawBrokerTransactionList> rawTranLists = transactionStatementContents
                .map(brokerStatementParser::parseTrasnsactionStatement)
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
                Object tranKey = rawTransactionKey(tranListTran);
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
    public Portfolio processTransactionStatements(Portfolio ptf, Stream<String> transactionStatementContents) {
        RawBrokerTransactionList rawTranList = parseTransactionStatements(transactionStatementContents);
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
        Transaction prevTran;
        if (ptfTrans.isEmpty()) {
            prevTran = null;
        } else {
            prevTran = ptfTrans.get(ptfTrans.size() - 1);
            LocalDateTime lastPtfTranDate = prevTran.getDate().toLocalDateTime();
            rawTrans.removeIf(t -> !t.getTradeDate().isAfter(lastPtfTranDate));
        }

        Portfolio finalPtf = ptf;
        Function<String, Country> symbolCountryProvider = symbol -> ptfManager.findPosition(finalPtf, symbol).getCountry();
        while (!rawTrans.isEmpty()) {
            int sizeBeforeMapper = rawTrans.size();
            List<Transaction> newTrans = transactionMapper.mapTransactions(prevTran, rawTrans, symbolCountryProvider);
            for (Transaction newTran : newTrans) {
                ptfTrans.add(newTran);
                ptfManager.applyTransaction(ptf, newTran);
                prevTran = newTran;
            }
            int sizeAfterMapper = rawTrans.size();
            assertTrue(sizeBeforeMapper > sizeAfterMapper);
        }
        return ptf;
    }

    @Override
    public Portfolio processTransactionStatements(Portfolio ptf, Collection<Path> transactionStatementFilePaths) {
        Stream<String> statementContentStream = transactionStatementFilePaths
                .stream()
                .map(filePath -> IOUtil.readTextFileContent(filePath, LazyHolder.DEFAULT_CHARSET, StandardCharsets.UTF_8));
        return processTransactionStatements(ptf, statementContentStream);
    }

    @Override
    public Map<LocalDate, PortfolioValue> getPortfolioValues(Collection<Path> portfolioStatementPaths) {
        Stream<String> statementContentStream = portfolioStatementPaths
                .stream()
                .map(filePath -> IOUtil.readTextFileContent(filePath, LazyHolder.DEFAULT_CHARSET, StandardCharsets.UTF_8));
        return getPortfolioValues(statementContentStream);
    }

    @Override
    public Map<LocalDate, PortfolioValue> getPortfolioValues(Stream<String> portfolioStatementContents) {
        TreeMap<LocalDate, PortfolioValue> results = new TreeMap<>();
        portfolioStatementContents.forEach(c -> {
            PortfolioValue ptfValue = brokerStatementParser.parsePortfolioStatement(c);
            LocalDate day = ptfValue.getDay();
            PortfolioValue oldPtfValue = results.get(day);
            if (oldPtfValue == null) {
                results.put(day, ptfValue);
            } else {
                if (!ptfValue.getCurrency().equals(oldPtfValue.getCurrency())
                    || ptfValue.getTotalValue().compareTo(oldPtfValue.getTotalValue()) != 0) {
                    throw new FiobankServiceException(String.format("Different data: %s oldPtfValue=%s ptfValue=%s",
                            day, oldPtfValue, ptfValue));
                }
            }
        });
        return results;
    }

    @Override
    public Portfolio processTransactionStatements(Collection<Path> transactionStatementFilePaths) {
        Stream<String> statementContentStream = transactionStatementFilePaths
                .stream()
                .map(filePath -> IOUtil.readTextFileContent(filePath, LazyHolder.DEFAULT_CHARSET, StandardCharsets.UTF_8));
        return processTransactionStatements(statementContentStream);
    }

    @Override
    public Portfolio processTransactionStatements(Stream<String> transactionStatementContents) {
        return processTransactionStatements(null, transactionStatementContents);
    }

    protected Object rawTransactionKey(RawBrokerTransaction tran) {
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

}
