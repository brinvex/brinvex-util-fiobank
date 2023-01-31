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
package com.brinvex.util.fiobank.impl.bank;

import com.brinvex.util.fiobank.api.model.Currency;
import com.brinvex.util.fiobank.api.model.Portfolio;
import com.brinvex.util.fiobank.api.model.RawBankTransaction;
import com.brinvex.util.fiobank.api.model.RawBankTransactionList;
import com.brinvex.util.fiobank.api.model.Transaction;
import com.brinvex.util.fiobank.api.model.TransactionType;
import com.brinvex.util.fiobank.api.service.FioBankService;
import com.brinvex.util.fiobank.api.service.exception.FiobankServiceException;
import com.brinvex.util.fiobank.impl.bank.parser.BankStatementParser;
import com.brinvex.util.fiobank.impl.broker.PortfolioManager;
import com.brinvex.util.fiobank.impl.util.IOUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.brinvex.util.fiobank.impl.util.ValidationUtil.assertEqual;
import static java.lang.String.format;
import static java.math.BigDecimal.ZERO;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toCollection;

public class FioBankServiceImpl implements FioBankService {

    private static class LazyHolder {

        private static final ZoneId FIO_TIME_ZONE = ZoneId.of("Europe/Prague");

        private static final String URL_FORMAT = "https://www.fio.cz/ib_api/rest/periods/%s/%s/%s/transactions.xml";
    }

    private final BankStatementParser bankStatementParser = new BankStatementParser();

    private final PortfolioManager ptfManager = new PortfolioManager();

    @SuppressWarnings("DuplicatedCode")
    @Override
    public RawBankTransactionList parseStatements(Stream<String> statementContents) {
        List<RawBankTransactionList> rawTranLists = statementContents
                .map(bankStatementParser::parseStatement)
                .sorted(comparing(RawBankTransactionList::getPeriodFrom).thenComparing(RawBankTransactionList::getPeriodTo))
                .collect(Collectors.toList());

        if (rawTranLists.isEmpty()) {
            throw new IllegalArgumentException("Expected non-empty stream of statements");
        }

        RawBankTransactionList result = new RawBankTransactionList();
        String accountNumber0;
        {
            RawBankTransactionList rawTranList0 = rawTranLists.get(0);
            accountNumber0 = rawTranList0.getAccountNumber();

            result.setAccountNumber(accountNumber0);
            result.setPeriodFrom(rawTranList0.getPeriodFrom());
            result.setPeriodTo(rawTranList0.getPeriodTo());
        }

        Set<RawBankTransaction> rawTrans = new LinkedHashSet<>();
        Set<Object> rawTranKeys = new LinkedHashSet<>();
        for (RawBankTransactionList rawTranList : rawTranLists) {
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

            for (RawBankTransaction tranListTran : rawTranList.getTransactions()) {
                Object tranKey = tranListTran.getId();
                if (rawTranKeys.add(tranKey)) {
                    rawTrans.add(tranListTran);
                }
            }
        }

        result.setTransactions(rawTrans
                .stream()
                .sorted(comparing(RawBankTransaction::getDate).thenComparing(RawBankTransaction::getId))
                .collect(toCollection(ArrayList::new))
        );

        return result;
    }

    @Override
    public RawBankTransactionList parseStatements(Collection<String> statementFilePaths) {
        return parseStatements(statementFilePaths
                .stream()
                .map(Path::of)
                .map(filePath -> IOUtil.readTextFileContent(filePath, StandardCharsets.UTF_8))
        );
    }

    @Override
    @SuppressWarnings({"DuplicatedCode", "SpellCheckingInspection", "UnnecessaryLocalVariable"})
    public Portfolio processStatements(Portfolio ptf, Stream<String> statementContents) {
        RawBankTransactionList rawTranList = parseStatements(statementContents);
        List<RawBankTransaction> rawTrans = rawTranList.getTransactions();

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
            Set<String> ptfTranIds = ptf.getTransactions().stream().map(Transaction::getId).collect(Collectors.toSet());
            rawTrans.removeIf(t -> ptfTranIds.contains(t.getId()));
        }

        List<Transaction> ptfTrans = ptf.getTransactions();
        for (int i = 0, rawTransSize = rawTrans.size(); i < rawTransSize; i++) {
            RawBankTransaction rawTran = rawTrans.get(i);

            LocalDate tranDate = rawTran.getDate();

            try {
                TransactionType tranType = detectTranType(rawTran);
                String id = rawTran.getId();
                BigDecimal rawValue = rawTran.getVolume();
                Currency ccy = rawTran.getCcy();
                String text = rawTran.getAdditionals().toString();
                BigDecimal tax = null;
                BigDecimal grossValue = rawValue;
                BigDecimal netValue = rawValue;
                BigDecimal income = rawValue;

                {
                    TransactionType nextTranType;
                    BigDecimal nextRawValue;
                    Currency nextCcy;
                    String nextRawType;
                    {
                        RawBankTransaction nextRawTran = i < rawTransSize - 1 ? rawTrans.get(i + 1) : null;
                        if (nextRawTran != null
                            && tranDate.isEqual(nextRawTran.getDate())
                        ) {
                            nextTranType = detectTranType(nextRawTran);
                            nextRawValue = nextRawTran.getVolume();
                            nextCcy = nextRawTran.getCcy();
                            nextRawType = nextRawTran.getType();
                        } else {
                            nextTranType = null;
                            nextRawValue = null;
                            nextCcy = null;
                            nextRawType = null;
                        }
                    }
                    if (TransactionType.INTEREST.equals(tranType)
                        && TransactionType.TAX.equals(nextTranType)
                        && "Odvod daně z úroků".equals(nextRawType)
                    ) {
                        assertEqual(nextCcy, ccy);
                        tax = nextRawValue;
                        netValue = grossValue.add(tax);
                        i++;
                    }
                }

                Transaction newTran = new Transaction();
                newTran.setType(tranType);
                newTran.setId(id);
                newTran.setDate(tranDate.atStartOfDay(LazyHolder.FIO_TIME_ZONE));
                newTran.setText(text);
                newTran.setCurrency(ccy);
                newTran.setGrossValue(grossValue);
                newTran.setNetValue(netValue);
                newTran.setQty(ZERO);
                newTran.setIncome(income);
                newTran.setFees(ZERO);
                newTran.setTax(tax);
                newTran.setSettlementDate(tranDate);
                ptfTrans.add(newTran);

                ptfManager.applyTransaction(ptf, newTran);

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
                .map(filePath -> IOUtil.readTextFileContent(filePath, StandardCharsets.UTF_8));
        return processStatements(ptf, statementContentStream);
    }

    @Override
    public Portfolio processStatements(
            String apiKey,
            LocalDate fromDayIncl,
            LocalDate toDayIncl,
            Function<String, String> fetcher
    ) {
        String url = String.format(LazyHolder.URL_FORMAT, apiKey, fromDayIncl, toDayIncl);
        String xml = fetcher.apply(url);
        return processStatements(Stream.of(xml));
    }

    @Override
    public String fetchStatement(String apiKey, LocalDate fromDayIncl, LocalDate toDayIncl) {
        String url = String.format(LazyHolder.URL_FORMAT, apiKey, fromDayIncl, toDayIncl);
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url)).build();
            HttpResponse<String> resp = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());
            return resp.body();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }


    @Override
    public Portfolio processStatements(Collection<String> statementFilePaths) {
        Stream<String> statementContentStream = statementFilePaths
                .stream()
                .map(Path::of)
                .map(filePath -> IOUtil.readTextFileContent(filePath, StandardCharsets.UTF_8));
        return processStatements(statementContentStream);
    }

    @Override
    public Portfolio processStatements(Stream<String> statementContents) {
        return processStatements(null, statementContents);
    }

    @SuppressWarnings("SpellCheckingInspection")
    private TransactionType detectTranType(RawBankTransaction tran) {
        String rawType = tran.getType();
        if ("Bezhotovostní příjem".equals(rawType)) {
            return TransactionType.CASH_TOP_UP;
        }
        if ("Příjem převodem uvnitř banky".equals(rawType)) {
            return TransactionType.CASH_TOP_UP;
        }
        if ("Platba kartou".equals(rawType)) {
            return TransactionType.CASH_WITHDRAWAL;
        }
        if ("Bezhotovostní platba".equals(rawType)) {
            return TransactionType.CASH_WITHDRAWAL;
        }
        if ("Platba převodem uvnitř banky".equals(rawType)) {
            return TransactionType.CASH_WITHDRAWAL;
        }
        if ("Připsaný úrok".equals(rawType)) {
            return TransactionType.INTEREST;
        }
        if ("Odvod daně z úroků".equals(rawType)) {
            return TransactionType.TAX;
        }
        throw new IllegalArgumentException("" + tran);

    }
}