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
package com.brinvex.util.fiobank.impl;

import com.brinvex.util.fiobank.api.model.Portfolio;
import com.brinvex.util.fiobank.api.model.Position;
import com.brinvex.util.fiobank.api.model.RawBrokerTransaction;
import com.brinvex.util.fiobank.api.model.RawBrokerTransactionList;
import com.brinvex.util.fiobank.api.model.Transaction;
import com.brinvex.util.fiobank.api.model.TransactionType;
import com.brinvex.util.fiobank.api.service.FioBrokerService;
import com.brinvex.util.fiobank.api.service.FioServiceFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.math.BigDecimal.ZERO;
import static java.util.Comparator.comparing;
import static java.util.Comparator.naturalOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FioBrokerServiceTest {

    private static final FioBrokerService brokerSvc = FioServiceFactory.INSTANCE.getBrokerService();

    private static TestHelper testHelper;

    @BeforeAll
    static void beforeAll() {
        testHelper = new TestHelper();
    }

    @AfterAll
    static void afterAll() throws Exception {
        testHelper.close();
    }

    @Test
    void processStatements_base() {
        List<String> testFilePaths = testHelper.getTestFilePaths(f ->
                f.equals("Fio_Broker_Transactions_2021_B_SK.csv") ||
                f.equals("Fio_Broker_Transactions_2022_B_SK.csv")
        );
        if (!testFilePaths.isEmpty()) {
            brokerSvc.processStatements(testFilePaths);
        }
    }


    @Test
    void parseStatements_lang() {
        List<String> testFilePaths = testHelper.getTestFilePaths(f ->
                f.equals("Fio_Broker_Transactions_2022_EN.csv") ||
                f.equals("Fio_Broker_Transactions_2022_CZ.csv") ||
                f.equals("Fio_Broker_Transactions_2022_SK.csv")
        );
        if (!testFilePaths.isEmpty()) {
            Consumer<RawBrokerTransaction> langCleaner = t -> {
                t.setText(null);
                t.setStatus(null);
                t.setMarket(null);
                t.setLang(null);
            };

            List<RawBrokerTransactionList> tranLists = testFilePaths
                    .stream()
                    .map(List::of)
                    .map(brokerSvc::parseStatements)
                    .collect(Collectors.toList());

            if (tranLists.size() >= 2) {
                RawBrokerTransactionList tranList0 = tranLists.get(0);
                List<RawBrokerTransaction> trans0 = tranList0.getTransactions();

                trans0.forEach(langCleaner);

                for (int i = 1; i < tranLists.size(); i++) {
                    RawBrokerTransactionList tranList = tranLists.get(i);
                    List<RawBrokerTransaction> trans = tranList.getTransactions();
                    trans.forEach(langCleaner);
                    testHelper.assertJsonEquals(tranList0, tranList);
                }
            }
        }
    }

    @Test
    void parseStatements_sort() {
        List<String> testFilePaths = testHelper.getTestFilePaths(fileName -> fileName.endsWith(".csv") && !fileName.contains("_B_"));
        if (!testFilePaths.isEmpty()) {
            RawBrokerTransactionList rawTranList = brokerSvc.parseStatements(testFilePaths);
            List<RawBrokerTransaction> rawTrans = rawTranList.getTransactions();
            List<RawBrokerTransaction> sortedTransactions = rawTrans
                    .stream()
                    .sorted(comparing(RawBrokerTransaction::getTradeDate))
                    .collect(Collectors.toList());
            assertEquals(sortedTransactions, rawTrans);
        }
    }

    @Test
    void parseStatements_duplicate() {
        List<String> testFilePaths1 = testHelper.getTestFilePaths(fileName ->
                fileName.endsWith("Fio_Broker_Transactions_2019.csv") ||
                fileName.endsWith("Fio_Broker_Transactions_2020.csv")
        );
        List<String> testFilePaths2 = testHelper.getTestFilePaths(fileName ->
                fileName.endsWith("Fio_Broker_Transactions_2020_SK.csv") ||
                fileName.endsWith("Fio_Broker_Transactions_2019_SK.csv")
        );
        if (!testFilePaths1.isEmpty() && !testFilePaths2.isEmpty()) {

            RawBrokerTransactionList tranList1 = brokerSvc.parseStatements(testFilePaths1);

            testFilePaths2.addAll(testFilePaths1);
            RawBrokerTransactionList tranList2 = brokerSvc.parseStatements(testFilePaths2);

            assertEquals(tranList1.getTransactions().size(), tranList2.getTransactions().size());
        }
    }

    @Test
    void processStatements_lang() {
        List<String> testFilePathsEn = testHelper.getTestFilePaths(f ->
                f.equals("Fio_Broker_Transactions_2019_EN.csv") ||
                f.equals("Fio_Broker_Transactions_2020_EN.csv") ||
                f.equals("Fio_Broker_Transactions_2021_EN.csv") ||
                f.equals("Fio_Broker_Transactions_2022_EN.csv")
        );
        List<String> testFilePathsSk = testHelper.getTestFilePaths(f ->
                f.equals("Fio_Broker_Transactions_2019_SK.csv") ||
                f.equals("Fio_Broker_Transactions_2020_SK.csv") ||
                f.equals("Fio_Broker_Transactions_2021_SK.csv") ||
                f.equals("Fio_Broker_Transactions_2022_SK.csv")
        );
        List<String> testFilePathsCz = testHelper.getTestFilePaths(f ->
                f.equals("Fio_Broker_Transactions_2019_CZ.csv") ||
                f.equals("Fio_Broker_Transactions_2020_CZ.csv") ||
                f.equals("Fio_Broker_Transactions_2021_CZ.csv") ||
                f.equals("Fio_Broker_Transactions_2022_CZ.csv")
        );
        int years = 4;
        if (testFilePathsSk.size() == years && testFilePathsCz.size() == years && testFilePathsEn.size() == years) {
            Portfolio ptfEn = brokerSvc.processStatements(testFilePathsEn);
            Portfolio ptfSk = brokerSvc.processStatements(testFilePathsSk);
            Portfolio ptfCz = brokerSvc.processStatements(testFilePathsCz);

            ptfEn.getTransactions().forEach(t -> t.setBunchId(null));
            ptfCz.getTransactions().forEach(t -> t.setBunchId(null));
            ptfSk.getTransactions().forEach(t -> t.setBunchId(null));

            testHelper.assertJsonEquals(ptfEn, ptfCz);
            testHelper.assertJsonEquals(ptfEn, ptfSk);
        }
    }

    @Test
    void processStatements_data_2019_2022() {
        List<String> transFilePaths = testHelper.getTestFilePaths(
                fileName -> fileName.endsWith("Fio_Broker_Transactions_2019_SK.csv")
                            || fileName.endsWith("Fio_Broker_Transactions_2020_SK.csv")
                            || fileName.endsWith("Fio_Broker_Transactions_2021_SK.csv")
                            || fileName.endsWith("Fio_Broker_Transactions_2022_SK.csv")
        );
        String ptfFilePath = testHelper.getTestFilePath(fileName -> fileName.endsWith("Fio_Broker_Portfolio_2019_2022.json"));
        if (!transFilePaths.isEmpty() && ptfFilePath != null) {
            Portfolio ptf = brokerSvc.processStatements(transFilePaths);

            Portfolio expectedPtf = testHelper.readFromJson(ptfFilePath, Portfolio.class);

            assertEquals(expectedPtf.getAccountNumber(), ptf.getAccountNumber());
            assertEquals(expectedPtf.getCash(), ptf.getCash());
            assertEquals(expectedPtf.getPeriodFrom(), ptf.getPeriodFrom());
            assertEquals(expectedPtf.getPeriodTo(), ptf.getPeriodTo());

            List<Position> positions = ptf.getPositions()
                    .stream()
                    .sorted(comparing(Position::getSymbol))
                    .peek(p -> p.getTransactions().clear())
                    .collect(Collectors.toList());
            testHelper.assertJsonEquals(expectedPtf.getPositions(), positions);
        }
    }

    @Test
    void processStatements_iterative() {
        List<String> transFilePaths = new ArrayList<>(testHelper.getTestFilePaths(
                fileName -> fileName.endsWith("Fio_Broker_Transactions_2019_SK.csv")
                            || fileName.endsWith("Fio_Broker_Transactions_2020_SK.csv")
                            || fileName.endsWith("Fio_Broker_Transactions_2021_SK.csv")
                            || fileName.endsWith("Fio_Broker_Transactions_2022_SK.csv")
        ));
        if (!transFilePaths.isEmpty()) {
            Portfolio ptf1 = brokerSvc.processStatements(transFilePaths);

            Portfolio ptf2 = null;
            transFilePaths.sort(naturalOrder());
            for (String transFilePath : transFilePaths) {
                ptf2 = brokerSvc.processStatements(ptf2, List.of(transFilePath));
            }

            assertNotNull(ptf2);
            assertEquals(ptf2.getAccountNumber(), ptf1.getAccountNumber());
            assertEquals(ptf2.getCash(), ptf1.getCash());
            assertEquals(ptf2.getPeriodFrom(), ptf1.getPeriodFrom());
            assertEquals(ptf2.getPeriodTo(), ptf1.getPeriodTo());

            List<Position> positions1 = ptf1.getPositions()
                    .stream()
                    .sorted(comparing(Position::getSymbol))
                    .peek(p -> p.getTransactions().clear())
                    .collect(Collectors.toList());
            List<Position> positions2 = ptf2.getPositions()
                    .stream()
                    .sorted(comparing(Position::getSymbol))
                    .peek(p -> p.getTransactions().clear())
                    .collect(Collectors.toList());
            testHelper.assertJsonEquals(positions2, positions1);

            List<Transaction> trans1 = ptf2.getTransactions();
            List<Transaction> trans2 = ptf1.getTransactions();
            trans1.forEach(t -> t.setBunchId(null));
            trans2.forEach(t -> t.setBunchId(null));
            testHelper.assertJsonEquals(trans1, trans2);
        }
    }

    @Test
    void processStatements_duplicate() {
        List<String> testFilePaths1 = testHelper.getTestFilePaths(fileName ->
                fileName.endsWith("Fio_Broker_Transactions_2019.csv") ||
                fileName.endsWith("Fio_Broker_Transactions_2020.csv")
        );
        List<String> testFilePaths2 = testHelper.getTestFilePaths(fileName ->
                fileName.endsWith("Fio_Broker_Transactions_2020_SK.csv") ||
                fileName.endsWith("Fio_Broker_Transactions_2019_SK.csv")
        );
        if (!testFilePaths1.isEmpty() && !testFilePaths2.isEmpty()) {
            Portfolio ptf1 = brokerSvc.processStatements(testFilePaths1);

            testFilePaths2.addAll(testFilePaths1);
            Portfolio ptf2 = brokerSvc.processStatements(testFilePaths2);

            assertEquals(ptf1.getTransactions().size(), ptf2.getTransactions().size());
        }
    }

    @Test
    void processStatements_typeConstraints() {
        List<String> testFilePaths1 = testHelper.getTestFilePaths(fileName ->
                fileName.endsWith("Fio_Broker_Transactions_2019.csv"));
        if (!testFilePaths1.isEmpty()) {
            Portfolio ptf1 = brokerSvc.processStatements(testFilePaths1);

            for (Transaction t : ptf1.getTransactions()) {
                if (t.getType().equals(TransactionType.FEE)) {
                    if (t.getText().contains("ADR")) {
                        assertNotNull(t.getSymbol());
                        assertNotNull(t.getCountry());
                    }
                }
            }
        }
    }

    @Test
    void processStatements_dividendTax() {
        List<String> testFilePaths1 = testHelper.getTestFilePaths(fileName ->
                (
                        fileName.contains("Fio_Broker_Transactions_2019") ||
                        fileName.contains("Fio_Broker_Transactions_2020") ||
                        fileName.contains("Fio_Broker_Transactions_2021") ||
                        fileName.contains("Fio_Broker_Transactions_2022")
                )
                && !fileName.contains("_B_")
        );
        if (!testFilePaths1.isEmpty()) {
            Portfolio ptf1 = brokerSvc.processStatements(testFilePaths1);

            for (Transaction t : ptf1.getTransactions()) {
                if (t.getType().equals(TransactionType.CASH_DIVIDEND)) {
                    String text = t.getText();
                    if (text.contains(" daň ")) {
                        assertTrue(t.getTax().compareTo(ZERO) < 0);
                    }
                }
            }
        }
    }

}