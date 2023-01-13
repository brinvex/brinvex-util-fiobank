package com.brinvex.util.fiobank.impl;

import com.brinvex.util.fiobank.api.model.Portfolio;
import com.brinvex.util.fiobank.api.model.Position;
import com.brinvex.util.fiobank.api.model.RawTransaction;
import com.brinvex.util.fiobank.api.model.RawTransactionList;
import com.brinvex.util.fiobank.api.service.FiobankBrokerService;
import com.brinvex.util.fiobank.api.service.FiobankServiceFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FiobankBrokerServiceTest {

    private static final FiobankBrokerService fiobankSvc = FiobankServiceFactory.INSTANCE.getBrokerService();

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
    void parseStatements_lang() {
        List<String> testFilePaths = testHelper.getTestFilePaths(f ->
                f.equals("Fio_Broker_Transactions_2022_EN.csv") ||
                f.equals("Fio_Broker_Transactions_2022_CZ.csv") ||
                f.equals("Fio_Broker_Transactions_2022_SK.csv")
        );
        if (!testFilePaths.isEmpty()) {
            Consumer<RawTransaction> langCleaner = t -> {
                t.setText(null);
                t.setStatus(null);
                t.setMarket(null);
                t.setLang(null);
            };

            List<RawTransactionList> tranLists = testFilePaths
                    .stream()
                    .map(List::of)
                    .map(fiobankSvc::parseStatements)
                    .collect(Collectors.toList());

            if (tranLists.size() >= 2) {
                RawTransactionList tranList0 = tranLists.get(0);
                List<RawTransaction> trans0 = tranList0.getTransactions();

                trans0.forEach(langCleaner);

                for (int i = 1; i < tranLists.size(); i++) {
                    RawTransactionList tranList = tranLists.get(i);
                    List<RawTransaction> trans = tranList.getTransactions();
                    trans.forEach(langCleaner);
                    testHelper.assertJsonEquals(tranList0, tranList);
                }
            }
        }
    }

    @Test
    void parseStatements_sort() {
        List<String> testFilePaths = testHelper.getTestFilePaths(fileName -> fileName.endsWith(".csv"));
        if (!testFilePaths.isEmpty()) {
            RawTransactionList rawTranList = fiobankSvc.parseStatements(testFilePaths);
            List<RawTransaction> rawTrans = rawTranList.getTransactions();
            List<RawTransaction> sortedTransactions = rawTrans
                    .stream()
                    .sorted(comparing(RawTransaction::getTradeDate))
                    .collect(Collectors.toList());
            assertEquals(sortedTransactions, rawTrans);
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
            Portfolio ptfEn = fiobankSvc.processStatements(testFilePathsEn);
            Portfolio ptfSk = fiobankSvc.processStatements(testFilePathsSk);
            Portfolio ptfCz = fiobankSvc.processStatements(testFilePathsCz);

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
            Portfolio ptf = fiobankSvc.processStatements(transFilePaths);

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
    void processStatements_duplicate() {
        List<String> testFilePaths = testHelper.getTestFilePaths(fileName -> fileName.endsWith("SK.csv"));
        if (!testFilePaths.isEmpty()) {
            String testFilePath = testFilePaths.get(0);
            Portfolio ptf1 = fiobankSvc.processStatements(List.of(testFilePath));
            Portfolio ptf2 = fiobankSvc.processStatements(List.of(testFilePath, testFilePath));
            assertEquals(ptf1.getTransactions().size(), ptf2.getTransactions().size());
        }
    }
}