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
import com.brinvex.util.fiobank.api.model.RawBankTransactionList;
import com.brinvex.util.fiobank.api.model.Transaction;
import com.brinvex.util.fiobank.api.service.FioBankService;
import com.brinvex.util.fiobank.api.service.FioServiceFactory;
import com.brinvex.util.fiobank.impl.util.IOUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class FioBankServiceTest {

    private static final FioBankService bankSvc = FioServiceFactory.INSTANCE.getBankService();

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
    void parseStatements_base() {
        List<String> testFilePaths = testHelper.getTestFilePaths(f ->
                f.equals("Fio_Bank_Transactions_1_201610-202302.xml") ||
                f.equals("Fio_Bank_Transactions_2_201610-202302.xml") ||
                f.equals("Fio_Bank_Transactions_3_201610-202302.xml") ||
                f.equals("Fio_Bank_Transactions_4_201610-202302.xml")
        );
        for (String testFilePath : testFilePaths) {
            RawBankTransactionList tranList = bankSvc.parseStatements(List.of(testFilePath));
            assertNotNull(tranList);
        }
    }

    @Test
    void processStatements_base() {
        List<String> testFilePaths = testHelper.getTestFilePaths(f ->
                f.equals("Fio_Bank_Transactions_1_201610-202302.xml") ||
                f.equals("Fio_Bank_Transactions_2_201610-202302.xml") ||
                f.equals("Fio_Bank_Transactions_3_201610-202302.xml") ||
                f.equals("Fio_Bank_Transactions_4_201610-202302.xml")
        );
        for (String testFilePath : testFilePaths) {
            Portfolio ptf = bankSvc.processStatements(List.of(testFilePath));
            assertNotNull(ptf);
            assertEquals(1, ptf.getCash().size());
            for (Transaction t : ptf.getTransactions()) {
                assertNotNull(t.getId());
            }
        }
    }

    @Test
    void fetch() {
        String testFilePath = testHelper.getTestFilePath(f -> f.equals("Fio_Bank_apiKey"));
        if (testFilePath != null) {
            String apiKey = IOUtil.readTextFileContent(Path.of(testFilePath), StandardCharsets.UTF_8).trim();
            String xml = bankSvc.fetchStatement(apiKey, LocalDate.parse("2022-01-01"), LocalDate.parse("2023-02-01"));
            Portfolio ptf = bankSvc.processStatements(Stream.of(xml));
            assertNotNull(ptf);
            assertEquals(1, ptf.getCash().size());
        }
    }
}