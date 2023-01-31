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
import com.brinvex.util.fiobank.api.model.RawBrokerTransactionList;
import com.brinvex.util.fiobank.api.service.exception.FiobankServiceException;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class BrokerStatementParser {

    private static class LazyHolder {

        private static final Pattern COLUMN_DELIMITER_PATTERN = Pattern.compile(";");

    }

    @SuppressWarnings("SpellCheckingInspection")
    public RawBrokerTransactionList parseStatement(String statementContent) {
        List<String> lines = statementContent
                .lines()
                .map(String::trim)
                .collect(Collectors.toList());

        String accountNumber = null;
        LocalDate periodFrom = null;
        LocalDate periodTo = null;
        Lang lang = null;
        Map<TranColumnDef, Integer> headers = null;


        List<RawBrokerTransaction> rawTrans = new ArrayList<>();
        for (int i = 0, linesSize = lines.size(); i < linesSize; i++) {
            String line = lines.get(i);
            if (line.isBlank()) {
                continue;
            }
            try {

                if (accountNumber == null) {
                    Pattern pattern = Pattern.compile(".*:\\s+(?<accountNumber>\\d+)\"");
                    Matcher matcher = pattern.matcher(line);
                    if (!matcher.find()) {
                        throw new FiobankServiceException(String.format("%s - Could not parse account number: '%s'", i + 1, line));
                    }
                    accountNumber = matcher.group("accountNumber");

                    if (line.startsWith("Overview")) {
                        lang = Lang.EN;
                    } else if (line.startsWith("Přehled")) {
                        lang = Lang.CZ;
                    } else if (line.startsWith("Prehľad")) {
                        lang = Lang.SK;
                    } else {
                        throw new FiobankServiceException(String.format("%s - Could not detect lang: '%s'", i + 1, line));
                    }
                    i++; // Skip "Created:" line
                    continue;
                }
                if (periodFrom == null || periodTo == null) {
                    Pattern pattern = Pattern.compile(".*:\\s+(?<periodFrom>\\d{1,2}\\.\\d{1,2}\\.\\d{4})\\s+-\\s+(?<periodTo>\\d{1,2}\\.\\d{1,2}\\.\\d{4})");
                    Matcher matcher = pattern.matcher(line);
                    if (!matcher.find()) {
                        throw new FiobankServiceException(String.format("%s - Could not parse period: '%s'", i + 1, line));
                    }
                    DateTimeFormatter df = DateTimeFormatter.ofPattern("d.M.yyyy");
                    periodFrom = LocalDate.parse(matcher.group("periodFrom"), df);
                    periodTo = LocalDate.parse(matcher.group("periodTo"), df);
                    continue;
                }

                if (headers == null) {
                    headers = new LinkedHashMap<>();
                    String[] headerTitles = LazyHolder.COLUMN_DELIMITER_PATTERN.split(line, -1);
                    for (int j = 0, headerTitlesLength = headerTitles.length; j < headerTitlesLength; j++) {
                        String headerTitle = headerTitles[j];
                        TranColumnDef header = TranColumnDef.ofTitle(headerTitle, lang);
                        if (header == null) {
                            continue;
                        }
                        headers.put(header, j);
                    }
                    Set<TranColumnDef> missingHeaders = new LinkedHashSet<>(TranColumnDef.STANDARD_COLUMNS);
                    missingHeaders.removeAll(headers.keySet());
                    if (!missingHeaders.isEmpty()) {
                        throw new FiobankServiceException(String.format("%s - Mising mandatory headers: %s, line='%s'", i + 1, missingHeaders, line));
                    }
                    continue;
                }

                RawBrokerTransaction rawTran = new RawBrokerTransaction();
                rawTran.setLang(lang);
                {
                    List<String> cells = List.of(LazyHolder.COLUMN_DELIMITER_PATTERN.split(line, -1));
                    if (cells.get(0).isBlank()) {
                        //"Total" row without a date
                        continue;
                    }
                    for (Map.Entry<TranColumnDef, Integer> e : headers.entrySet()) {
                        TranColumnDef columnDef = e.getKey();
                        Integer index = e.getValue();
                        String cell = cells.get(index);
                        cell = cell.trim();
                        columnDef.fill(rawTran, cell, lang);
                    }
                }
                rawTrans.add(rawTran);
            } catch (FiobankServiceException se) {
                throw se;
            } catch (Exception e) {
                throw new FiobankServiceException(String.format("%s - '%s'", i, line), e);
            }

        }

        RawBrokerTransactionList rawTranList = new RawBrokerTransactionList();

        rawTranList.setAccountNumber(accountNumber);
        rawTranList.setPeriodFrom(periodFrom);
        rawTranList.setPeriodTo(periodTo);
        rawTranList.setTransactions(rawTrans);

        return rawTranList;

    }

}
