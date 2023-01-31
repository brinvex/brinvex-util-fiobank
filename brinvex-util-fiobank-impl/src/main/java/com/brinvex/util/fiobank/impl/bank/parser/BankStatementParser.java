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
package com.brinvex.util.fiobank.impl.bank.parser;

import com.brinvex.util.fiobank.api.model.Currency;
import com.brinvex.util.fiobank.api.model.RawBankTransaction;
import com.brinvex.util.fiobank.api.model.RawBankTransactionList;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.StringReader;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import static com.brinvex.util.fiobank.impl.util.ValidationUtil.assertTrue;

public class BankStatementParser {

    private static class LazyHolder {
        private static final XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();

        private static final DateTimeFormatter fioDtf = DateTimeFormatter.ISO_OFFSET_DATE;
    }

    @SuppressWarnings("DataFlowIssue")
    public RawBankTransactionList parseStatement(String statementContent) {

        RawBankTransactionList tranList = new RawBankTransactionList();
        ArrayList<RawBankTransaction> trans = new ArrayList<>();
        tranList.setTransactions(trans);
        try {
            XMLEventReader reader = LazyHolder.xmlInputFactory.createXMLEventReader(new StringReader(statementContent));

            RawBankTransaction tran = null;
            while (reader.hasNext()) {
                XMLEvent xmlEvent = reader.nextEvent();
                if (xmlEvent.isStartElement()) {
                    StartElement startElement = xmlEvent.asStartElement();
                    String elementName = startElement.getName().getLocalPart();

                    switch (elementName) {
                        case "accountId":
                            xmlEvent = reader.nextEvent();
                            tranList.setAccountNumber(xmlEvent.asCharacters().getData());
                            reader.nextEvent();
                            break;
                        case "dateStart":
                            xmlEvent = reader.nextEvent();
                            tranList.setPeriodFrom(parseDay(xmlEvent));
                            reader.nextEvent();
                            break;
                        case "dateEnd":
                            xmlEvent = reader.nextEvent();
                            tranList.setPeriodTo(parseDay(xmlEvent));
                            reader.nextEvent();
                            break;
                        case "Transaction":
                            assertTrue(tran == null);
                            tran = new RawBankTransaction();
                            tran.setAdditionals(new LinkedHashMap<>());
                            break;
                        case "column_22":
                            xmlEvent = reader.nextEvent();
                            tran.setId(xmlEvent.asCharacters().getData());
                            reader.nextEvent();
                            break;
                        case "column_0":
                            xmlEvent = reader.nextEvent();
                            tran.setDate(parseDay(xmlEvent));
                            reader.nextEvent();
                            break;
                        case "column_1":
                            xmlEvent = reader.nextEvent();
                            tran.setVolume(new BigDecimal(xmlEvent.asCharacters().getData()));
                            reader.nextEvent();
                            break;
                        case "column_8":
                            xmlEvent = reader.nextEvent();
                            tran.setType(xmlEvent.asCharacters().getData());
                            reader.nextEvent();
                            break;
                        case "column_14":
                            xmlEvent = reader.nextEvent();
                            tran.setCcy(Currency.valueOf(xmlEvent.asCharacters().getData()));
                            reader.nextEvent();
                            break;
                        default:
                            if (elementName.startsWith("column_") && tran != null) {
                                String columnName = startElement.getAttributeByName(new QName("name")).getValue();
                                xmlEvent = reader.nextEvent();
                                String columnValue = xmlEvent.asCharacters().getData();
                                tran.getAdditionals().put(columnName, columnValue);
                            }
                    }
                } else if (xmlEvent.isEndElement()) {
                    EndElement endElement = xmlEvent.asEndElement();
                    String elementName = endElement.getName().getLocalPart();
                    if (elementName.equals("Transaction")) {
                        assertTrue(tran != null);
                        trans.add(tran);
                        tran = null;
                    }
                }
            }
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
        return tranList;
    }

    private static LocalDate parseDay(XMLEvent xmlEvent) {
        return LocalDate.parse(xmlEvent.asCharacters().getData(), LazyHolder.fioDtf);
    }
}
