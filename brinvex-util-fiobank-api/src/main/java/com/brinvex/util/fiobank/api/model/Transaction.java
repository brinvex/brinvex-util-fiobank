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
package com.brinvex.util.fiobank.api.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZonedDateTime;

@SuppressWarnings("UnusedReturnValue")
public class Transaction implements Serializable {

    private String id;

    private ZonedDateTime date;

    private TransactionType type;

    private Country country;

    private String symbol;

    private BigDecimal qty;

    private Currency ccy;

    private BigDecimal price;

    private BigDecimal grossValue;

    private BigDecimal netValue;

    private BigDecimal tax;

    private BigDecimal fees;

    private LocalDate settlementDate;

    private String bunchId;

    private String note;

    public String getId() {
        return id;
    }

    public Transaction setId(String id) {
        this.id = id;
        return this;
    }

    public ZonedDateTime getDate() {
        return date;
    }

    public Transaction setDate(ZonedDateTime date) {
        this.date = date;
        return this;
    }

    public TransactionType getType() {
        return type;
    }

    public Transaction setType(TransactionType type) {
        this.type = type;
        return this;
    }

    public Country getCountry() {
        return country;
    }

    public Transaction setCountry(Country country) {
        this.country = country;
        return this;
    }

    public String getSymbol() {
        return symbol;
    }

    public Transaction setSymbol(String symbol) {
        this.symbol = symbol;
        return this;
    }

    public BigDecimal getQty() {
        return qty;
    }

    public Transaction setQty(BigDecimal qty) {
        this.qty = qty;
        return this;
    }

    public Currency getCcy() {
        return ccy;
    }

    public Transaction setCcy(Currency ccy) {
        this.ccy = ccy;
        return this;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public Transaction setPrice(BigDecimal price) {
        this.price = price;
        return this;
    }

    public BigDecimal getGrossValue() {
        return grossValue;
    }

    public Transaction setGrossValue(BigDecimal grossValue) {
        this.grossValue = grossValue;
        return this;
    }

    public BigDecimal getNetValue() {
        return netValue;
    }

    public Transaction setNetValue(BigDecimal netValue) {
        this.netValue = netValue;
        return this;
    }

    public BigDecimal getTax() {
        return tax;
    }

    public Transaction setTax(BigDecimal tax) {
        this.tax = tax;
        return this;
    }

    public BigDecimal getFees() {
        return fees;
    }

    public Transaction setFees(BigDecimal fees) {
        this.fees = fees;
        return this;
    }

    public LocalDate getSettlementDate() {
        return settlementDate;
    }

    public Transaction setSettlementDate(LocalDate settlementDate) {
        this.settlementDate = settlementDate;
        return this;
    }

    public String getBunchId() {
        return bunchId;
    }

    public Transaction setBunchId(String bunchId) {
        this.bunchId = bunchId;
        return this;
    }

    public String getNote() {
        return note;
    }

    public Transaction setNote(String note) {
        this.note = note;
        return this;
    }

    @Override
    public String toString() {
        return "Transaction{" +
               "id='" + id + '\'' +
               ", date=" + date +
               ", type=" + type +
               ", country=" + country +
               ", symbol='" + symbol + '\'' +
               ", qty=" + qty +
               ", ccy=" + ccy +
               ", price=" + price +
               ", grossValue=" + grossValue +
               ", netValue=" + netValue +
               ", tax=" + tax +
               ", fees=" + fees +
               ", settlementDate=" + settlementDate +
               ", bunchId='" + bunchId + '\'' +
               ", note='" + note + '\'' +
               '}';
    }
}
