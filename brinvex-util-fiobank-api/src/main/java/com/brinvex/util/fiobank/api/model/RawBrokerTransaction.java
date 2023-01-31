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
import java.time.LocalDateTime;

public class RawBrokerTransaction implements Serializable {

    private LocalDateTime tradeDate;

    private RawBrokerTranDirection direction;

    private String symbol;

    private String rawSymbol;

    private BigDecimal price;

    private BigDecimal shares;

    private Currency ccy;

    private String rawCurrency;

    private BigDecimal volumeCzk;

    private BigDecimal feesCzk;

    private BigDecimal volumeUsd;

    private BigDecimal feesUsd;

    private BigDecimal volumeEur;

    private BigDecimal feesEur;

    private String market;

    private String instrumentName;

    private LocalDate settlementDate;

    private String status;

    private String orderId;

    private String text;

    private String userComments;

    private Lang lang;

    public LocalDateTime getTradeDate() {
        return tradeDate;
    }

    public void setTradeDate(LocalDateTime tradeDate) {
        this.tradeDate = tradeDate;
    }

    public RawBrokerTranDirection getDirection() {
        return direction;
    }

    public void setDirection(RawBrokerTranDirection direction) {
        this.direction = direction;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getRawSymbol() {
        return rawSymbol;
    }

    public void setRawSymbol(String rawSymbol) {
        this.rawSymbol = rawSymbol;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public BigDecimal getShares() {
        return shares;
    }

    public void setShares(BigDecimal shares) {
        this.shares = shares;
    }

    public Currency getCcy() {
        return ccy;
    }

    public void setCcy(Currency ccy) {
        this.ccy = ccy;
    }

    public String getRawCurrency() {
        return rawCurrency;
    }

    public void setRawCurrency(String rawCurrency) {
        this.rawCurrency = rawCurrency;
    }

    public BigDecimal getVolumeCzk() {
        return volumeCzk;
    }

    public void setVolumeCzk(BigDecimal volumeCzk) {
        this.volumeCzk = volumeCzk;
    }

    public BigDecimal getFeesCzk() {
        return feesCzk;
    }

    public void setFeesCzk(BigDecimal feesCzk) {
        this.feesCzk = feesCzk;
    }

    public BigDecimal getVolumeUsd() {
        return volumeUsd;
    }

    public void setVolumeUsd(BigDecimal volumeUsd) {
        this.volumeUsd = volumeUsd;
    }

    public BigDecimal getFeesUsd() {
        return feesUsd;
    }

    public void setFeesUsd(BigDecimal feesUsd) {
        this.feesUsd = feesUsd;
    }

    public BigDecimal getVolumeEur() {
        return volumeEur;
    }

    public void setVolumeEur(BigDecimal volumeEur) {
        this.volumeEur = volumeEur;
    }

    public BigDecimal getFeesEur() {
        return feesEur;
    }

    public void setFeesEur(BigDecimal feesEur) {
        this.feesEur = feesEur;
    }

    public String getMarket() {
        return market;
    }

    public void setMarket(String market) {
        this.market = market;
    }

    public String getInstrumentName() {
        return instrumentName;
    }

    public void setInstrumentName(String instrumentName) {
        this.instrumentName = instrumentName;
    }

    public LocalDate getSettlementDate() {
        return settlementDate;
    }

    public void setSettlementDate(LocalDate settlementDate) {
        this.settlementDate = settlementDate;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getUserComments() {
        return userComments;
    }

    public void setUserComments(String userComments) {
        this.userComments = userComments;
    }

    public Lang getLang() {
        return lang;
    }

    public void setLang(Lang lang) {
        this.lang = lang;
    }

    @Override
    public String toString() {
        return "RawTransaction{" +
               "tradeDate=" + tradeDate +
               ", direction=" + direction +
               ", symbol='" + symbol + '\'' +
               ", rawSymbol='" + rawSymbol + '\'' +
               ", price=" + price +
               ", shares=" + shares +
               ", currency=" + ccy +
               ", rawCurrency='" + rawCurrency + '\'' +
               ", volumeCzk=" + volumeCzk +
               ", feesCzk=" + feesCzk +
               ", volumeUsd=" + volumeUsd +
               ", feesUsd=" + feesUsd +
               ", volumeEur=" + volumeEur +
               ", feesEur=" + feesEur +
               ", market='" + market + '\'' +
               ", instrumentName='" + instrumentName + '\'' +
               ", settlementDate=" + settlementDate +
               ", status='" + status + '\'' +
               ", orderId='" + orderId + '\'' +
               ", text='" + text + '\'' +
               ", userComments='" + userComments + '\'' +
               ", lang=" + lang +
               '}';
    }
}
