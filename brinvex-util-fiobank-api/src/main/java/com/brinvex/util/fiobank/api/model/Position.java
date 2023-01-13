package com.brinvex.util.fiobank.api.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class Position implements Serializable {

    private Country country;

    private String symbol;

    private BigDecimal qty;

    private List<Transaction> transactions = new ArrayList<>();

    public Country getCountry() {
        return country;
    }

    public void setCountry(Country country) {
        this.country = country;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public BigDecimal getQty() {
        return qty;
    }

    public void setQty(BigDecimal qty) {
        this.qty = qty;
    }

    public List<Transaction> getTransactions() {
        return transactions;
    }

    public void setTransactions(List<Transaction> transactions) {
        this.transactions = transactions;
    }

    @Override
    public String toString() {
        return "Position{" +
               "country='" + country + '\'' +
               ", symbol='" + symbol + '\'' +
               ", qty=" + qty +
               '}';
    }
}
