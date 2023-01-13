package com.brinvex.util.fiobank.api.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Portfolio implements Serializable {

    private String accountNumber;

    private LocalDate periodFrom;

    private LocalDate periodTo;

    private Map<Currency, BigDecimal> cash = new HashMap<>();

    private List<Position> positions = new ArrayList<>();

    private List<Transaction> transactions = new ArrayList<>();

    public String getAccountNumber() {
        return accountNumber;
    }

    public void setAccountNumber(String accountNumber) {
        this.accountNumber = accountNumber;
    }

    public LocalDate getPeriodFrom() {
        return periodFrom;
    }

    public void setPeriodFrom(LocalDate periodFrom) {
        this.periodFrom = periodFrom;
    }

    public LocalDate getPeriodTo() {
        return periodTo;
    }

    public void setPeriodTo(LocalDate periodTo) {
        this.periodTo = periodTo;
    }

    public Map<Currency, BigDecimal> getCash() {
        return cash;
    }

    public void setCash(Map<Currency, BigDecimal> cash) {
        this.cash = cash;
    }

    public List<Position> getPositions() {
        return positions;
    }

    public void setPositions(List<Position> positions) {
        this.positions = positions;
    }

    public List<Transaction> getTransactions() {
        return transactions;
    }

    public void setTransactions(List<Transaction> transactions) {
        this.transactions = transactions;
    }

    @Override
    public String toString() {
        return "Portfolio{" +
               "accountNumber='" + accountNumber + '\'' +
               ", periodFrom=" + periodFrom +
               ", periodTo=" + periodTo +
               ", cash=" + cash +
               ", positions=" + positions +
               '}';
    }
}
