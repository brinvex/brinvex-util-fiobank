package com.brinvex.util.fiobank.api.model;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.List;

public class RawTransactionList implements Serializable {

    private String accountNumber;

    private LocalDate periodFrom;

    private LocalDate periodTo;

    private List<RawTransaction> transactions;

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

    public List<RawTransaction> getTransactions() {
        return transactions;
    }

    public void setTransactions(List<RawTransaction> transactions) {
        this.transactions = transactions;
    }

    @Override
    public String toString() {
        return "RawTransactionList{" +
               "accountNumber='" + accountNumber + '\'' +
               ", periodFrom=" + periodFrom +
               ", periodTo=" + periodTo +
               ", transactions=" + transactions +
               '}';
    }
}
