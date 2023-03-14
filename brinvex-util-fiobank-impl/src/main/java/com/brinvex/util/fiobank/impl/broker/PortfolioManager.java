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
package com.brinvex.util.fiobank.impl.broker;

import com.brinvex.util.fiobank.api.model.Country;
import com.brinvex.util.fiobank.api.model.Currency;
import com.brinvex.util.fiobank.api.model.Portfolio;
import com.brinvex.util.fiobank.api.model.Position;
import com.brinvex.util.fiobank.api.model.Transaction;
import com.brinvex.util.fiobank.api.model.TransactionType;
import com.brinvex.util.fiobank.api.service.exception.FiobankServiceException;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.math.BigDecimal.ZERO;
import static java.util.Objects.requireNonNull;

public class PortfolioManager {

    public Portfolio initPortfolio(String accountNumber, LocalDate periodFrom, LocalDate periodTo) {
        Portfolio ptf = new Portfolio();
        ptf.setAccountNumber(accountNumber);
        ptf.setPeriodFrom(periodFrom);
        ptf.setPeriodTo(periodTo);
        return ptf;
    }

    public Position findPosition(Portfolio ptf, String symbol) {
        requireNonNull(ptf);
        requireNonNull(symbol);
        List<Position> positions = ptf.getPositions()
                .stream()
                .filter(p -> symbol.equals(p.getSymbol()))
                .collect(Collectors.toList());
        int size = positions.size();
        if (size == 0) {
            throw new FiobankServiceException(format("Could not find position by symbol: %s, ptf=%s", symbol, ptf));
        }
        if (size > 1) {
            throw new FiobankServiceException(format("Multiple positions found by symbol: %s, positions=%s, ptf=%s", symbol, positions, ptf));
        }
        return positions.get(0);
    }

    public void applyTransaction(Portfolio ptf, Transaction tran) {
        Set<String> ptfTranIds = ptf.getTransactions()
                .stream()
                .filter(t -> t != tran)
                .map(Transaction::getId)
                .collect(Collectors.toSet());
        String tranId = tran.getId();
        if (ptfTranIds.contains(tranId)) {
            throw new FiobankServiceException(format("Transaction ID conflict: %s", tranId));
        }
        TransactionType tranType = tran.getType();
        boolean tranIsValid = tranType.isValid(tran);
        if (!tranIsValid) {
            throw new FiobankServiceException(format("Invalid transaction: %s", tran));
        }
        Country country = tran.getCountry();
        String symbol = tran.getSymbol();
        Currency ccy = tran.getCcy();
        BigDecimal netValue = tran.getNetValue();
        BigDecimal qty = tran.getQty();

        if (netValue != null && netValue.compareTo(ZERO) != 0) {
            updateCash(ptf, ccy, netValue);
        }

        if (qty.compareTo(ZERO) != 0) {
            if (tranType.equals(TransactionType.FX_BUY) || tranType.equals(TransactionType.FX_SELL)) {
                updateCash(ptf, Currency.valueOf(symbol), qty);
            } else {
                Position position = updatePosition(ptf, country, symbol, qty);
                position.getTransactions().add(tran);
            }
        }
    }

    private void updateCash(Portfolio ptf, Currency ccy, BigDecimal moneyToAdd) {
        requireNonNull(ptf);
        requireNonNull(ccy);
        requireNonNull(moneyToAdd);
        ptf.getCash().merge(ccy, moneyToAdd, BigDecimal::add);
    }

    private Position updatePosition(Portfolio ptf, Country country, String symbol, BigDecimal qtyToAdd) {
        requireNonNull(qtyToAdd);
        return findPosition(ptf, country, symbol)
                .or(() -> {
                    Position newPosition = new Position();
                    newPosition.setCountry(country);
                    newPosition.setSymbol(symbol);
                    newPosition.setQty(ZERO);
                    ptf.getPositions().add(newPosition);
                    return Optional.of(newPosition);
                })
                .stream()
                .peek(p -> p.setQty(p.getQty().add(qtyToAdd)))
                .findAny()
                .orElseThrow();
    }

    private Optional<Position> findPosition(Portfolio ptf, Country country, String symbol) {
        requireNonNull(ptf);
        requireNonNull(country);
        requireNonNull(symbol);
        return ptf.getPositions()
                .stream()
                .filter(p -> country.equals(p.getCountry()))
                .filter(p -> symbol.equals(p.getSymbol()))
                .reduce((p1, p2) -> {
                    throw new IllegalStateException(format("Duplicate position: %s, %s", p1, p2));
                });
    }

}
