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

import java.math.BigDecimal;
import java.util.List;
import java.util.function.Predicate;

import static java.math.BigDecimal.ZERO;
import static java.util.Objects.requireNonNullElse;

@SuppressWarnings({"UnnecessaryLocalVariable", "DuplicatedCode"})
public enum TransactionType {

    BUY {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue().compareTo(ZERO) < 0,
                    t -> t.getCurrency() != null,
                    t -> t.getSymbol() != null,
                    t -> t.getQty().compareTo(ZERO) > 0,
                    t -> t.getPrice().compareTo(ZERO) > 0,
                    t -> t.getIncome().compareTo(ZERO) == 0,
                    t -> t.getFees().compareTo(ZERO) <= 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) == 0
            );
        }
    },
    SELL {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue().compareTo(ZERO) > 0,
                    t -> t.getCurrency() != null,
                    t -> t.getSymbol() != null,
                    t -> t.getQty().compareTo(ZERO) < 0,
                    t -> t.getPrice().compareTo(ZERO) > 0,
                    t -> t.getIncome().compareTo(ZERO) == 0,
                    t -> t.getFees().compareTo(ZERO) <= 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) <= 0
            );
        }
    },
    CASH_TOP_UP {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue().compareTo(ZERO) > 0,
                    t -> t.getCurrency() != null,
                    t -> t.getSymbol() == null,
                    t -> t.getQty().compareTo(ZERO) == 0,
                    t -> t.getPrice() == null,
                    t -> t.getIncome().compareTo(ZERO) > 0,
                    t -> t.getFees().compareTo(ZERO) <= 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) == 0
            );
        }
    },
    CASH_WITHDRAWAL {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue().compareTo(ZERO) < 0,
                    t -> t.getCurrency() != null,
                    t -> t.getSymbol() == null,
                    t -> t.getQty().compareTo(ZERO) == 0,
                    t -> t.getPrice() == null,
                    t -> t.getIncome().compareTo(ZERO) < 0,
                    t -> t.getFees().compareTo(ZERO) <= 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) == 0
            );
        }
    },
    CASH_DIVIDEND {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue().compareTo(ZERO) > 0,
                    t -> t.getCurrency() != null,
                    t -> t.getSymbol() != null,
                    t -> t.getQty().compareTo(ZERO) == 0,
                    t -> t.getPrice() == null,
                    t -> t.getIncome().compareTo(ZERO) > 0,
                    t -> t.getFees().compareTo(ZERO) <= 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) <= 0
            );
        }
    },
    CAPITAL_DIVIDEND {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue().compareTo(ZERO) > 0,
                    t -> t.getCurrency() != null,
                    t -> t.getSymbol() != null,
                    t -> t.getQty().compareTo(ZERO) == 0,
                    t -> t.getPrice() == null,
                    t -> t.getIncome().compareTo(ZERO) > 0,
                    t -> t.getFees().compareTo(ZERO) <= 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) == 0
            );
        }
    },
    STOCK_DIVIDEND {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> requireNonNullElse(t.getNetValue(), ZERO).compareTo(ZERO) >= 0,
                    t -> t.getSymbol() != null,
                    t -> t.getQty().compareTo(ZERO) >= 0,
                    t -> t.getPrice() == null,
                    t -> t.getIncome().compareTo(ZERO) >= 0,
                    t -> t.getFees().compareTo(ZERO) <= 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) == 0
            );
        }
    },
    DIVIDEND_REVERSAL {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue().compareTo(ZERO) < 0,
                    t -> t.getCurrency() != null,
                    t -> t.getSymbol() != null,
                    t -> t.getQty().compareTo(ZERO) == 0,
                    t -> t.getPrice() == null,
                    t -> t.getIncome().compareTo(ZERO) < 0,
                    t -> t.getFees().compareTo(ZERO) >= 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) >= 0
            );
        }
    },
    FX_BUY {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue().compareTo(ZERO) < 0,
                    t -> t.getCurrency() != null,
                    t -> t.getSymbol() != null,
                    t -> t.getQty().compareTo(ZERO) > 0,
                    t -> t.getPrice().compareTo(ZERO) > 0,
                    t -> t.getIncome().compareTo(ZERO) == 0,
                    t -> t.getFees().compareTo(ZERO) <= 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) == 0
            );
        }
    },
    FX_SELL {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue().compareTo(ZERO) > 0,
                    t -> t.getCurrency() != null,
                    t -> t.getSymbol() != null,
                    t -> t.getQty().compareTo(ZERO) < 0,
                    t -> t.getPrice().compareTo(ZERO) > 0,
                    t -> t.getIncome().compareTo(ZERO) == 0,
                    t -> t.getFees().compareTo(ZERO) <= 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) == 0
            );
        }
    },
    FEE {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue().compareTo(ZERO) < 0,
                    t -> t.getCurrency() != null,
                    t -> t.getQty().compareTo(ZERO) == 0,
                    t -> t.getPrice() == null,
                    t -> t.getIncome().compareTo(ZERO) == 0,
                    t -> t.getFees().compareTo(ZERO) < 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) == 0
            );
        }
    },
    TAX {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue().compareTo(ZERO) < 0,
                    t -> t.getCurrency() != null,
                    t -> t.getSymbol() != null,
                    t -> t.getQty().compareTo(ZERO) == 0,
                    t -> t.getPrice() == null,
                    t -> t.getIncome().compareTo(ZERO) == 0,
                    t -> t.getFees().compareTo(ZERO) == 0,
                    t -> t.getTax().compareTo(ZERO) < 0
            );
        }
    },
    TAX_REFUND {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue().compareTo(ZERO) > 0,
                    t -> t.getCurrency() != null,
                    t -> t.getSymbol() != null,
                    t -> t.getQty().compareTo(ZERO) == 0,
                    t -> t.getPrice() == null,
                    t -> t.getIncome().compareTo(ZERO) == 0,
                    t -> t.getFees().compareTo(ZERO) == 0,
                    t -> t.getTax().compareTo(ZERO) > 0
            );
        }
    },
    RECLAMATION {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue().compareTo(ZERO) > 0,
                    t -> t.getCurrency() != null,
                    t -> t.getSymbol() == null,
                    t -> t.getQty().compareTo(ZERO) == 0,
                    t -> t.getPrice() == null,
                    t -> t.getIncome().compareTo(ZERO) > 0,
                    t -> t.getFees().compareTo(ZERO) == 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) == 0
            );
        }
    },

    INSTRUMENT_CHANGE_PARENT {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue().compareTo(ZERO) == 0,
                    t -> t.getCurrency() == null,
                    t -> t.getSymbol() != null,
                    t -> t.getQty().compareTo(ZERO) <= 0,
                    t -> t.getPrice() == null,
                    t -> t.getIncome().compareTo(ZERO) == 0,
                    t -> t.getFees().compareTo(ZERO) == 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) == 0
            );
        }
    },

    INSTRUMENT_CHANGE_CHILD {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue().compareTo(ZERO) == 0,
                    t -> t.getCurrency() == null,
                    t -> t.getSymbol() != null,
                    t -> t.getQty().compareTo(ZERO) >= 0,
                    t -> t.getPrice() == null,
                    t -> t.getIncome().compareTo(ZERO) == 0,
                    t -> t.getFees().compareTo(ZERO) == 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) == 0
            );
        }
    },

    SPINOFF_PARENT {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue() == null,
                    t -> t.getCurrency() != null,
                    t -> t.getSymbol() != null,
                    t -> t.getQty().compareTo(ZERO) == 0,
                    t -> t.getPrice() == null,
                    t -> t.getIncome().compareTo(ZERO) == 0,
                    t -> t.getFees().compareTo(ZERO) == 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) == 0
            );
        }
    },

    SPINOFF_CHILD {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue() == null,
                    t -> t.getCurrency() != null,
                    t -> t.getSymbol() != null,
                    t -> t.getQty().compareTo(ZERO) > 0,
                    t -> t.getPrice() == null,
                    t -> t.getIncome().compareTo(ZERO) == 0,
                    t -> t.getFees().compareTo(ZERO) == 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) == 0
            );
        }
    },

    SPINOFF_VALUE {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue().compareTo(ZERO) != 0,
                    t -> t.getCurrency() != null,
                    t -> t.getSymbol() != null,
                    t -> t.getQty().compareTo(ZERO) == 0,
                    t -> t.getPrice() == null,
                    t -> t.getIncome().compareTo(ZERO) != 0,
                    t -> t.getFees().compareTo(ZERO) == 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) == 0
            );
        }
    },

    SPLIT {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue().compareTo(ZERO) == 0,
                    t -> t.getCurrency() == null,
                    t -> t.getSymbol() != null,
                    t -> t.getQty().compareTo(ZERO) != 0,
                    t -> t.getPrice() == null,
                    t -> t.getIncome().compareTo(ZERO) == 0,
                    t -> t.getFees().compareTo(ZERO) == 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) == 0
            );
        }
    },
    LIQUIDATION {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue().compareTo(ZERO) >= 0,
                    t -> t.getSymbol() != null,
                    t -> t.getQty().compareTo(ZERO) < 0,
                    t -> t.getPrice().compareTo(ZERO) == 0,
                    t -> t.getIncome().compareTo(ZERO) >= 0,
                    t -> t.getFees().compareTo(ZERO) == 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) == 0
            );
        }
    },

    MERGER_PARENT {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue() == null,
                    t -> t.getCurrency() != null,
                    t -> t.getSymbol() != null,
                    t -> t.getQty().compareTo(ZERO) < 0,
                    t -> t.getPrice() == null,
                    t -> t.getIncome().compareTo(ZERO) == 0,
                    t -> t.getFees().compareTo(ZERO) == 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) == 0
            );
        }
    },

    MERGER_CHILD {
        @Override
        protected List<Predicate<Transaction>> predicates() {
            return List.of(
                    t -> t.getNetValue() == null,
                    t -> t.getCurrency() != null,
                    t -> t.getSymbol() != null,
                    t -> t.getQty().compareTo(ZERO) > 0,
                    t -> t.getPrice() == null,
                    t -> t.getIncome().compareTo(ZERO) == 0,
                    t -> t.getFees().compareTo(ZERO) == 0,
                    t -> requireNonNullElse(t.getTax(), ZERO).compareTo(ZERO) == 0
            );
        }
    };

    private static final BigDecimal NUMBER_DIFF_TOLERANCE = new BigDecimal("0.005");

    protected abstract List<Predicate<Transaction>> predicates();

    private List<Predicate<Transaction>> predicates;

    @SuppressWarnings({"RedundantIfStatement", "ForLoopReplaceableByForEach"})
    public boolean isValid(Transaction t) {
        if (t.getDate() == null) {
            return false;
        }
        if (t.getSettlementDate() == null) {
            return false;
        }
        if (t.getType() == null) {
            return false;
        }
        if (!t.getType().equals(FX_BUY) && !t.getType().equals(FX_SELL)) {
            if (t.getSymbol() != null && t.getCountry() == null) {
                return false;
            }
            if (t.getSymbol() == null && t.getCountry() != null) {
                return false;
            }
        }
        if (this.predicates == null) {
            this.predicates = predicates();
        }
        for (int i = 0, predicatesSize = this.predicates.size(); i < predicatesSize; i++) {
            Predicate<Transaction> predicate = this.predicates.get(i);
            if (!predicate.test(t)) {
                return false;
            }
        }
        if (!grossValueIsValid(t)) {
            return false;
        }
        if (!netValueIsValid(t)) {
            return false;
        }
        return true;
    }

    protected boolean grossValueIsValid(Transaction t) {
        BigDecimal grossValue = t.getGrossValue();
        if (grossValue == null) {
            return t.getNetValue() == null;
        }
        BigDecimal qty = t.getQty();
        BigDecimal price = t.getPrice();
        BigDecimal income = t.getIncome();
        BigDecimal compGrossValue;
        if (price == null) {
            compGrossValue = income;
        } else {
            compGrossValue = qty.multiply(price).negate().add(income);
        }
        boolean isValid = compGrossValue.subtract(grossValue).abs().compareTo(NUMBER_DIFF_TOLERANCE) < 0;
        return isValid;
    }

    protected boolean netValueIsValid(Transaction t) {
        BigDecimal netValue = t.getNetValue();
        if (netValue == null) {
            return t.getGrossValue() == null;
        }
        BigDecimal qty = t.getQty();
        BigDecimal price = t.getPrice();
        BigDecimal income = t.getIncome();
        BigDecimal fees = t.getFees();
        BigDecimal tax = requireNonNullElse(t.getTax(), ZERO);
        BigDecimal compNetValue;
        if (price == null) {
            compNetValue = income.add(fees).add(tax);
        } else {
            compNetValue = qty.multiply(price).negate().add(income).add(fees).add(tax);
        }
        boolean isValid = compNetValue.subtract(netValue).abs().compareTo(NUMBER_DIFF_TOLERANCE) < 0;
        return isValid;
    }

}
