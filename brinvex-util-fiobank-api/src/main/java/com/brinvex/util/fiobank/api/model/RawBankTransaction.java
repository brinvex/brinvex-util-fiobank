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
import java.util.Map;

public class RawBankTransaction implements Serializable {

    private String id;

    private LocalDate date;

    private BigDecimal volume;

    private Currency ccy;

    private String type;

    private Map<String, String> additionals;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public LocalDate getDate() {
        return date;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }

    public BigDecimal getVolume() {
        return volume;
    }

    public void setVolume(BigDecimal volume) {
        this.volume = volume;
    }

    public Currency getCcy() {
        return ccy;
    }

    public void setCcy(Currency ccy) {
        this.ccy = ccy;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, String> getAdditionals() {
        return additionals;
    }

    public void setAdditionals(Map<String, String> additionals) {
        this.additionals = additionals;
    }

    @Override
    public String toString() {
        return "RawBankTransaction{" +
               "id='" + id + '\'' +
               ", date=" + date +
               ", volume=" + volume +
               ", ccy=" + ccy +
               ", type='" + type + '\'' +
               ", additionals=" + additionals +
               '}';
    }
}
