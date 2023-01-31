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
package com.brinvex.util.fiobank.api.service;

import java.util.ServiceLoader;

/**
 * A factory for {@link FioBrokerService} and {@link FioBankService} based on Java SPI.
 */
public enum FioServiceFactory {

    INSTANCE;

    private FioBrokerService brokerService;

    private FioBankService bankService;

    public FioBrokerService getBrokerService() {
        if (brokerService == null) {
            ServiceLoader<FioBrokerService> loader = ServiceLoader.load(FioBrokerService.class);
            for (FioBrokerService provider : loader) {
                this.brokerService = provider;
                break;
            }
        }
        if (brokerService == null) {
            throw new IllegalStateException(String.format("Not found any implementation of '%s'", FioBrokerService.class));
        }
        return brokerService;
    }

    public FioBankService getBankService() {
        if (bankService == null) {
            ServiceLoader<FioBankService> loader = ServiceLoader.load(FioBankService.class);
            for (FioBankService provider : loader) {
                this.bankService = provider;
                break;
            }
        }
        if (bankService == null) {
            throw new IllegalStateException(String.format("Not found any implementation of '%s'", FioBankService.class));
        }
        return bankService;
    }
}
