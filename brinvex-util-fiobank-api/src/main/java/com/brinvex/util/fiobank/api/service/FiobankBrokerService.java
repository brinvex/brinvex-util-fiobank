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

import com.brinvex.util.fiobank.api.model.Portfolio;
import com.brinvex.util.fiobank.api.model.RawTransactionList;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * An interface publishing methods for working with Fio Bank Broker data.
 * An implementation class instance should be retrieved using {@link FiobankServiceFactory#getBrokerService()}.
 * The factory as well as the default implementation instance is a thread-safe singleton.
 */
public interface FiobankBrokerService {

    RawTransactionList parseStatements(Stream<String> statementContents);

    RawTransactionList parseStatements(Collection<String> statementFilePaths);

    Portfolio processStatements(Stream<String> statementContents);

    Portfolio processStatements(Collection<String> statementFilePaths);

    Portfolio processStatements(Portfolio ptf, Stream<String> statementContents);

    Portfolio processStatements(Portfolio ptf, Collection<String> statementFilePaths);

}
