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
package com.brinvex.util.fiobank.impl.util;

import com.brinvex.util.fiobank.api.service.exception.FiobankServiceException;

import java.math.BigDecimal;
import java.util.function.Supplier;

import static java.math.BigDecimal.ZERO;

public class ValidationUtil {

    public static void assertTrue(boolean test) {
        if (!test) {
            throw new FiobankServiceException("");
        }
    }

    public static void assertTrue(boolean test, Supplier<String> msgSupplier) {
        if (!test) {
            throw new FiobankServiceException(msgSupplier.get());
        }
    }

    public static void assertTrue(boolean test, Supplier<String> msgSupplier, Object... msgArgs) {
        if (!test) {
            throw new FiobankServiceException(String.format(msgSupplier.get(), msgArgs));
        }
    }

    public static void assertNull(Object o) {
        assertTrue(o == null, () -> "Expected null but got: %s", o);
    }

    public static void assertNotNull(Object o) {
        assertTrue(o != null, () -> "Expected non-null");
    }

    public static void assertIsZero(BigDecimal number) {
        assertTrue(number.compareTo(ZERO) == 0, () -> "Expected zero but got: %s", number);
    }

    public static void assertIsPositive(BigDecimal number) {
        assertTrue(number.compareTo(ZERO) > 0, () -> "Expected positive number but got: %s", number);
    }

    public static void assertIsNegative(BigDecimal number) {
        assertTrue(number.compareTo(ZERO) < 0, () -> "Expected negative number but got: %s", number);
    }

    public static void assertIsZeroOrNegative(BigDecimal number) {
        assertTrue(number.compareTo(ZERO) <= 0, () -> "Expected zero or negative number but got: %s", number);
    }

    public static void assertIsZeroOrPositive(BigDecimal number) {
        assertTrue(number.compareTo(ZERO) >= 0, () -> "Expected zero or positive number but got: %s", number);
    }

    public static void assertEqual(BigDecimal number1, BigDecimal number2) {
        assertTrue(number1.compareTo(number2) == 0, () -> "Expected equal numbers but got: %s, %s", number1, number2);
    }

    public static <E extends Enum<E>> void assertEqual(E enum1, E enum2) {
        assertTrue(enum1 == enum2, () -> "Expected equal enums but got: %s, %s", enum1, enum2);
    }


}
