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
