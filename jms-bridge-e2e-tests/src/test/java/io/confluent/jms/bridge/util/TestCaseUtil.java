package io.confluent.jms.bridge.util;

public class TestCaseUtil {
    public static boolean compareValues(int expected, int actual) {
        return expected == actual;
    }
    public static boolean compareValuesAndLog(int expected, int actual, String testcaseName) {
        return expected == actual;
    }

}