package com.couchbase.utils;

public class Utils {

    /**
     * Handles creating the string with the provided precision. If the length of the string is longer than the
     * precision, return only up to the precision and get rid of the rest, if the length of the string is smaller than
     * the precision, fill in the rest of the length with trailing white spaces.
     *
     * @param value     The actual string value
     * @param precision The expected string precision
     * @return The adjust string with the precision applied
     */
    public static String createStringWithPadding(String value, int precision) {

        // If the string is longer than the precision, cut the rest and take the precision only
        if (value.length() > precision) {
            return value.substring(0, precision);
        }

        // If the string is shorter than the precision, add trailing white spaces
        if (value.length() < precision) {
            return String.format("%-" + precision + "s", value);
        }

        return value;
    }
}
