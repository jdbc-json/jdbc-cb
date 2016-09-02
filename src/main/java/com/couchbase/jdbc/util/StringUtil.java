package com.couchbase.jdbc.util;

public class StringUtil {
    public static String stripBackquotes(String s) {
        if (s == null) {
            return s;
        }
        final String BACKQUOTE = "`";

        if (s.startsWith(BACKQUOTE) && s.endsWith(BACKQUOTE)) {
            return s.substring(1, s.length()-1);
        }
        return s;
    }
}
