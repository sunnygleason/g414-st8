package com.g414.st8.helpers;

import java.util.Map;

import org.apache.commons.codec.binary.Hex;

public class OpaquePaginationHelper {
    public static final Long DEFAULT_PAGE_SIZE = 25L;

    public static String createOpaqueCursor(Map<String, Object> next)
            throws Exception {
        return Hex.encodeHexString(EncodingHelper.convertToSmileLzf(next));
    }

    public static Map<String, Object> decodeOpaqueCursor(String token)
            throws Exception {
        if (token == null) {
            return null;
        }

        byte[] tokenValue = Hex.decodeHex(token.toCharArray());

        return (Map<String, Object>) EncodingHelper.parseSmileLzf(tokenValue);
    }
}
