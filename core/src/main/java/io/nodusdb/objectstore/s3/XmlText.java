package io.nodusdb.objectstore.s3;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class XmlText {

    private XmlText() {
    }

    static Optional<String> text(String xml, String tag) {
        return blocks(xml, tag).stream().findFirst().map(XmlText::unescape);
    }

    static List<String> blocks(String xml, String tag) {
        List<String> blocks = new ArrayList<>();
        String close = "</" + tag + ">";
        int from = 0;
        while (true) {
            int open = indexOfOpen(xml, tag, from);
            if (open < 0) {
                return blocks;
            }
            int openEnd = xml.indexOf('>', open);
            if (openEnd < 0) {
                return blocks;
            }
            if (xml.charAt(openEnd - 1) == '/') {
                blocks.add("");
                from = openEnd + 1;
                continue;
            }
            int end = xml.indexOf(close, openEnd + 1);
            if (end < 0) {
                return blocks;
            }
            blocks.add(xml.substring(openEnd + 1, end));
            from = end + close.length();
        }
    }

    static String escape(String text) {
        StringBuilder out = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '&' -> out.append("&amp;");
                case '<' -> out.append("&lt;");
                case '>' -> out.append("&gt;");
                default -> out.append(c);
            }
        }
        return out.toString();
    }

    static String unescape(String text) {
        if (text.indexOf('&') < 0) {
            return text;
        }
        StringBuilder out = new StringBuilder(text.length());
        int i = 0;
        while (i < text.length()) {
            char c = text.charAt(i);
            int semicolon = c == '&' ? text.indexOf(';', i) : -1;
            if (semicolon < 0) {
                out.append(c);
                i++;
                continue;
            }
            String entity = text.substring(i + 1, semicolon);
            String replacement = decode(entity);
            if (replacement == null) {
                out.append(c);
                i++;
            } else {
                out.append(replacement);
                i = semicolon + 1;
            }
        }
        return out.toString();
    }

    private static int indexOfOpen(String xml, String tag, int from) {
        String open = "<" + tag;
        int index = from;
        while (true) {
            index = xml.indexOf(open, index);
            if (index < 0) {
                return -1;
            }
            int after = index + open.length();
            if (after < xml.length()) {
                char next = xml.charAt(after);
                if (next == '>' || next == '/' || next == ' ' || next == '\n' || next == '\r' || next == '\t') {
                    return index;
                }
            }
            index = after;
        }
    }

    private static String decode(String entity) {
        switch (entity) {
            case "amp":
                return "&";
            case "lt":
                return "<";
            case "gt":
                return ">";
            case "quot":
                return "\"";
            case "apos":
                return "'";
            default:
                return decodeNumeric(entity);
        }
    }

    private static String decodeNumeric(String entity) {
        if (entity.length() < 2 || entity.charAt(0) != '#') {
            return null;
        }
        try {
            int codePoint = entity.charAt(1) == 'x' || entity.charAt(1) == 'X'
                    ? Integer.parseInt(entity.substring(2), 16)
                    : Integer.parseInt(entity.substring(1));
            return Character.isValidCodePoint(codePoint) ? new String(Character.toChars(codePoint)) : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
