package io.nodusdb.objectstore.s3;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;

final class FaultProxy implements AutoCloseable {

    record Request(int ordinal, String method, String target) {
    }

    enum Fault {
        DROP_REQUEST, DROP_RESPONSE, TRUNCATE_RESPONSE, DELAY
    }

    private static final class Rule {

        final Predicate<Request> matcher;
        final Fault fault;
        final long delayMillis;
        int remaining;

        Rule(Predicate<Request> matcher, Fault fault, int times, long delayMillis) {
            this.matcher = matcher;
            this.fault = fault;
            this.remaining = times;
            this.delayMillis = delayMillis;
        }
    }

    private static final byte[] HEAD_END = {'\r', '\n', '\r', '\n'};
    private static final int MAX_HEAD_BYTES = 64 * 1024;

    private final ServerSocket listener;
    private final InetSocketAddress upstream;
    private final List<Rule> rules = new ArrayList<>();
    private final List<Request> requests = new ArrayList<>();
    private int ordinal;
    private volatile boolean closed;

    FaultProxy(URI upstreamEndpoint) throws IOException {
        this.upstream = new InetSocketAddress(upstreamEndpoint.getHost(), upstreamEndpoint.getPort());
        this.listener = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
        Thread acceptor = new Thread(this::acceptLoop, "fault-proxy-accept");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    URI endpoint() {
        return URI.create("http://127.0.0.1:" + listener.getLocalPort());
    }

    synchronized FaultProxy inject(Predicate<Request> matcher, Fault fault, int times) {
        rules.add(new Rule(matcher, fault, times, 0));
        return this;
    }

    synchronized FaultProxy delay(Predicate<Request> matcher, int times, long millis) {
        rules.add(new Rule(matcher, Fault.DELAY, times, millis));
        return this;
    }

    synchronized List<Request> requests() {
        return List.copyOf(requests);
    }

    @Override
    public void close() {
        closed = true;
        try {
            listener.close();
        } catch (IOException ignored) {
            return;
        }
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket client = listener.accept();
                Thread worker = new Thread(() -> serve(client), "fault-proxy-worker");
                worker.setDaemon(true);
                worker.start();
            } catch (IOException e) {
                return;
            }
        }
    }

    private void serve(Socket client) {
        try (client) {
            InputStream in = client.getInputStream();
            OutputStream out = client.getOutputStream();
            byte[] head = readHead(in);
            if (head == null) {
                return;
            }
            byte[] body = readBody(in, head);
            String[] line = new String(head, StandardCharsets.ISO_8859_1).split("\r\n", 2)[0].split(" ");
            Rule rule = choose(line[0], line[1]);
            Fault fault = rule == null ? null : rule.fault;
            if (fault == Fault.DELAY) {
                sleep(rule.delayMillis);
            }
            if (fault == Fault.DROP_REQUEST) {
                return;
            }
            byte[] response = forward(head, body);
            if (fault == Fault.DROP_RESPONSE) {
                return;
            }
            out.write(response, 0, fault == Fault.TRUNCATE_RESPONSE ? truncatedLength(response) : response.length);
            out.flush();
        } catch (IOException ignored) {
            return;
        }
    }

    private synchronized Rule choose(String method, String target) {
        Request request = new Request(++ordinal, method, target);
        requests.add(request);
        for (Rule rule : rules) {
            if (rule.remaining > 0 && rule.matcher.test(request)) {
                rule.remaining--;
                return rule;
            }
        }
        return null;
    }

    private byte[] forward(byte[] head, byte[] body) throws IOException {
        try (Socket server = new Socket()) {
            server.connect(upstream, 5_000);
            OutputStream out = server.getOutputStream();
            out.write(withConnectionClose(head));
            out.write(body);
            out.flush();
            return server.getInputStream().readAllBytes();
        }
    }

    private static byte[] withConnectionClose(byte[] head) {
        String text = new String(head, StandardCharsets.ISO_8859_1);
        StringBuilder rebuilt = new StringBuilder();
        for (String line : text.split("\r\n")) {
            if (line.isEmpty() || line.toLowerCase(Locale.ROOT).startsWith("connection:")) {
                continue;
            }
            rebuilt.append(line).append("\r\n");
        }
        rebuilt.append("Connection: close\r\n\r\n");
        return rebuilt.toString().getBytes(StandardCharsets.ISO_8859_1);
    }

    private static int truncatedLength(byte[] response) {
        int bodyStart = indexOf(response, HEAD_END) + HEAD_END.length;
        int bodyLength = response.length - bodyStart;
        return bodyStart + Math.max(0, bodyLength / 2);
    }

    private static byte[] readHead(InputStream in) throws IOException {
        ByteArrayOutputStream head = new ByteArrayOutputStream();
        int matched = 0;
        while (head.size() < MAX_HEAD_BYTES) {
            int next = in.read();
            if (next < 0) {
                return head.size() == 0 ? null : head.toByteArray();
            }
            head.write(next);
            matched = next == HEAD_END[matched] ? matched + 1 : (next == HEAD_END[0] ? 1 : 0);
            if (matched == HEAD_END.length) {
                return head.toByteArray();
            }
        }
        throw new IOException("a request head larger than " + MAX_HEAD_BYTES + " bytes");
    }

    private static byte[] readBody(InputStream in, byte[] head) throws IOException {
        String text = new String(head, StandardCharsets.ISO_8859_1).toLowerCase(Locale.ROOT);
        int length = headerValue(text, "content-length");
        if (length >= 0) {
            return in.readNBytes(length);
        }
        if (text.contains("transfer-encoding: chunked")) {
            return readChunked(in);
        }
        return new byte[0];
    }

    private static byte[] readChunked(InputStream in) throws IOException {
        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        while (true) {
            String sizeLine = readLine(in);
            raw.write((sizeLine + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
            int size = Integer.parseInt(sizeLine.split(";")[0].trim(), 16);
            raw.write(in.readNBytes(size));
            raw.write(in.readNBytes(2));
            if (size == 0) {
                return raw.toByteArray();
            }
        }
    }

    private static String readLine(InputStream in) throws IOException {
        StringBuilder line = new StringBuilder();
        while (true) {
            int next = in.read();
            if (next < 0 || next == '\n') {
                return line.toString().replace("\r", "");
            }
            line.append((char) next);
        }
    }

    private static int headerValue(String lowerCaseHead, String name) {
        for (String line : lowerCaseHead.split("\r\n")) {
            if (line.startsWith(name + ":")) {
                return Integer.parseInt(line.substring(name.length() + 1).trim());
            }
        }
        return -1;
    }

    private static int indexOf(byte[] haystack, byte[] needle) {
        for (int i = 0; i + needle.length <= haystack.length; i++) {
            boolean match = true;
            for (int j = 0; j < needle.length && match; j++) {
                match = haystack[i + j] == needle[j];
            }
            if (match) {
                return i;
            }
        }
        return 0;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
