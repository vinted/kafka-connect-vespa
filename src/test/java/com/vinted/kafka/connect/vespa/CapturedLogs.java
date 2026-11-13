package com.vinted.kafka.connect.vespa;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Captures what slf4j-simple, the logging backend of the tests, writes to standard error. */
public final class CapturedLogs implements AutoCloseable {
    private static final Pattern ENTRY = Pattern.compile("^\\[[^\\]]*\\] (TRACE|DEBUG|INFO|WARN|ERROR) ");

    private final PrintStream original = System.err;
    private final ByteArrayOutputStream captured = new ByteArrayOutputStream();

    private CapturedLogs() {
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
    }

    public static CapturedLogs capture() {
        return new CapturedLogs();
    }

    /** Each ERROR entry, with the stack trace of its exception if it has one. */
    public List<String> errors() {
        return entries().stream().filter(entry -> entry.contains("] ERROR ")).collect(Collectors.toList());
    }

    private List<String> entries() {
        List<String> entries = new ArrayList<>();
        for (String line : captured.toString(StandardCharsets.UTF_8).split("\\R")) {
            if (ENTRY.matcher(line).find() || entries.isEmpty()) {
                entries.add(line);
                continue;
            }
            entries.set(entries.size() - 1, entries.get(entries.size() - 1) + "\n" + line);
        }
        return entries;
    }

    @Override
    public void close() {
        System.setErr(original);
    }
}
