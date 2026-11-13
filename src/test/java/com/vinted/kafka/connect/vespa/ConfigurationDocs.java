package com.vinted.kafka.connect.vespa;

import org.apache.kafka.common.config.ConfigDef;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/** Renders a ConfigDef as the README's Markdown configuration section, grouped by group. */
final class ConfigurationDocs {
    static final String START = "<!-- config:start -->";
    static final String END = "<!-- config:end -->";
    private static final int WIDTH = 120;

    private ConfigurationDocs() {
    }

    /** Returns {@code readme} with the text between the markers replaced by the documentation of {@code definition}. */
    static String update(String readme, ConfigDef definition) {
        int start = readme.indexOf(START);
        int end = readme.indexOf(END);

        if (start < 0 || end < start) {
            throw new IllegalArgumentException(String.format("README.md must contain '%s' followed by '%s'.", START, END));
        }

        return readme.substring(0, start + START.length()) + "\n\n" + markdown(definition) + readme.substring(end);
    }

    static String markdown(ConfigDef definition) {
        Values values = new Values(definition);
        return definition.groups().stream()
                .map(group -> group(definition, values, group))
                .collect(Collectors.joining());
    }

    private static String group(ConfigDef definition, Values values, String group) {
        return definition.configKeys().values().stream()
                .filter(key -> group.equals(key.group))
                .sorted(Comparator.comparingInt(key -> key.orderInGroup))
                .map(key -> key(values, key))
                .collect(Collectors.joining("", "#### " + group + "\n\n", ""));
    }

    private static String key(Values values, ConfigDef.ConfigKey key) {
        StringBuilder markdown = new StringBuilder()
                .append('`').append(key.name).append("`\n\n")
                .append(wrap(key.documentation)).append("\n\n")
                .append(bullet(values, key, "Type"))
                .append(bullet(values, key, "Default"));

        // As Kafka's own ConfigDef.toRst, which lists valid values only for a key with a validator.
        if (key.validator != null) {
            markdown.append(bullet(values, key, "Valid Values"));
        }

        return markdown.append(bullet(values, key, "Importance")).append('\n').toString();
    }

    private static String bullet(Values values, ConfigDef.ConfigKey key, String header) {
        // connect-utils renders its validators as reStructuredText literals.
        String value = values.of(key, header).replace("``", "`");
        return ("* " + header + ": " + value).stripTrailing() + "\n";
    }

    private static String wrap(String text) {
        List<String> lines = new ArrayList<>();
        StringBuilder line = new StringBuilder();

        for (String word : words(text)) {
            if (line.length() > 0 && line.length() + 1 + word.length() > WIDTH) {
                lines.add(line.toString());
                line.setLength(0);
            }
            line.append(line.length() > 0 ? " " : "").append(word);
        }

        lines.add(line.toString());
        return String.join("\n", lines);
    }

    /** Splits on whitespace, keeping each code span whole so that wrapping never breaks it. */
    private static List<String> words(String text) {
        List<String> words = new ArrayList<>();
        StringBuilder word = new StringBuilder();

        for (String part : text.trim().split("\\s+")) {
            word.append(word.length() > 0 ? " " : "").append(part);
            if (word.chars().filter(c -> c == '`').count() % 2 == 0) {
                words.add(word.toString());
                word.setLength(0);
            }
        }

        if (word.length() > 0) {
            words.add(word.toString());
        }

        return words;
    }

    /** Exposes Kafka's own rendering of a key's type, default, valid values and importance. */
    private static final class Values extends ConfigDef {
        Values(ConfigDef definition) {
            super(definition);
        }

        String of(ConfigKey key, String header) {
            return getConfigValue(key, header);
        }
    }
}
