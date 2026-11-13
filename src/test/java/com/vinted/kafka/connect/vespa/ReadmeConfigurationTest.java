package com.vinted.kafka.connect.vespa;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** README.md's configuration section is generated from {@link VespaSinkConfig#CONFIG}. */
class ReadmeConfigurationTest {
    private static final Path README = Paths.get("README.md");
    private static final String UPDATE = "readme.update";

    @Test
    void documentsTheConfigDef() throws IOException {
        String readme = Files.readString(README);
        String documented = ConfigurationDocs.update(readme, VespaSinkConfig.CONFIG);

        if (Boolean.getBoolean(UPDATE)) {
            Files.writeString(README, documented);
        }

        String actual = Files.readString(README);
        assertTrue(documented.equals(actual), () -> String.format(
                "README.md's configuration section differs from VespaSinkConfig.CONFIG at %s. Edit the *_DOC strings "
                        + "and settings in VespaSinkConfig, not README.md, then regenerate the section with "
                        + "`mvn test -Dtest=ReadmeConfigurationTest -D%s=true`.",
                firstDifference(documented, actual), UPDATE));
    }

    private static String firstDifference(String expected, String actual) {
        List<String> expectedLines = expected.lines().collect(Collectors.toList());
        List<String> actualLines = actual.lines().collect(Collectors.toList());
        int line = IntStream.range(0, Math.min(expectedLines.size(), actualLines.size()))
                .filter(i -> !expectedLines.get(i).equals(actualLines.get(i)))
                .findFirst()
                .orElse(Math.min(expectedLines.size(), actualLines.size()));

        return String.format("line %d, expected \"%s\" but was \"%s\"",
                line + 1, lineOrEnd(expectedLines, line), lineOrEnd(actualLines, line));
    }

    private static String lineOrEnd(List<String> lines, int line) {
        return line < lines.size() ? lines.get(line) : "<end of file>";
    }
}
