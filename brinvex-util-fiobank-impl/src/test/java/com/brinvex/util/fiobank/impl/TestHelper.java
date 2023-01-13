package com.brinvex.util.fiobank.impl;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.System.out;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHelper implements AutoCloseable {

    private static final String TEST_DATA_FOLDER = "c:/prj/brinvex/brinvex-util-fiobank/test-data";

    private final Jsonb jsonb = JsonbBuilder.create();

    @Override
    public void close() throws Exception {
        jsonb.close();
    }

    public String getTestFilePath(Predicate<String> fileNameFilter) {
        List<String> paths = getTestFilePaths(fileNameFilter);
        int size = paths.size();
        if (size == 0) {
            return null;
        }
        if (size > 1) {
            throw new IllegalArgumentException(String.format("Expecting one file but found %s: %s", size, paths));
        }
        return paths.get(0);
    }

    public List<String> getTestFilePaths(Predicate<String> fileNameFilter) {
        String testDataFolder = TEST_DATA_FOLDER;

        List<String> testStatementFilePaths;
        Path testFolderPath = Paths.get(testDataFolder);
        File testFolder = testFolderPath.toFile();
        if (!testFolder.exists() || !testFolder.isDirectory()) {
            out.printf(String.format("Test data folder not found: '%s'", testDataFolder));
        }
        try (Stream<Path> filePaths = Files.walk(testFolderPath)) {
            testStatementFilePaths = filePaths
                    .filter(p -> fileNameFilter.test(p.getFileName().toString()))
                    .filter(p -> p.toFile().isFile())
                    .map(Path::toString)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (testStatementFilePaths.isEmpty()) {
            out.printf(String.format("No files found in test data folder: '%s'", testDataFolder));
        }
        return testStatementFilePaths;
    }

    public <T> T readFromJson(String filePath, Class<T> type) {
        try {
            return jsonb.fromJson(Files.readString(Paths.get(filePath)), type);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public <T> void assertJsonEquals(T o1, T o2) {
        String s1 = jsonb.toJson(o1);
        String s2 = jsonb.toJson(o2);
        assertEquals(s1, s2);
    }
}
