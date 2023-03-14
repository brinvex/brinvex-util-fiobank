/*
 * Copyright © 2023 Brinvex (dev@brinvex.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    private static final String TEST_DATA_FOLDER = "c:/prj/brinvex/brinvex-util/brinvex-util-fiobank/test-data";

    private final Jsonb jsonb = JsonbBuilder.create();

    @Override
    public void close() throws Exception {
        jsonb.close();
    }

    public Path getTestFilePath(Predicate<String> fileNameFilter) {
        List<Path> paths = getTestFilePaths(fileNameFilter);
        int size = paths.size();
        if (size == 0) {
            return null;
        }
        if (size > 1) {
            throw new IllegalArgumentException(String.format("Expecting one file but found %s: %s", size, paths));
        }
        return paths.get(0);
    }

    public List<Path> getTestFilePaths(Predicate<String> fileNameFilter) {
        String testDataFolder = TEST_DATA_FOLDER;

        List<Path> testStatementFilePaths;
        Path testFolderPath = Paths.get(testDataFolder);
        File testFolder = testFolderPath.toFile();
        if (!testFolder.exists() || !testFolder.isDirectory()) {
            out.printf(String.format("Test data folder not found: '%s'", testDataFolder));
        }
        try (Stream<Path> filePaths = Files.walk(testFolderPath)) {
            testStatementFilePaths = filePaths
                    .filter(p -> fileNameFilter.test(p.getFileName().toString()))
                    .filter(p -> p.toFile().isFile())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (testStatementFilePaths.isEmpty()) {
            out.printf(String.format("No files found in test data folder: '%s'", testDataFolder));
        }
        return testStatementFilePaths;
    }

    public <T> T readFromJson(Path filePath, Class<T> type) {
        try {
            return jsonb.fromJson(Files.readString(filePath), type);
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
