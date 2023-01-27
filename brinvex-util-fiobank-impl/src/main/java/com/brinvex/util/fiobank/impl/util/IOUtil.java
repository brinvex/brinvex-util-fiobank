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
package com.brinvex.util.fiobank.impl.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class IOUtil {

    public static String readTextFileContent(Path filePath, Charset charset, Charset... alternativeCharsets) {

        List<Charset> charsets = new ArrayList<>();
        charsets.add(charset);
        if (alternativeCharsets != null && alternativeCharsets.length > 0) {
            charsets.addAll(List.of(alternativeCharsets));
        }

        String content = null;
        List<CharacterCodingException> characterCodingExceptions = new ArrayList<>();
        try {
            for (Charset chs : charsets) {
                try {
                    content = Files.readString(filePath, chs);
                    break;
                } catch (Throwable throwable) {
                    if (throwable instanceof CharacterCodingException) {
                        characterCodingExceptions.add((CharacterCodingException) throwable);
                    } else {
                        Throwable cause = throwable.getCause();
                        if (cause instanceof CharacterCodingException) {
                            characterCodingExceptions.add((CharacterCodingException) cause);
                        } else {
                            throw throwable;
                        }
                    }
                }
            }
        } catch (IOException e) {
            for (Exception charsetException : characterCodingExceptions) {
                e.addSuppressed(charsetException);
            }
            throw new UncheckedIOException(e);
        }

        if (content == null) {
            CharacterCodingException lastCharsetException = characterCodingExceptions.remove(characterCodingExceptions.size() - 1);
            UncheckedIOException uncheckedIOException = new UncheckedIOException(lastCharsetException);
            for (Exception charsetException : characterCodingExceptions) {
                uncheckedIOException.addSuppressed(charsetException);
            }
            throw uncheckedIOException;
        }
        return content;
    }

}
