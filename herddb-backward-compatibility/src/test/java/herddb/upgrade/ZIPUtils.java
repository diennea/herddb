/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package herddb.upgrade;

import herddb.utils.SimpleBufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.apache.commons.io.IOUtils;

public class ZIPUtils {

    public static void createZipWithOneEntry(String entryfilename, InputStream filedata, OutputStream out, Charset fileNamesCharset) throws IOException {
        try (ZipOutputStream zipper = new ZipOutputStream(out, fileNamesCharset);) {
            int posslash = entryfilename.indexOf('/');
            if (posslash >= 0) { // gestione caso semplice di directory singola es. META-INF/magnews-app.xml per i test di MN
                String dire = entryfilename.substring(0, posslash);
                ZipEntry entry = new ZipEntry(dire);
                zipper.putNextEntry(entry);
                zipper.closeEntry();
            }
            ZipEntry entry = new ZipEntry(entryfilename);
            zipper.putNextEntry(entry);
            IOUtils.copyLarge(filedata, zipper);
            zipper.closeEntry();
        }
    }

    public static byte[] createZipWithOneEntry(String entryfilename, byte[] filedata, Charset fileNamesCharset) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        createZipWithOneEntry(entryfilename, new ByteArrayInputStream(filedata), out, fileNamesCharset);
        return out.toByteArray();
    }

    public static byte[] createZipFromDirectory(File directory, Charset fileNamesCharset) throws IOException {
        return createZipFromDirectory(directory, fileNamesCharset, null);
    }

    public static abstract class ZipEnhancer {

        public static void addEntry(String name, byte[] content, ZipOutputStream zip) throws IOException {
            ZipEntry entry = new ZipEntry(name);
            zip.putNextEntry(entry);
            zip.write(content);
            zip.closeEntry();
        }

        public abstract void accept(ZipOutputStream out) throws IOException;
    }

    public static byte[] createZipFromDirectory(File directory, Charset fileNamesCharset,
            ZipEnhancer additionalData) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String source = directory.getAbsolutePath().replace("\\", "/");
        int skipprefix = source.length();
        try (ZipOutputStream zos = new ZipOutputStream(out);) {
            addFileToZip(skipprefix, directory, zos);
            if (additionalData != null) {
                additionalData.accept(zos);
            }
        }
        return out.toByteArray();
    }

    private static void addFileToZip(int skipprefix, File file, ZipOutputStream zipper) throws IOException {
        String raw = file.getAbsolutePath().replace("\\", "/");
        if (raw.length() == skipprefix) {
            if (file.isDirectory()) {
                File[] listFiles = file.listFiles();
                if (listFiles != null) {
                    for (File child : listFiles) {
                        addFileToZip(skipprefix, child, zipper);
                    }
                }
            }
        } else {
            String path = raw.substring(skipprefix + 1);
            if (file.isDirectory()) {
                ZipEntry entry = new ZipEntry(path);
                zipper.putNextEntry(entry);
                zipper.closeEntry();
                File[] listFiles = file.listFiles();
                if (listFiles != null) {
                    for (File child : listFiles) {
                        addFileToZip(skipprefix, child, zipper);
                    }
                }
            } else {
                ZipEntry entry = new ZipEntry(path);
                zipper.putNextEntry(entry);
                try (FileInputStream in = new FileInputStream(file)) {
                    IOUtils.copyLarge(in, zipper);
                }
                zipper.closeEntry();
            }
        }

    }

    private static final String GENERIC_VALID_FILENAME_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890._-?&/:";

    private static String normalizeFilenameForFileSystem(String filename) {
        if (filename == null) {
            return null;
        }

        filename = filename.replace(" ", "%20").replace("%20", "_");

        StringBuilder res = new StringBuilder();
        for (int i = 0; i < filename.length(); i++) {
            char c = filename.charAt(i);
            if (GENERIC_VALID_FILENAME_CHARS.indexOf(c) >= 0) {
                res.append(c);
            } else if (c == '\\') {
                res.append('/');
            } else {
                res.append("_");
            }
        }
        if (res.length() == 0) {
            return "";
        }
        return res.toString();
    }

    public static List<File> unZip(InputStream fs, File outDir) throws IOException {

        try (
                ZipInputStream zipStream = new ZipInputStream(fs, StandardCharsets.UTF_8);) {
            ZipEntry entry = zipStream.getNextEntry();
            List<File> listFiles = new ArrayList<>();
            while (entry != null) {
                if (entry.isDirectory()) {
                    entry = zipStream.getNextEntry();
                    continue;
                }

                String normalized = normalizeFilenameForFileSystem(entry.getName());
                File outFile = new File(outDir, normalized);
                File parentDir = outFile.getParentFile();
                if (parentDir != null && !parentDir.isDirectory()) {
                    Files.createDirectories(parentDir.toPath());
                }

                listFiles.add(outFile);
                try (FileOutputStream out = new FileOutputStream(outFile);
                        SimpleBufferedOutputStream oo = new SimpleBufferedOutputStream(out)) {
                    IOUtils.copyLarge(zipStream, oo);
                }
                entry = zipStream.getNextEntry();

            }
            return listFiles;
        } catch (IllegalArgumentException ex) {
            throw new IOException(ex);
        }

    }
}
