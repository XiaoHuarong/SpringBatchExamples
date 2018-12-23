package com.flower.springbatch.common;

import org.springframework.batch.item.file.FlatFileHeaderCallback;

import java.io.IOException;
import java.io.Writer;

/**
 */
public class StringHeaderWriter implements FlatFileHeaderCallback {

    private final String header;

    public StringHeaderWriter(String header) {
        this.header = header;
    }

    @Override
    public void writeHeader(Writer writer) throws IOException {

        writer.write(header);
    }
}
