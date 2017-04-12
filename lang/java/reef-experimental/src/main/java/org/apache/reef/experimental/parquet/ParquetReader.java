/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.experimental.parquet;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter.MetadataFilter;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A reader for Parquet files that can serialize data to local disk and return Avro schema and Avro reader.
 * The intent is not to build a general parquet reader, but to consume data with table-like property.
 */
public final class ParquetReader {
  /**
   * Standard java logger.
   */
  private static final Logger LOG = Logger.getLogger(ParquetReader.class.getName());

  private Path parquetFilePath;

  @Inject
  private ParquetReader(@Parameter(PathString.class) final String path) throws IOException {
    parquetFilePath = new Path(new File(path).getAbsolutePath());
    final Schema schema = createAvroSchema();
    if (schema.getType() != Schema.Type.RECORD) {
      LOG.log(Level.SEVERE, "ParquetReader only support Avro record type that can be consumed as a table.");
      throw new IOException("ParquetReader only support Avro record type that can be consumed as a table.");
    }
    for (final Schema.Field f : schema.getFields()) {
      if (f.schema().getType() == Schema.Type.RECORD) {
        LOG.log(Level.SEVERE, "ParquetReader doesn't support nested record type for its elements.");
        throw new IOException("ParquetReader doesn't support nested record type for its elements.");
      }
    }
  }

  /**
   * Retrieve avro schema from parquet file.
   * @return avro schema from parquet file.
   * @throws IOException if the Avro schema couldn't be parsed from the parquet file.
   */
  public Schema createAvroSchema() throws IOException {
    return createAvroSchema(new Configuration(true), NO_FILTER);
  }
  
  /**
   * Retrieve avro schema from parquet file.
   * @param configuration Hadoop configuration.
   * @param filter Filter for Avro metadata.
   * @return avro schema from parquet file.
   * @throws IOException if the Avro schema couldn't be parsed from the parquet file.
   */
  private Schema createAvroSchema(final Configuration configuration, final MetadataFilter filter) throws IOException {
    final ParquetMetadata footer = ParquetFileReader.readFooter(configuration, parquetFilePath, filter);
    final AvroSchemaConverter converter = new AvroSchemaConverter();
    final MessageType schema = footer.getFileMetaData().getSchema();
    return converter.convert(schema);
  }
  
  /**
   * Construct an avro reader from parquet file.
   * @return avro reader based on the provided parquet file.
   * @throws IOException if the parquet file couldn't be parsed correctly.
   */
  private AvroParquetReader<GenericRecord> createAvroReader() throws IOException {
    return new AvroParquetReader<GenericRecord>(parquetFilePath);
  }
  
  /**
   * Serialize Avro data to a local file.
   * @param file Local destination file for serialization.
   * @throws IOException if the parquet file couldn't be parsed correctly.
   */
  public void serializeToDisk(final File file) throws IOException {
    final DatumWriter datumWriter = new GenericDatumWriter<GenericRecord>();
    final DataFileWriter fileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    final AvroParquetReader<GenericRecord> reader = createAvroReader();
    fileWriter.create(createAvroSchema(), file);

    GenericRecord record = reader.read();
    while (record != null) {
      fileWriter.append(record);
      record = reader.read();
    }

    try {
      reader.close();
    } catch (IOException ex){
      LOG.log(Level.SEVERE, ex.getMessage());
      throw ex;
    }

    try {
      fileWriter.close();
    } catch (IOException ex){
      LOG.log(Level.SEVERE, ex.getMessage());
      throw ex;
    }
  }
  
  /**
   * Serialize Avro data to a in-memory ByteBuffer.
   * @return A ByteBuffer that contains avro data.
   * @throws IOException if the parquet file couldn't be parsed correctly.
   */
  public ByteBuffer serializeToByteBuffer() throws IOException {
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    final Encoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
    final DatumWriter writer = new GenericDatumWriter<GenericRecord>();
    writer.setSchema(createAvroSchema());
    final AvroParquetReader<GenericRecord> reader = createAvroReader();

    GenericRecord record = reader.read();
    while (record != null) {
      writer.write(record, encoder);
      record = reader.read();
    }

    try {
      reader.close();
    } catch (IOException ex){
      LOG.log(Level.SEVERE, ex.getMessage());
      throw ex;
    }

    encoder.flush();
    final ByteBuffer buf = ByteBuffer.wrap(stream.toByteArray());
    buf.order(ByteOrder.LITTLE_ENDIAN);
    return buf;
  }

  public static void main(final String[] args) throws IOException {
    final String parquetPath = args[0];
    final String avroPath = args[1];
    final ParquetReader reader = new ParquetReader(parquetPath);
    final File avroFile = new File(avroPath);
    reader.serializeToDisk(avroFile);
  }
}
