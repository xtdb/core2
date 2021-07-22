/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package core2.json;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter.NopIndenter;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.BufferLayout.BufferType;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.arrow.vector.util.DictionaryUtility;
import org.apache.commons.codec.binary.Hex;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

import static org.apache.arrow.vector.BufferLayout.BufferType.*;

/**
 * A writer that converts binary Vectors into a JSON format suitable
 * for integration testing.
 */
@SuppressWarnings("deprecation")
public class JsonFileWriter implements AutoCloseable {

    /**
     * Configuration POJO for writing JSON files.
     */
    public static final class JSONWriteConfig {
        private final boolean pretty;

        private JSONWriteConfig(boolean pretty) {
            this.pretty = pretty;
        }

        private JSONWriteConfig() {
            this.pretty = false;
        }

        public JSONWriteConfig pretty(boolean pretty) {
            return new JSONWriteConfig(pretty);
        }
    }

    public static JSONWriteConfig config() {
        return new JSONWriteConfig();
    }

    private final JsonGenerator generator;
    private Schema schema;

    /**
     * Constructs a new writer that will output to  <code>outputFile</code>.
     */
    public JsonFileWriter(File outputFile) throws IOException {
        this(outputFile, config());
    }

    /**
     * Constructs a new writer that will output to  <code>outputFile</code> with the given options.
     */
    public JsonFileWriter(File outputFile, JSONWriteConfig config) throws IOException {
        MappingJsonFactory jsonFactory = new MappingJsonFactory();
        this.generator = jsonFactory.createGenerator(outputFile, JsonEncoding.UTF8);
        if (config.pretty) {
            DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
            prettyPrinter.indentArraysWith(NopIndenter.instance);
            this.generator.setPrettyPrinter(prettyPrinter);
        }
        // Allow writing of floating point NaN values not as strings
        this.generator.configure(JsonGenerator.Feature.QUOTE_NON_NUMERIC_NUMBERS, false);
    }

    /**
     * Writes out the "header" of the file including the schema and any dictionaries required.
     */
    public void start(Schema schema, DictionaryProvider provider) throws IOException {
        List<Field> fields = new ArrayList<>(schema.getFields().size());
        Set<Long> dictionaryIdsUsed = new HashSet<>();
        this.schema = schema; // Store original Schema to ensure batches written match

        // Convert fields with dictionaries to have dictionary type
        for (Field field : schema.getFields()) {
            fields.add(DictionaryUtility.toMessageFormat(field, provider, dictionaryIdsUsed));
        }
        Schema updatedSchema = new Schema(fields, schema.getCustomMetadata());

        generator.writeStartObject();
        generator.writeObjectField("schema", updatedSchema);

        // Write all dictionaries that were used
        if (!dictionaryIdsUsed.isEmpty()) {
            writeDictionaryBatches(generator, dictionaryIdsUsed, provider);
        }

        // Start writing of record batches
        generator.writeArrayFieldStart("batches");
    }

    private void writeDictionaryBatches(JsonGenerator generator, Set<Long> dictionaryIdsUsed, DictionaryProvider provider)
            throws IOException {
        generator.writeArrayFieldStart("dictionaries");
        for (Long id : dictionaryIdsUsed) {
            generator.writeStartObject();
            generator.writeObjectField("id", id);

            generator.writeFieldName("data");
            Dictionary dictionary = provider.lookup(id);
            FieldVector vector = dictionary.getVector();
            List<Field> fields = Collections.singletonList(vector.getField());
            List<FieldVector> vectors = Collections.singletonList(vector);
            VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, vector.getValueCount());
            writeBatch(root);

            generator.writeEndObject();
        }
        generator.writeEndArray();
    }

    /**
     * Writes the record batch to the JSON file.
     */
    public void write(VectorSchemaRoot recordBatch) throws IOException {
        if (!recordBatch.getSchema().equals(schema)) {
            throw new IllegalArgumentException("record batches must have the same schema: " + schema);
        }
        writeBatch(recordBatch);
    }

    private void writeBatch(VectorSchemaRoot recordBatch) throws IOException {
        generator.writeStartObject();
        {
            generator.writeObjectField("count", recordBatch.getRowCount());
            generator.writeArrayFieldStart("columns");
            for (Field field : recordBatch.getSchema().getFields()) {
                FieldVector vector = recordBatch.getVector(field);
                writeFromVectorIntoJson(field, vector);
            }
            generator.writeEndArray();
        }
        generator.writeEndObject();
    }

    private void writeFromVectorIntoJson(Field field, FieldVector vector) throws IOException {
        List<BufferType> vectorTypes = TypeLayout.getTypeLayout(field.getType()).getBufferTypes();
        List<ArrowBuf> vectorBuffers = vector.getFieldBuffers();
        if (vectorTypes.size() != vectorBuffers.size()) {
            throw new IllegalArgumentException("vector types and inner vector buffers are not the same size: " +
                    vectorTypes.size() + " != " + vectorBuffers.size());
        }
        generator.writeStartObject();
        {
            generator.writeObjectField("name", field.getName());
            int valueCount = vector.getValueCount();
            generator.writeObjectField("count", valueCount);

            for (int v = 0; v < vectorTypes.size(); v++) {
                BufferType bufferType = vectorTypes.get(v);
                ArrowBuf vectorBuffer = vectorBuffers.get(v);
                generator.writeArrayFieldStart(bufferType.getName());
                final int bufferValueCount = (bufferType.equals(OFFSET) && vector.getMinorType() != MinorType.DENSEUNION) ?
                        valueCount + 1 : valueCount;
                for (int i = 0; i < bufferValueCount; i++) {
                    if (bufferType.equals(DATA) && (vector.getMinorType() == MinorType.VARCHAR ||
                            vector.getMinorType() == MinorType.VARBINARY)) {
                        writeValueToGenerator(bufferType, vectorBuffer, vectorBuffers.get(v - 1), vector, i);
                    } else if (bufferType.equals(OFFSET) && vector.getValueCount() == 0 &&
                            (vector.getMinorType() == MinorType.VARBINARY || vector.getMinorType() == MinorType.VARCHAR)) {
                        ArrowBuf vectorBufferTmp = vector.getAllocator().buffer(4);
                        vectorBufferTmp.setInt(0, 0);
                        writeValueToGenerator(bufferType, vectorBufferTmp, null, vector, i);
                        vectorBufferTmp.release();
                    } else {
                        writeValueToGenerator(bufferType, vectorBuffer, null, vector, i);
                    }
                }
                generator.writeEndArray();
            }
            List<Field> fields = field.getChildren();
            List<FieldVector> children = vector.getChildrenFromFields();
            if (fields.size() != children.size()) {
                throw new IllegalArgumentException("fields and children are not the same size: " + fields.size() + " != " +
                        children.size());
            }
            if (fields.size() > 0) {
                generator.writeArrayFieldStart("children");
                for (int i = 0; i < fields.size(); i++) {
                    Field childField = fields.get(i);
                    FieldVector childVector = children.get(i);
                    writeFromVectorIntoJson(childField, childVector);
                }
                generator.writeEndArray();
            }
        }
        generator.writeEndObject();
    }

    private void writeValueToGenerator(
            BufferType bufferType,
            ArrowBuf buffer,
            ArrowBuf offsetBuffer,
            FieldVector vector,
            final int index) throws IOException {
        if (bufferType.equals(TYPE)) {
            generator.writeNumber(buffer.getByte(index * TinyIntVector.TYPE_WIDTH));
        } else if (bufferType.equals(OFFSET)) {
            generator.writeNumber(buffer.getInt(index * BaseVariableWidthVector.OFFSET_WIDTH));
        } else if (bufferType.equals(VALIDITY)) {
            generator.writeNumber(vector.isNull(index) ? 0 : 1);
        } else if (bufferType.equals(DATA)) {
            switch (vector.getMinorType()) {
                case TINYINT:
                    generator.writeNumber(TinyIntVector.get(buffer, index));
                    break;
                case SMALLINT:
                    generator.writeNumber(SmallIntVector.get(buffer, index));
                    break;
                case INT:
                    generator.writeNumber(IntVector.get(buffer, index));
                    break;
                case BIGINT:
                    generator.writeString(String.valueOf(BigIntVector.get(buffer, index)));
                    break;
                case UINT1:
                    generator.writeNumber(UInt1Vector.getNoOverflow(buffer, index));
                    break;
                case UINT2:
                    generator.writeNumber(UInt2Vector.get(buffer, index));
                    break;
                case UINT4:
                    generator.writeNumber(UInt4Vector.getNoOverflow(buffer, index));
                    break;
                case UINT8:
                    generator.writeString(UInt8Vector.getNoOverflow(buffer, index).toString());
                    break;
                case FLOAT4:
                    generator.writeNumber(Float4Vector.get(buffer, index));
                    break;
                case FLOAT8:
                    generator.writeNumber(Float8Vector.get(buffer, index));
                    break;
                case DATEDAY:
                    generator.writeNumber(DateDayVector.get(buffer, index));
                    break;
                case DATEMILLI:
                    generator.writeNumber(DateMilliVector.get(buffer, index));
                    break;
                case TIMESEC:
                    generator.writeNumber(TimeSecVector.get(buffer, index));
                    break;
                case TIMEMILLI:
                    generator.writeNumber(TimeMilliVector.get(buffer, index));
                    break;
                case TIMEMICRO:
                    generator.writeNumber(TimeMicroVector.get(buffer, index));
                    break;
                case TIMENANO:
                    generator.writeNumber(TimeNanoVector.get(buffer, index));
                    break;
                case TIMESTAMPSEC:
                    generator.writeNumber(TimeStampSecVector.get(buffer, index));
                    break;
                case TIMESTAMPMILLI:
                    generator.writeNumber(TimeStampMilliVector.get(buffer, index));
                    break;
                case TIMESTAMPMICRO:
                    generator.writeNumber(TimeStampMicroVector.get(buffer, index));
                    break;
                case TIMESTAMPNANO:
                    generator.writeNumber(TimeStampNanoVector.get(buffer, index));
                    break;
                case TIMESTAMPSECTZ:
                    generator.writeNumber(TimeStampSecTZVector.get(buffer, index));
                    break;
                case TIMESTAMPMILLITZ:
                    generator.writeNumber(TimeStampMilliTZVector.get(buffer, index));
                    break;
                case TIMESTAMPMICROTZ:
                    generator.writeNumber(TimeStampMicroTZVector.get(buffer, index));
                    break;
                case TIMESTAMPNANOTZ:
                    generator.writeNumber(TimeStampNanoTZVector.get(buffer, index));
                    break;
                case DURATION:
                    generator.writeNumber(DurationVector.get(buffer, index));
                    break;
                case INTERVALYEAR:
                    generator.writeNumber(IntervalYearVector.getTotalMonths(buffer, index));
                    break;
                case INTERVALDAY:
                    generator.writeStartObject();
                    generator.writeObjectField("days", IntervalDayVector.getDays(buffer, index));
                    generator.writeObjectField("milliseconds", IntervalDayVector.getMilliseconds(buffer, index));
                    generator.writeEndObject();
                    break;
                case BIT:
                    generator.writeNumber(BitVectorHelper.get(buffer, index));
                    break;
                case VARBINARY: {
                    Preconditions.checkNotNull(offsetBuffer);
                    String hexString = Hex.encodeHexString(BaseVariableWidthVector.get(buffer,
                            offsetBuffer, index));
                    generator.writeObject(hexString);
                    break;
                }
                case FIXEDSIZEBINARY:
                    int byteWidth = ((FixedSizeBinaryVector) vector).getByteWidth();
                    String fixedSizeHexString = Hex.encodeHexString(FixedSizeBinaryVector.get(buffer, index, byteWidth));
                    generator.writeObject(fixedSizeHexString);
                    break;
                case VARCHAR: {
                    Preconditions.checkNotNull(offsetBuffer);
                    byte[] b = (BaseVariableWidthVector.get(buffer, offsetBuffer, index));
                    generator.writeString(new String(b, "UTF-8"));
                    break;
                }
                case DECIMAL: {
                    int scale = ((DecimalVector) vector).getScale();
                    BigDecimal decimalValue = DecimalUtility.getBigDecimalFromArrowBuf(buffer, index, scale,
                            DecimalVector.TYPE_WIDTH);
                    // We write the unscaled value, because the scale is stored in the type metadata.
                    generator.writeString(decimalValue.unscaledValue().toString());
                    break;
                }
                case DECIMAL256: {
                    int scale = ((Decimal256Vector) vector).getScale();
                    BigDecimal decimalValue = DecimalUtility.getBigDecimalFromArrowBuf(buffer, index, scale,
                            Decimal256Vector.TYPE_WIDTH);
                    // We write the unscaled value, because the scale is stored in the type metadata.
                    generator.writeString(decimalValue.unscaledValue().toString());
                    break;
                }

                case EXTENSIONTYPE: {
                    generator.writeStartObject();
                    generator.writeFieldName(String.format("$%s", ((ArrowType.ExtensionType) vector.getField().getType()).extensionName()));
                    FieldVector underlyingVector = ((ExtensionTypeVector<?>) vector).getUnderlyingVector();

                    MinorType underlyingMinorType = underlyingVector.getMinorType();
                    ArrowBuf offsetBuf =
                            (underlyingMinorType == MinorType.VARCHAR || underlyingMinorType == MinorType.VARBINARY)
                                    ? underlyingVector.getOffsetBuffer() : null;

                    writeValueToGenerator(bufferType, underlyingVector.getDataBuffer(), offsetBuf, underlyingVector, index);
                    generator.writeEndObject();
                    break;
                }

                default:
                    throw new UnsupportedOperationException("minor type: " + vector.getMinorType());
            }
        }
    }

    @Override
    public void close() throws IOException {
        generator.writeEndArray();
        generator.writeEndObject();
        generator.close();
    }

}
