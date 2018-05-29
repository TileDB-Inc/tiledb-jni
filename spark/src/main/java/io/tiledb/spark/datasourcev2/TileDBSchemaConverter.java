/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.tiledb.spark.datasourcev2;

import io.tiledb.java.api.*;
import io.tiledb.libtiledb.*;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;

import static org.apache.spark.sql.types.DataTypes.*;

public class TileDBSchemaConverter {

  private Context ctx;
  private TileDBOptions options;
  private StructType requiredSchema;

  public TileDBSchemaConverter(Context ctx, DataSourceOptions options) {
    this.ctx = ctx;
    this.options = new TileDBOptions(options);
  }

  public TileDBSchemaConverter(Context ctx, TileDBOptions tileDBOptions) {
    this.ctx = ctx;
    this.options = tileDBOptions;
  }

  public void setRequiredSchema(StructType requiredSchema) {
    this.requiredSchema = requiredSchema;
  }

  public StructType getSchema() throws TileDBError {
    String arrayURI = options.ARRAY_URI;
    Array array = new Array(ctx, arrayURI);
    ArraySchema arraySchema = array.getSchema();
    StructType schema = new StructType();
    for (Dimension dimension : arraySchema.getDomain().getDimensions()){
      if(requiredSchema==null || requiredSchema.getFieldIndex(dimension.getName()).isDefined()) {
        schema = schema.add(toStructField(dimension.getType(), 1l, dimension.getName()));
      }
    }
    for( Attribute attribute : arraySchema.getAttributes().values()){
      if(requiredSchema==null || requiredSchema.getFieldIndex(attribute.getName()).isDefined()) {
        schema = schema.add(toStructField(attribute.getType(), attribute.getCellValNum(), attribute.getName()));
      }
    }
    return schema;
  }

  private StructField toStructField(tiledb_datatype_t type, long cellValNum, String name) throws TileDBError {
    StructField field = null;
    switch (type) {
      case TILEDB_FLOAT32: {
        if (cellValNum > 1)
          field = new StructField(name, DataTypes.createArrayType(FloatType), true, Metadata.empty());
        else
          field = new StructField(name, FloatType, true, Metadata.empty());
        break;
      }
      case TILEDB_FLOAT64: {
        if (cellValNum > 1)
          field = new StructField(name, DataTypes.createArrayType(DoubleType), true, Metadata.empty());
        else
          field = new StructField(name, DoubleType, true, Metadata.empty());
        break;
      }
      case TILEDB_INT8: {
        if (cellValNum > 1)
          field = new StructField(name, DataTypes.createArrayType(ByteType), true, Metadata.empty());
        else
          field = new StructField(name, ByteType, true, Metadata.empty());
        break;
      }
      case TILEDB_INT16: {
        if (cellValNum > 1)
          field = new StructField(name, DataTypes.createArrayType(ShortType), true, Metadata.empty());
        else
          field = new StructField(name, ShortType, true, Metadata.empty());
        break;
      }
      case TILEDB_INT32: {
        if (cellValNum > 1)
          field = new StructField(name, DataTypes.createArrayType(IntegerType), true, Metadata.empty());
        else
          field = new StructField(name, IntegerType, true, Metadata.empty());
        break;
      }
      case TILEDB_INT64: {
        if (cellValNum > 1)
          field = new StructField(name, DataTypes.createArrayType(LongType), true, Metadata.empty());
        else
          field = new StructField(name, LongType, true, Metadata.empty());
        break;
      }
      case TILEDB_UINT8: {
        if (cellValNum > 1)
          field = new StructField(name, DataTypes.createArrayType(ShortType), true, Metadata.empty());
        else
          field = new StructField(name, ShortType, true, Metadata.empty());
        break;
      }
      case TILEDB_UINT16: {
        if (cellValNum > 1)
          field = new StructField(name, DataTypes.createArrayType(IntegerType), true, Metadata.empty());
        else
          field = new StructField(name, IntegerType, true, Metadata.empty());
        break;
      }
      case TILEDB_UINT32: {
        if (cellValNum > 1)
          field = new StructField(name, DataTypes.createArrayType(LongType), true, Metadata.empty());
        else
          field = new StructField(name, LongType, true, Metadata.empty());
        break;
      }
      case TILEDB_UINT64: {
        if (cellValNum > 1)
          field = new StructField(name, DataTypes.createArrayType(LongType), true, Metadata.empty());
        else
          field = new StructField(name, LongType, true, Metadata.empty());
        break;
      }
      case TILEDB_CHAR: {
        field = new StructField(name, StringType, true, Metadata.empty());
        break;
      }
      default: {
        throw new TileDBError("Not supported getDomain getType " + type);
      }
    }
    return field;
  }

  public ArraySchema toTileDBSchema(StructType schema) throws Exception {
    ArraySchema arraySchema = new ArraySchema(ctx, tiledb_array_type_t.TILEDB_SPARSE);
    arraySchema.setTileOrder(tiledb_layout_t.TILEDB_ROW_MAJOR);
    arraySchema.setCellOrder(tiledb_layout_t.TILEDB_ROW_MAJOR);
    Domain domain = new Domain(ctx);
    ArrayList<Attribute> attributes = new ArrayList<Attribute>();
    for(StructField field : schema.fields()){
      if(options.DIMENSIONS.contains(field.name())) {
        Dimension dimension = toDimension(field);
        domain.addDimension(dimension);
      }
      else {
        Attribute attribute = toAttribute(field);
        attributes.add(attribute);
      }
    }

    arraySchema.setDomain(domain);
    for(Attribute attribute : attributes){
      arraySchema.addAttribute(attribute);
    }

    // Check array schema
    arraySchema.check();

    return arraySchema;
  }

  private Dimension toDimension(StructField field) throws Exception {
    DataType dataType = field.dataType();
    if (dataType instanceof IntegerType) {
    } else if (dataType instanceof LongType) {
      return new Dimension<Long>(ctx,field.name(),Long.class, new Pair<Long, Long>(1l,4l),2l);
    } else if (dataType instanceof ShortType) {
    } else if (dataType instanceof ByteType) {
    } else {
      throw new Exception("Datatype not supported for dimension: " + dataType);
    }
    return null;

  }

  private Attribute toAttribute(StructField field) throws Exception {
    DataType dataType = field.dataType();
    if (dataType instanceof IntegerType) {
      return new Attribute(ctx, field.name(), Integer.class);
    } else if (dataType instanceof StringType) {
      Attribute attribute = new Attribute(ctx, field.name(), String.class);
      attribute.setCellValNum(tiledb.tiledb_var_num());
      return attribute;
    } else if (dataType instanceof ShortType) {
    } else if (dataType instanceof ByteType) {

    } else if (dataType instanceof ArrayType) {
      ArrayType at = (ArrayType)dataType;
      DataType type = at.elementType();
      if (type instanceof FloatType) {
        Attribute attribute = new Attribute(ctx, field.name(), Float.class);
        attribute.setCellValNum(tiledb.tiledb_var_num());
        return attribute;
      } else if (type instanceof LongType) {
      } else if (type instanceof ShortType) {
      } else if (type instanceof ByteType) {

      } else {
        throw new Exception("Datatype not supported for attribute: " + dataType);
      }

    } else {
      throw new Exception("Datatype not supported for attribute: " + dataType);
    }
    return null;

//    Attribute a1 = new Attribute(ctx, "a1", Integer.class);
//    Attribute a2 = new Attribute(ctx, "a2", String.class);
//    a2.setCellValNum(tiledb.tiledb_var_num());
//    Attribute a3 = new Attribute(ctx, "a3", Float.class);
//    a3.setCellValNum(2);
//    a1.setCompressor(new Compressor(tiledb_compressor_t.TILEDB_BLOSC_LZ4, -1));
  }
}
