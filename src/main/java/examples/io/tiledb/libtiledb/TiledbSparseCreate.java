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

package examples.io.tiledb.libtiledb;

import java.math.BigInteger;

import io.tiledb.libtiledb.*;

public class TiledbSparseCreate {

  /*
   * It shows how to create a sparse array. Make sure that no directory exists
   * with the name `my_sparse_array` in the current working directory.
   */
  public static void main(String[] args) {
    // Create TileDB context
    SWIGTYPE_p_p_tiledb_ctx_t ctxpp = tiledb.new_tiledb_ctx_tpp();
    tiledb.tiledb_ctx_alloc(null, ctxpp);
    SWIGTYPE_p_tiledb_ctx_t ctx = tiledb.tiledb_ctx_tpp_value(ctxpp);

    // Create getDimensions
    long[] d1_domain_ = {1, 4};
    uint64_tArray d1_domain = Utils.newUint64Array(d1_domain_);
    long[] d1_tile_extents_ = {2};
    uint64_tArray d1_tile_extents = Utils
        .newUint64Array(d1_tile_extents_);
    SWIGTYPE_p_p_tiledb_dimension_t d1pp = tiledb.new_tiledb_dimension_tpp();
    tiledb.tiledb_dimension_alloc(ctx, "d1",
        tiledb_datatype_t.TILEDB_UINT64,
        PointerUtils.toVoid(d1_domain),
        PointerUtils.toVoid(d1_tile_extents), 
	d1pp);
    SWIGTYPE_p_tiledb_dimension_t d1 = tiledb.tiledb_dimension_tpp_value(d1pp);

    long[] d2_domain_ = {1, 4};
    uint64_tArray d2_domain = Utils.newUint64Array(d2_domain_);
    long[] d2_tile_extents_ = {2};
    uint64_tArray d2_tile_extents = Utils
        .newUint64Array(d2_tile_extents_);
    SWIGTYPE_p_p_tiledb_dimension_t d2pp = tiledb.new_tiledb_dimension_tpp();
    tiledb.tiledb_dimension_alloc(ctx, "d2",
        tiledb_datatype_t.TILEDB_UINT64,
        PointerUtils.toVoid(d2_domain),
        PointerUtils.toVoid(d2_tile_extents),
	d2pp);
    SWIGTYPE_p_tiledb_dimension_t d2 = tiledb.tiledb_dimension_tpp_value(d2pp);

    // Create getDomain
    SWIGTYPE_p_p_tiledb_domain_t domainpp = tiledb.new_tiledb_domain_tpp();
    tiledb.tiledb_domain_alloc(ctx, domainpp);
    SWIGTYPE_p_tiledb_domain_t domain = tiledb.tiledb_domain_tpp_value(domainpp);
    tiledb.tiledb_domain_add_dimension(ctx, domain, d1);
    tiledb.tiledb_domain_add_dimension(ctx, domain, d2);

    // Create getAttributes
    SWIGTYPE_p_p_tiledb_attribute_t a1pp = tiledb.new_tiledb_attribute_tpp();
    tiledb.tiledb_attribute_alloc(ctx, "a1",
        tiledb_datatype_t.TILEDB_INT32, a1pp);
    SWIGTYPE_p_tiledb_attribute_t a1 = tiledb.tiledb_attribute_tpp_value(a1pp);
    tiledb.tiledb_attribute_set_compressor(ctx, a1,
        tiledb_compressor_t.TILEDB_GZIP, -1);
    tiledb.tiledb_attribute_set_cell_val_num(ctx, a1, 1);

    SWIGTYPE_p_p_tiledb_attribute_t a2pp = tiledb.new_tiledb_attribute_tpp();
    tiledb.tiledb_attribute_alloc(ctx, "a2",
        tiledb_datatype_t.TILEDB_CHAR, a2pp);
    SWIGTYPE_p_tiledb_attribute_t a2 = tiledb.tiledb_attribute_tpp_value(a2pp);
    tiledb.tiledb_attribute_set_compressor(ctx, a2,
        tiledb_compressor_t.TILEDB_GZIP, -1);
    tiledb.tiledb_attribute_set_cell_val_num(ctx, a2, tiledb.tiledb_var_num());

    SWIGTYPE_p_p_tiledb_attribute_t a3pp = tiledb.new_tiledb_attribute_tpp();
    tiledb.tiledb_attribute_alloc(ctx, "a3",
        tiledb_datatype_t.TILEDB_FLOAT32, a3pp);
    SWIGTYPE_p_tiledb_attribute_t a3 = tiledb.tiledb_attribute_tpp_value(a3pp);
    tiledb.tiledb_attribute_set_compressor(ctx, a3,
        tiledb_compressor_t.TILEDB_ZSTD, -1);
    tiledb.tiledb_attribute_set_cell_val_num(ctx, a3, 2);

    // Create array schema
    SWIGTYPE_p_p_tiledb_array_schema_t array_schemapp = tiledb.new_tiledb_array_schema_tpp();
    tiledb.tiledb_array_schema_alloc(ctx,
        tiledb_array_type_t.TILEDB_SPARSE, array_schemapp);
    SWIGTYPE_p_tiledb_array_schema_t array_schema = tiledb.tiledb_array_schema_tpp_value(array_schemapp);
    tiledb.tiledb_array_schema_set_cell_order(ctx, array_schema,
        tiledb_layout_t.TILEDB_ROW_MAJOR);
    tiledb.tiledb_array_schema_set_tile_order(ctx, array_schema,
        tiledb_layout_t.TILEDB_ROW_MAJOR);
    tiledb.tiledb_array_schema_set_capacity(ctx, array_schema,
        new BigInteger("2"));
    tiledb.tiledb_array_schema_set_domain(ctx, array_schema, domain);
    tiledb.tiledb_array_schema_add_attribute(ctx, array_schema, a1);
    tiledb.tiledb_array_schema_add_attribute(ctx, array_schema, a2);
    tiledb.tiledb_array_schema_add_attribute(ctx, array_schema, a3);

    // Check array metadata
    if (tiledb.tiledb_array_schema_check(ctx, array_schema) != tiledb.TILEDB_OK) {
      System.out.println("Invalid array metadata");
      System.exit(1);
    }
    // Create array
    tiledb.tiledb_array_create(ctx, "my_sparse_array", array_schema);

    // Clean up
    tiledb.tiledb_attribute_free(a1pp);
    tiledb.tiledb_attribute_free(a2pp);
    tiledb.tiledb_attribute_free(a3pp);
    tiledb.tiledb_dimension_free(d1pp);
    tiledb.tiledb_dimension_free(d2pp);
    tiledb.tiledb_domain_free(domainpp);
    tiledb.tiledb_array_schema_free(array_schemapp);
    tiledb.tiledb_ctx_free(ctxpp);
  }
}