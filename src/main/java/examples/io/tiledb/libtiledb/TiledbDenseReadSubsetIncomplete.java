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

public class TiledbDenseReadSubsetIncomplete {

  public static void main(String[] args) {
    // Create TileDB context
    SWIGTYPE_p_p_tiledb_ctx_t ctxpp = tiledb.new_tiledb_ctx_tpp();
    tiledb.tiledb_ctx_alloc(null, ctxpp);
    SWIGTYPE_p_tiledb_ctx_t ctx = tiledb.tiledb_ctx_tpp_value(ctxpp);

    // Open array
    SWIGTYPE_p_p_tiledb_array_t arraypp = tiledb.new_tiledb_array_tpp();
    tiledb.tiledb_array_alloc(ctx, "my_dense_array", arraypp);
    SWIGTYPE_p_tiledb_array_t arrayp = tiledb.tiledb_array_tpp_value(arraypp);
    tiledb.tiledb_array_open(ctx, arrayp, tiledb_query_type_t.TILEDB_READ);

    // Prepare cell buffers
    int32_tArray buffer_a1 = new int32_tArray(2);

    uint64_tArray buffer_a1_size = new uint64_tArray(1);
    buffer_a1_size.setitem(0, new BigInteger("8"));

    // Create query
    long[] subarray_ = {3, 4, 2, 4};
    uint64_tArray subarray = Utils.newUint64Array(subarray_);
    SWIGTYPE_p_p_tiledb_query_t querypp = tiledb.new_tiledb_query_tpp();
    tiledb.tiledb_query_alloc(ctx, arrayp,
        tiledb_query_type_t.TILEDB_READ, querypp);

    SWIGTYPE_p_tiledb_query_t query = tiledb.tiledb_query_tpp_value(querypp);
    tiledb.tiledb_query_set_layout(ctx, query,
        tiledb_layout_t.TILEDB_COL_MAJOR);

    tiledb.tiledb_query_set_subarray(ctx, query,
        PointerUtils.toVoid(subarray));

    tiledb.tiledb_query_set_buffer(ctx, query, "a1",
	PointerUtils.toVoid(buffer_a1), buffer_a1_size.cast());

    // Loop until the query is completed
    System.out.printf("a1\n---\n");

    SWIGTYPE_p_tiledb_query_status_t statusp = tiledb.new_tiledb_query_status_tp();

    do {
      System.out.printf("Reading cells...\n");
      tiledb.tiledb_query_submit(ctx, query);

      // Print cell values
      int result_num = buffer_a1_size.getitem(0).intValue() / 4;
      for (int i = 0; i < result_num; ++i)
        System.out.printf("%d\n", buffer_a1.getitem(i));

      // Get status
      tiledb.tiledb_query_get_status(ctx, query, statusp);
    } while (tiledb.tiledb_query_status_tp_value(statusp) == tiledb_query_status_t.TILEDB_INCOMPLETE);

    // Finalize query
    tiledb.tiledb_query_finalize(ctx, query);

    // Close array
    tiledb.tiledb_array_close(ctx, arrayp);

    // Clean up
    tiledb.tiledb_array_free(arraypp);
    tiledb.tiledb_query_free(querypp);
    tiledb.tiledb_ctx_free(ctxpp);
    buffer_a1.delete();
  }
}