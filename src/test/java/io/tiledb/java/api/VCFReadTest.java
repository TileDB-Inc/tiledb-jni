/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 TileDB, Inc.
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

package io.tiledb.java.api;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.tiledb.java.api.ArrayType.TILEDB_SPARSE;
import static io.tiledb.java.api.Constants.TILEDB_COORDS;
import static io.tiledb.java.api.Constants.TILEDB_VAR_NUM;
import static io.tiledb.java.api.Layout.TILEDB_GLOBAL_ORDER;
import static io.tiledb.java.api.Layout.TILEDB_ROW_MAJOR;
import static io.tiledb.java.api.QueryType.TILEDB_READ;
import static io.tiledb.java.api.QueryType.TILEDB_WRITE;

@SuppressWarnings("ALL")
public class VCFReadTest {

  private Context ctx;
  private String arrayURI = "my_sparse_array";

  @Before
  public void setup() throws Exception {
    ctx = new Context();
    if (Files.exists(Paths.get(arrayURI))) {
      TileDBObject.remove(ctx, arrayURI);
    }
  }

  @After
  public void teardown() throws Exception {
    if (Files.exists(Paths.get(arrayURI))) {
      TileDBObject.remove(ctx, arrayURI);
    }
  }

  @Test
  public void read() throws Exception {
    int bufferSize = 2048;
    // Create TileDB context
    Context ctx = new Context();

    // Calculate maximum buffer elements for the query results per attribute
    Array vcf_array = new Array(ctx, "/Users/victor/Dev/tiledb/TileDB-Helix-Spark/libtiledbvcf/test/inputs/arrays/ingested_2samples/sample");

    // Set data buffers
    NativeArray posData = new NativeArray(ctx, bufferSize,Datatype.TILEDB_UINT32);
    NativeArray qualData = new NativeArray(ctx, bufferSize,Datatype.TILEDB_FLOAT32);
    NativeArray allelesData = new NativeArray(ctx, bufferSize,Datatype.TILEDB_CHAR);
    NativeArray filterIdsData = new NativeArray(ctx, bufferSize,Datatype.TILEDB_INT32);

    // Set offsets buffers
    NativeArray filterIdsOffsets = new NativeArray(ctx, bufferSize*2,Datatype.TILEDB_UINT64);
    NativeArray allelesOffsets = new NativeArray(ctx, bufferSize*2, Datatype.TILEDB_UINT64);

    // Set coordinates buffer
    NativeArray coords = new NativeArray(ctx, bufferSize, Datatype.TILEDB_UINT32);

    Query query = new Query(vcf_array, TILEDB_READ);
    query.setBuffer("pos", posData);
    query.setBuffer("qual", qualData);
    query.setBuffer("alleles", allelesOffsets, allelesData);
    query.setBuffer("filter_ids", filterIdsOffsets, filterIdsData);

    query.setCoordinates(coords);

    query.setLayout(TILEDB_ROW_MAJOR);

    query.setSubarray(new NativeArray(ctx, new long[]{1, 1, 53435, 3435976}, Datatype.TILEDB_UINT32));

    while (query.getQueryStatus() != QueryStatus.TILEDB_COMPLETED){
      query.submit();

      long[] posResults = (long[])query.getBuffer("pos");
      float[] qual = (float[])query.getBuffer("qual");
      byte[] alleles = (byte[])query.getBuffer("alleles");
      long[] allelesOffs = (long[])query.getVarBuffer("alleles");
      int[] filterIds = (int[])query.getBuffer("filter_ids");
      long[] filterIdsOffs = (long[])query.getVarBuffer("filter_ids");
      long[] coordsData = (long[]) query.getBuffer(TILEDB_COORDS);

      long numResults = query.resultBufferElements().get(TILEDB_COORDS).getSecond() / vcf_array.getSchema().getDomain().getNDim();


      for (int r=0; r<numResults; ++r) {
        long i = coordsData[2 * r], j = coordsData[2 * r + 1];

        int allelesStartPos = (int)allelesOffs[r];
        int allelesEndPos;

        int filterIdsStartPos = (int)filterIdsOffs[r];
        int filterIdsEndPos;

        String allelesStr;
        int[] f_ids;

        if (r == numResults-1) {
          allelesStr = new String(Arrays.copyOfRange(alleles, allelesStartPos, alleles.length));
          //f_ids = Arrays.copyOfRange(filterIds, filterIdsStartPos, filterIds.length);
        }
        else {
          allelesEndPos = (int)allelesOffs[r+1] - 1;
          filterIdsEndPos = (int)filterIdsOffs[r+1] - 1;
          allelesStr = new String(Arrays.copyOfRange(alleles, allelesStartPos, allelesEndPos));
          //f_ids = Arrays.copyOfRange(filterIds, filterIdsStartPos, filterIdsEndPos);
        }
        //
        System.out.printf("(%d, %d) -> |%d|%f|%s|%s|\n", i, j, posResults[r], qual[r], allelesStr, "");

      }
    }
  }
}
