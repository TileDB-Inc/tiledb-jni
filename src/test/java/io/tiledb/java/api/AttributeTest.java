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

import static io.tiledb.java.api.Constants.TILEDB_VAR_NUM;

import org.junit.Assert;
import org.junit.Test;

public class AttributeTest {

  @Test
  public void testAttribute() throws Exception {
    try (Context ctx = new Context();
        Attribute a = new Attribute(ctx, "a1", Long.class).setCellVar()) {
      try (FilterList filterList = new FilterList(ctx).addFilter(new GzipFilter(ctx, 1))) {
        a.setFilterList(filterList);
      }
      Assert.assertEquals(a.getName(), "a1");
      Assert.assertEquals(a.getType(), Datatype.TILEDB_INT64);
      Assert.assertTrue(a.isVar());
      Assert.assertEquals(a.getCellValNum(), TILEDB_VAR_NUM);
      try (FilterList filterList = a.getFilterList()) {
        Assert.assertEquals(filterList.getNumFilters(), 1L);
        try (Filter filter = filterList.getFilter(0L)) {
          Assert.assertTrue(filter instanceof GzipFilter);
          Assert.assertEquals(((GzipFilter) filter).getLevel(), 1L);
        }
      }
    }
  }

  @Test
  public void testAttributeDatatype() throws Exception {
    try (Context ctx = new Context();
        Attribute a = new Attribute(ctx, "a2", Datatype.TILEDB_FLOAT32)) {
      Assert.assertEquals(Datatype.TILEDB_FLOAT32, a.getType());
    }
  }

  @Test
  public void testAttributeSetFillValue() throws Exception {
    try (Context ctx = new Context();
        Attribute a = new Attribute(ctx, "a2", Datatype.TILEDB_INT32)) {

      a.setFillValue(5);

      Assert.assertEquals(5, a.getFillValue().getFirst());

      Assert.assertEquals(a.getType().getNativeSize(), (int) (long) a.getFillValue().getSecond());
    }

    try (Context ctx = new Context();
        Attribute a = new Attribute(ctx, "a2", Datatype.TILEDB_INT64)) {

      a.setFillValue(5L);

      Assert.assertEquals(5L, a.getFillValue().getFirst());
      Assert.assertEquals(a.getType().getNativeSize(), (int) (long) a.getFillValue().getSecond());
    }

    try (Context ctx = new Context();
        Attribute a = new Attribute(ctx, "a2", Datatype.TILEDB_CHAR)) {

      a.setFillValue((byte) 'c');

      Assert.assertEquals((byte) 'c', a.getFillValue().getFirst());
      Assert.assertEquals(
          (int) (long) a.getType().getNativeSize(), (int) (long) a.getFillValue().getSecond());
    }

    try (Context ctx = new Context();
        Attribute a = new Attribute(ctx, "a2", Datatype.TILEDB_CHAR)) {

      a.setFillValue((byte) 'c');

      Assert.assertEquals((byte) 'c', a.getFillValue().getFirst());
      Assert.assertEquals(a.getType().getNativeSize(), (int) (long) a.getFillValue().getSecond());
    }

    try (Context ctx = new Context();
        Attribute a = new Attribute(ctx, "a2", Datatype.TILEDB_STRING_ASCII)) {
      a.setCellVar();

      String str = "abcdef";
      a.setFillValue(str);

      Assert.assertEquals(str, new String((byte[]) a.getFillValue().getFirst()));
      Assert.assertEquals(str.length(), (int) (long) a.getFillValue().getSecond());
    }

    try (Context ctx = new Context();
        Attribute a = new Attribute(ctx, "a2", Datatype.TILEDB_INT32)) {

      a.setCellValNum(2);

      int[] arr = new int[] {1, 2};
      a.setFillValue(arr);

      Assert.assertArrayEquals(arr, (int[]) a.getFillValue().getFirst());
      Assert.assertEquals(
          arr.length * a.getType().getNativeSize(), (int) (long) a.getFillValue().getSecond());
    }
  }

  @Test
  public void testAttributeSetFillValueVarSize() throws Exception {
    try (Context ctx = new Context();
        Attribute a = new Attribute(ctx, "a2", Datatype.TILEDB_INT32)) {

      a.setCellValNum(2);

      int[] arr = new int[] {1, 2};

      a.setFillValue(arr);
      Assert.assertArrayEquals(arr, (int[]) a.getFillValue().getFirst());

      a.setCellVar();

      a.setFillValue(arr);
      Assert.assertArrayEquals(arr, (int[]) a.getFillValue().getFirst());
    }
  }

  @Test
  public void testAttributeSetFillValueNullable() throws Exception {
    try (Context ctx = new Context();
        Attribute a = new Attribute(ctx, "a2", Datatype.TILEDB_INT32)) {

      a.setNullable(true);

      a.setFillValueNullable(5, true);

      Assert.assertEquals(5, a.getFillValueNullable().getFirst());
      Assert.assertEquals(
          a.getType().getNativeSize(),
          (int) (long) a.getFillValueNullable().getSecond().getFirst());

      Assert.assertEquals(true, a.getFillValueNullable().getSecond().getSecond());
    }

    try (Context ctx = new Context();
        Attribute a = new Attribute(ctx, "a2", Datatype.TILEDB_INT64)) {

      a.setNullable(true);

      a.setFillValueNullable(5L, false);

      Assert.assertEquals(5L, a.getFillValueNullable().getFirst());
      Assert.assertEquals(
          a.getType().getNativeSize(),
          (int) (long) a.getFillValueNullable().getSecond().getFirst());

      Assert.assertEquals(false, a.getFillValueNullable().getSecond().getSecond());
    }

    try (Context ctx = new Context();
        Attribute a = new Attribute(ctx, "a2", Datatype.TILEDB_CHAR)) {

      a.setNullable(true);

      a.setFillValueNullable((byte) 'c', false);

      Assert.assertEquals((byte) 'c', a.getFillValueNullable().getFirst());
      Assert.assertEquals(
          a.getType().getNativeSize(),
          (int) (long) a.getFillValueNullable().getSecond().getFirst());

      Assert.assertEquals(false, a.getFillValueNullable().getSecond().getSecond());
    }

    try (Context ctx = new Context();
        Attribute a = new Attribute(ctx, "a2", Datatype.TILEDB_STRING_ASCII)) {
      a.setNullable(true);
      a.setCellVar();

      String str = "abcdef";
      a.setFillValueNullable(str, true);

      Assert.assertEquals(str, new String((byte[]) a.getFillValueNullable().getFirst()));
      Assert.assertEquals(
          str.length(), (int) (long) a.getFillValueNullable().getSecond().getFirst());
    }

    try (Context ctx = new Context();
        Attribute a = new Attribute(ctx, "a2", Datatype.TILEDB_INT32)) {
      a.setNullable(true);

      a.setCellValNum(2);

      int[] arr = new int[] {1, 2};
      a.setFillValueNullable(arr, true);

      Assert.assertArrayEquals(arr, (int[]) a.getFillValueNullable().getFirst());
      Assert.assertEquals(
          arr.length * a.getType().getNativeSize(),
          (long) (int) a.getFillValueNullable().getSecond().getFirst());

      a.setCellVar();

      a.setFillValueNullable(arr, true);

      Assert.assertArrayEquals(arr, (int[]) a.getFillValueNullable().getFirst());
      Assert.assertEquals(
          arr.length * a.getType().getNativeSize(),
          (long) (int) a.getFillValueNullable().getSecond().getFirst());
    }
  }

  @Test
  public void testAttributeSetFillValueNullableVarSize() throws Exception {
    try (Context ctx = new Context();
        Attribute a = new Attribute(ctx, "a2", Datatype.TILEDB_INT32)) {
      a.setNullable(true);

      a.setCellValNum(2);

      int[] arr = new int[] {1, 2};
      a.setFillValueNullable(arr, true);

      Assert.assertArrayEquals(arr, (int[]) a.getFillValueNullable().getFirst());
    }
  }

  @Test
  public void testAttributeSetNullable() throws Exception {
    try (Context ctx = new Context();
        Attribute a = new Attribute(ctx, "a2", Datatype.TILEDB_FLOAT32)) {
      a.setNullable(false);
    }
  }
}
