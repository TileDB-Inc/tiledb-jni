package io.tiledb.java.api;

import static io.tiledb.java.api.ArrayType.TILEDB_DENSE;
import static io.tiledb.java.api.ArrayType.TILEDB_SPARSE;
import static io.tiledb.java.api.Layout.TILEDB_GLOBAL_ORDER;
import static io.tiledb.java.api.Layout.TILEDB_ROW_MAJOR;
import static io.tiledb.java.api.QueryType.TILEDB_WRITE;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FragmentInfoTest {

  private Context ctx;
  private String arrayURI = "array";

  @Before
  public void setup() throws Exception {
    this.ctx = new Context();
    if (Files.exists(Paths.get(arrayURI))) {
      TileDBObject.remove(ctx, arrayURI);
    }
  }

  @After
  public void teardown() throws Exception {
    if (Files.exists(Paths.get(arrayURI))) {
      TileDBObject.remove(ctx, arrayURI);
    }
    ctx.close();
  }

  @Test
  public void testFragmentInfo() throws Exception {
    int testFragmentCount = 10;
    createDenseArray();

    // Write three fragments
    for (int i = 0; i < testFragmentCount; ++i) writeDenseArray();

    FragmentInfo info = new FragmentInfo(ctx, arrayURI);

    long numFragments = info.getFragmentNum();

    Assert.assertEquals(testFragmentCount, numFragments);

    for (int i = 0; i < numFragments; ++i) {
      URI uri = new URI(info.getFragmentURI(i));
      String path = uri.getPath();

      File fragment = new File(path);

      long size = FileUtils.sizeOfDirectory(fragment);

      Assert.assertEquals(size, info.getFragmentSize(i));
      Assert.assertTrue(info.getDense(i));
      Assert.assertFalse(info.getSparse(i));

      Pair<Long, Long> range = info.getTimestampRange(i);

      // Check if the timestamp range values comply with the timestamps in the file name
      String[] fileNameSplit = fragment.getName().split("_");

      Assert.assertEquals(fileNameSplit[2], range.getFirst().toString());
      Assert.assertEquals(fileNameSplit[3], range.getSecond().toString());

      Array arr = new Array(ctx, arrayURI);
      Domain domain = arr.getSchema().getDomain();
      for (int dim = 0; dim < domain.getNDim(); ++dim) {
        Dimension dimension = domain.getDimension(dim);

        Pair p = info.getNonEmptyDomainFromIndex(i, dim);

        Assert.assertEquals(p.getFirst(), arr.nonEmptyDomain().get(dimension.getName()).getFirst());
        Assert.assertEquals(
            p.getSecond(), arr.nonEmptyDomain().get(dimension.getName()).getSecond());

        p = info.getNonEmptyDomainFromName(i, dimension.getName());

        Assert.assertEquals(p.getFirst(), arr.nonEmptyDomain().get(dimension.getName()).getFirst());
        Assert.assertEquals(
            p.getSecond(), arr.nonEmptyDomain().get(dimension.getName()).getSecond());
      }

      Assert.assertEquals(8, info.getCellNum(i));

      Assert.assertEquals(
          fileNameSplit[fileNameSplit.length - 1], ((Long) info.getVersion(i)).toString());

      Assert.assertFalse(info.hasConsolidatedMetadata(i));

      Assert.assertEquals(testFragmentCount, info.getUnconsolidatedMetadataNum());
    }
  }

  @Test
  public void testFragmentInfoVar() throws Exception {
    int testFragmentCount = 10;

    createSparseVarDimArray();

    // Write three fragments
    for (int i = 0; i < testFragmentCount; ++i) writeSparseVarDimArray();

    FragmentInfo info = new FragmentInfo(ctx, arrayURI);

    long numFragments = info.getFragmentNum();

    Assert.assertEquals(testFragmentCount, numFragments);
    //
    for (int i = 0; i < numFragments; ++i) {
      URI uri = new URI(info.getFragmentURI(i));
      String path = uri.getPath();

      File fragment = new File(path);

      long size = FileUtils.sizeOfDirectory(fragment);

      Assert.assertEquals(size, info.getFragmentSize(i));
      Assert.assertFalse(info.getDense(i));
      Assert.assertTrue(info.getSparse(i));

      Pair<Long, Long> range = info.getTimestampRange(i);

      // Check if the timestamp range values comply with the timestamps in the file name
      String[] fileNameSplit = fragment.getName().split("_");

      Assert.assertEquals(fileNameSplit[2], range.getFirst().toString());
      Assert.assertEquals(fileNameSplit[3], range.getSecond().toString());

      Array arr = new Array(ctx, arrayURI);
      Domain domain = arr.getSchema().getDomain();
      for (int dim = 0; dim < domain.getNDim(); ++dim) {
        Dimension dimension = domain.getDimension(dim);

        Pair p = info.getNonEmptyDomainVarFromIndex(i, dim);

        Assert.assertEquals(
            new String((byte[]) p.getFirst()),
            arr.nonEmptyDomain().get(dimension.getName()).getFirst());
        Assert.assertEquals(
            new String((byte[]) p.getSecond()),
            arr.nonEmptyDomain().get(dimension.getName()).getSecond());

        p = info.getNonEmptyDomainVarFromName(i, dimension.getName());

        Assert.assertEquals(
            new String((byte[]) p.getFirst()),
            arr.nonEmptyDomain().get(dimension.getName()).getFirst());
        Assert.assertEquals(
            new String((byte[]) p.getSecond()),
            arr.nonEmptyDomain().get(dimension.getName()).getSecond());
      }

      Assert.assertEquals(5, info.getCellNum(i));

      Assert.assertEquals(
          fileNameSplit[fileNameSplit.length - 1], ((Long) info.getVersion(i)).toString());

      Assert.assertFalse(info.hasConsolidatedMetadata(i));

      Assert.assertEquals(testFragmentCount, info.getUnconsolidatedMetadataNum());
    }
  }

  @Test
  public void TestFragmentInfoVarSizesOnly() throws Exception {

    int testFragmentCount = 10;
    createSparseVarDimArray();

    // Write fragments
    for (int i = 0; i < testFragmentCount; ++i) writeSparseVarDimArray();

    FragmentInfo info = new FragmentInfo(ctx, arrayURI);

    long numFragments = info.getFragmentNum();

    for (int i = 0; i < numFragments; ++i) {

      Array arr = new Array(ctx, arrayURI);
      Domain domain = arr.getSchema().getDomain();
      for (int dim = 0; dim < domain.getNDim(); ++dim) {
        Dimension dimension = domain.getDimension(dim);

        Pair p = info.getNonEmptyDomainVarSizeFromIndex(i, dim);

        Assert.assertEquals(
            new String((byte[]) p.getFirst()),
            arr.nonEmptyDomain().get(dimension.getName()).getFirst());
        Assert.assertEquals(
            new String((byte[]) p.getSecond()),
            arr.nonEmptyDomain().get(dimension.getName()).getSecond());

        p = info.getNonEmptyDomainVarSizeFromName(i, dimension.getName());

        Assert.assertEquals(
            new String((byte[]) p.getFirst()),
            arr.nonEmptyDomain().get(dimension.getName()).getFirst());
        Assert.assertEquals(
            new String((byte[]) p.getSecond()),
            arr.nonEmptyDomain().get(dimension.getName()).getSecond());
      }
    }
  }

  public void createDenseArray() throws Exception {
    // Create getDimensions
    Dimension<Integer> rows = new Dimension<Integer>(ctx, "rows", Integer.class, new Pair(1, 4), 2);
    Dimension<Integer> cols = new Dimension<Integer>(ctx, "cols", Integer.class, new Pair(1, 4), 2);

    // Create and set getDomain
    Domain domain = new Domain(ctx);
    domain.addDimension(rows);
    domain.addDimension(cols);

    // Create and add getAttributes
    Attribute a = new Attribute(ctx, "a", Integer.class);

    ArraySchema schema = new ArraySchema(ctx, TILEDB_DENSE);
    schema.setTileOrder(TILEDB_ROW_MAJOR);
    schema.setCellOrder(TILEDB_ROW_MAJOR);
    schema.setDomain(domain);
    schema.addAttribute(a);

    Array.create(arrayURI, schema);
  }

  public void writeDenseArray() throws Exception {
    // Prepare cell buffers
    NativeArray data = new NativeArray(ctx, new int[] {1, 2, 3, 4, 5, 6, 7, 8}, Integer.class);

    NativeArray subarray = new NativeArray(ctx, new int[] {1, 2, 1, 4}, Integer.class);

    // Create query
    Array array = new Array(ctx, arrayURI, TILEDB_WRITE);
    Query query = new Query(array);
    query.setLayout(TILEDB_ROW_MAJOR);
    query.setBuffer("a", data);
    query.setSubarray(subarray);
    // Submit query
    query.submit();
    query.close();
    array.close();
  }

  public void createSparseVarDimArray() throws TileDBError {
    // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
    Dimension<Integer> d1 =
        new Dimension<Integer>(ctx, "d1", Datatype.TILEDB_STRING_ASCII, null, null);

    // Create and set getDomain
    Domain domain = new Domain(ctx);
    domain.addDimension(d1);

    // Add two attributes "a1" and "a2", so each (i,j) cell can store
    // a character on "a1" and a vector of two floats on "a2".
    Attribute a1 = new Attribute(ctx, "a1", Integer.class);

    ArraySchema schema = new ArraySchema(ctx, TILEDB_SPARSE);
    schema.setTileOrder(TILEDB_ROW_MAJOR);
    schema.setCellOrder(TILEDB_ROW_MAJOR);
    schema.setDomain(domain);
    schema.addAttribute(a1);

    Array.create(arrayURI, schema);
  }

  public void writeSparseVarDimArray() throws TileDBError {

    NativeArray d_data = new NativeArray(ctx, "aabbccddee", Datatype.TILEDB_STRING_ASCII);
    NativeArray d_off = new NativeArray(ctx, new long[] {0, 2, 4, 6, 8}, Datatype.TILEDB_UINT64);

    // Prepare cell buffers
    NativeArray a1 = new NativeArray(ctx, new int[] {1, 2, 3, 4, 5}, Integer.class);

    // Create query
    Array array = new Array(ctx, arrayURI, TILEDB_WRITE);
    Query query = new Query(array);
    query.setLayout(TILEDB_GLOBAL_ORDER);

    query.setBuffer("d1", d_off, d_data);
    query.setBuffer("a1", a1);

    // Submit query
    query.submit();

    query.finalizeQuery();
    query.close();
    array.close();
  }
}
