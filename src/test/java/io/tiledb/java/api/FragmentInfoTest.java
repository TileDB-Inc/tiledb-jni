package io.tiledb.java.api;

import static io.tiledb.java.api.ArrayType.TILEDB_DENSE;
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
  private String arrayURI = "fragments_consolidation";

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
    arrayCreateDense();

    // Write three fragments
    for (int i = 0; i < testFragmentCount; ++i) arrayWriteDense();

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

  public void arrayCreateDense() throws Exception {

    // Create getDimensions
    Dimension<Integer> rows =
        new Dimension<Integer>(ctx, "rows", Integer.class, new Pair<Integer, Integer>(1, 4), 2);
    Dimension<Integer> cols =
        new Dimension<Integer>(ctx, "cols", Integer.class, new Pair<Integer, Integer>(1, 4), 2);

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

  public void arrayWriteDense() throws Exception {
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
}
