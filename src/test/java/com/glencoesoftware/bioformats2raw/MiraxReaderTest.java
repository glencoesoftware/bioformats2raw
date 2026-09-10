package com.glencoesoftware.bioformats2raw;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

import loci.common.ByteArrayHandle;
import loci.common.IniList;
import loci.common.IniTable;
import loci.common.RandomAccessInputStream;
import loci.formats.CoreMetadata;
import loci.formats.FormatReader;

public class MiraxReaderTest {

  @Test
  public void testSlideHierarchyIndexFindsFilterLevelAfterFocusLevel()
    throws Exception
  {
    IniTable hierarchy = new IniTable();
    hierarchy.put("HIER_COUNT", "4");
    hierarchy.put("HIER_0_NAME", "Slide zoom level");
    hierarchy.put("HIER_1_NAME", "Microscope focus level");
    hierarchy.put("HIER_2_NAME", "Slide filter level");
    hierarchy.put("HIER_2_COUNT", "4");
    hierarchy.put("HIER_3_NAME", "Ignored hierarchy");

    MiraxReader reader = new MiraxReader();
    Method method = MiraxReader.class.getDeclaredMethod(
      "getSlideHierarchyIndex", IniTable.class);
    method.setAccessible(true);

    assertEquals(2, method.invoke(reader, hierarchy));
  }

  @Test
  public void testChannelCountUsesResolvedFilterHierarchy() throws Exception {
    IniTable hierarchy = new IniTable();
    hierarchy.put("HIER_COUNT", "4");
    hierarchy.put("HIER_0_NAME", "Slide zoom level");
    hierarchy.put("HIER_1_NAME", "Microscope focus level");
    hierarchy.put("HIER_2_NAME", "Slide filter level");
    hierarchy.put("HIER_2_COUNT", "4");
    hierarchy.put("HIER_3_NAME", "Ignored hierarchy");

    MiraxReader reader = new MiraxReader();
    Method method = MiraxReader.class.getDeclaredMethod(
      "getChannelCount", IniTable.class);
    method.setAccessible(true);

    assertEquals(4, method.invoke(reader, hierarchy));
  }

  @Test
  public void testChannelCountDefaultsWithoutFilterHierarchy()
    throws Exception
  {
    IniTable hierarchy = new IniTable();
    hierarchy.put("HIER_COUNT", "1");
    hierarchy.put("HIER_0_NAME", "Slide zoom level");

    MiraxReader reader = new MiraxReader();
    Method method = MiraxReader.class.getDeclaredMethod(
      "getChannelCount", IniTable.class);
    method.setAccessible(true);

    assertEquals(1, method.invoke(reader, hierarchy));
  }

  @Test
  public void testFocusCountExcludesNonZStackLevels() {
    IniTable hierarchy = new IniTable();
    hierarchy.put("HIER_COUNT", "3");
    hierarchy.put("HIER_0_NAME", "Slide zoom level");
    hierarchy.put("HIER_1_NAME", "Slide filter level");
    hierarchy.put("HIER_2_NAME", "Microscope focus level");
    hierarchy.put("HIER_2_COUNT", "26");
    hierarchy.put("HIER_2_VAL_0", "ExtFocusLevel");
    for (int i=1; i<26; i++) {
      hierarchy.put("HIER_2_VAL_" + i, "ZStackLevel_(" + (i - 13) + ")");
    }

    MiraxReader reader = new MiraxReader();
    assertEquals(25, reader.getFocusCount(hierarchy));
  }

  @Test
  public void testMissingFocusHierarchyDefaultsToSinglePlane() {
    IniTable hierarchy = new IniTable();
    hierarchy.put("HIER_COUNT", "2");
    hierarchy.put("HIER_0_NAME", "Slide zoom level");
    hierarchy.put("HIER_1_NAME", "Slide filter level");

    MiraxReader reader = new MiraxReader();
    assertEquals(1, reader.getFocusCount(hierarchy));
  }

  @Test
  public void testInitializeMainSeriesKeepsFormatAlignedWithCore()
    throws Exception
  {
    MiraxReader reader = new MiraxReader();
    Field coreField = FormatReader.class.getDeclaredField("core");
    coreField.setAccessible(true);
    coreField.set(reader, new ArrayList<CoreMetadata>());
    reader.initializeMainSeries(10);

    @SuppressWarnings("unchecked")
    List<String> format = (List<String>) getField(reader, "format");
    @SuppressWarnings("unchecked")
    List<CoreMetadata> core = (List<CoreMetadata>) coreField.get(reader);

    assertEquals(10, format.size());
    assertEquals(10, core.size());
    assertTrue(format.stream().allMatch(String::isEmpty));
  }

  @Test
  public void testTileGroupWithVariousChannelCounts() {
    // sizeC=1: single channel, one group per Z
    assertEquals(0, MiraxReader.getTileGroup(0, 0, 1));
    assertEquals(1, MiraxReader.getTileGroup(1, 0, 1));
    // sizeC=3: exactly MAX_CHANNELS, all channels in one group
    assertEquals(0, MiraxReader.getTileGroup(0, 0, 3));
    assertEquals(0, MiraxReader.getTileGroup(0, 2, 3));
    assertEquals(1, MiraxReader.getTileGroup(1, 0, 3));
    // sizeC=7: two full groups + one partial = 3 groups per Z
    assertEquals(0, MiraxReader.getTileGroup(0, 0, 7));
    assertEquals(1, MiraxReader.getTileGroup(0, 3, 7));
    assertEquals(2, MiraxReader.getTileGroup(0, 6, 7));
    assertEquals(3, MiraxReader.getTileGroup(1, 0, 7));
  }

  @Test
  public void testStoredChannelHandlesPartialJpeg2000Groups()
    throws Exception
  {
    MiraxReader reader = new MiraxReader();
    setField(reader, "fluorescence", true);
    setField(reader, "format", java.util.Arrays.asList("JPEG2000"));
    setSuperclassField(reader, "currentId", "synthetic");
    java.util.List<CoreMetadata> core = new java.util.ArrayList<>();
    CoreMetadata metadata = new CoreMetadata();
    metadata.sizeC = 4;
    core.add(metadata);
    setSuperclassField(reader, "core", core);

    assertEquals(2, reader.getStoredChannel(0, 0));
    assertEquals(1, reader.getStoredChannel(0, 1));
    assertEquals(0, reader.getStoredChannel(0, 2));
    assertEquals(0, reader.getStoredChannel(0, 3));

    metadata.sizeC = 5;
    assertEquals(2, reader.getStoredChannel(0, 0));
    assertEquals(1, reader.getStoredChannel(0, 1));
    assertEquals(0, reader.getStoredChannel(0, 2));
    assertEquals(0, reader.getStoredChannel(0, 3));
    assertEquals(1, reader.getStoredChannel(0, 4));
  }

  @Test
  public void testStoredChannelKeepsTwoChannelJpeg2000Order()
    throws Exception
  {
    MiraxReader reader = new MiraxReader();
    setField(reader, "fluorescence", true);
    setField(reader, "format", java.util.Arrays.asList("JPEG2000"));
    setSuperclassField(reader, "currentId", "synthetic");
    java.util.List<CoreMetadata> core = new java.util.ArrayList<>();
    CoreMetadata metadata = new CoreMetadata();
    metadata.sizeC = 2;
    core.add(metadata);
    setSuperclassField(reader, "core", core);

    assertEquals(0, reader.getStoredChannel(0, 0));
    assertEquals(1, reader.getStoredChannel(0, 1));
  }

  @Test
  public void testSingleLayerStoredChannelKeepsOriginalNonStackBehavior()
    throws Exception
  {
    MiraxReader reader = new MiraxReader();
    setField(reader, "fluorescence", true);
    setField(reader, "format", java.util.Arrays.asList("JPEG"));
    setSuperclassField(reader, "currentId", "synthetic");

    java.util.List<CoreMetadata> core = new java.util.ArrayList<>();
    CoreMetadata metadata = new CoreMetadata();
    metadata.sizeC = 5;
    core.add(metadata);
    setSuperclassField(reader, "core", core);

    Method method = MiraxReader.class.getDeclaredMethod(
      "getSingleLayerStoredChannel", int.class, int.class);
    method.setAccessible(true);

    assertEquals(2, method.invoke(reader, 0, 0));
    assertEquals(1, method.invoke(reader, 0, 1));
    assertEquals(0, method.invoke(reader, 0, 2));
    assertEquals(2, method.invoke(reader, 0, 3));
    assertEquals(1, method.invoke(reader, 0, 4));
  }

  @Test
  public void testLookupTileSkipsExtraFluorescenceTile() throws Exception {
    MiraxReader reader = new MiraxReader();
    setField(reader, "fluorescence", true);
    setField(reader, "offsets", new TreeMap<>());
    setField(reader, "pyramidDepth", 1);
    setField(reader, "minRowIndex", new int[] {0});
    setField(reader, "minColIndex", new int[] {0});
    setField(reader, "xTiles", 1);
    setSuperclassField(reader, "currentId", "synthetic");
    java.util.List<CoreMetadata> core = new java.util.ArrayList<>();
    CoreMetadata metadata = new CoreMetadata();
    metadata.sizeC = 4;
    core.add(metadata);
    setSuperclassField(reader, "core", core);

    @SuppressWarnings("unchecked")
    TreeMap<MiraxReader.TilePointer, List<MiraxReader.TilePointer>> offsets =
      (TreeMap<MiraxReader.TilePointer, List<MiraxReader.TilePointer>>) getField(
        reader, "offsets");

    MiraxReader.TilePointer key = reader.new TilePointer(0, 0);
    List<MiraxReader.TilePointer> tiles = new java.util.ArrayList<>();
    tiles.add(reader.new TilePointer(0, 1, 10, 0, 5));
    tiles.add(reader.new TilePointer(0, 1, 20, 0, 5));
    tiles.add(reader.new TilePointer(0, 1, 30, 0, 5));
    offsets.put(key, tiles);

    Method method = MiraxReader.class.getDeclaredMethod(
      "lookupTile", int.class, int.class, int.class, int.class);
    method.setAccessible(true);

    MiraxReader.TilePointer tile =
      (MiraxReader.TilePointer) method.invoke(reader, 0, 0, 0, 1);
    assertNotNull(tile);
    assertEquals(30, tile.offset);
  }

  @Test
  public void testLookupTileForPlaneRespectsCounterLookupFlag()
    throws Exception
  {
    MiraxReader reader = new MiraxReader();
    setField(reader, "offsets", new TreeMap<>());
    setField(reader, "pyramidDepth", 1);
    setField(reader, "minRowIndex", new int[] {0});
    setField(reader, "minColIndex", new int[] {0});
    setField(reader, "xTiles", 10);
    setSuperclassField(reader, "currentId", "synthetic");

    java.util.List<CoreMetadata> core = new java.util.ArrayList<>();
    CoreMetadata metadata = new CoreMetadata();
    metadata.sizeC = 3;
    core.add(metadata);
    setSuperclassField(reader, "core", core);

    @SuppressWarnings("unchecked")
    TreeMap<MiraxReader.TilePointer, List<MiraxReader.TilePointer>> offsets =
      (TreeMap<MiraxReader.TilePointer, List<MiraxReader.TilePointer>>) getField(
        reader, "offsets");

    List<MiraxReader.TilePointer> singleLayerTile = new java.util.ArrayList<>();
    singleLayerTile.add(reader.new TilePointer(0, 1, 10, 32, 5));
    offsets.put(reader.new TilePointer(0, 32), singleLayerTile);

    List<MiraxReader.TilePointer> tilePositionTile = new java.util.ArrayList<>();
    tilePositionTile.add(reader.new TilePointer(0, 1, 20, 999, 5));
    offsets.put(reader.new TilePointer(0, 999), tilePositionTile);

    setField(reader, "useTilePositionCounterLookup", false);
    MiraxReader.TilePointer tile =
      reader.lookupTileForPlane(0, 2, 3, 0, 0, 999);
    assertNotNull(tile);
    assertEquals(10, tile.offset);

    setField(reader, "useTilePositionCounterLookup", true);
    tile = reader.lookupTileForPlane(0, 2, 3, 0, 0, 999);
    assertNotNull(tile);
    assertEquals(20, tile.offset);
  }

  @Test
  public void testStoredFocusCountHandlesNullCount() {
    // HIER_1_COUNT is absent; should return 1, not throw NumberFormatException
    IniTable hierarchy = new IniTable();
    hierarchy.put("HIER_COUNT", "2");
    hierarchy.put("HIER_0_NAME", "Slide zoom level");
    hierarchy.put("HIER_1_NAME", "Microscope focus level");

    MiraxReader reader = new MiraxReader();
    assertEquals(1, reader.getStoredFocusCount(hierarchy));
  }

  @Test
  public void testFocusStepMicrometersUsesZStackLevelsOnly() {
    IniTable hierarchy = new IniTable();
    hierarchy.put("HIER_COUNT", "3");
    hierarchy.put("HIER_0_NAME", "Slide zoom level");
    hierarchy.put("HIER_1_NAME", "Slide filter level");
    hierarchy.put("HIER_2_NAME", "Microscope focus level");
    hierarchy.put("HIER_2_COUNT", "4");
    hierarchy.put("HIER_2_VAL_0", "ExtFocusLevel");
    hierarchy.put("HIER_2_VAL_0_SECTION", "LAYER_2_LEVEL_0_SECTION");
    hierarchy.put("HIER_2_VAL_1", "ZStackLevel_(-1)");
    hierarchy.put("HIER_2_VAL_1_SECTION", "LAYER_2_LEVEL_1_SECTION");
    hierarchy.put("HIER_2_VAL_2", "ZStackLevel_(0)");
    hierarchy.put("HIER_2_VAL_2_SECTION", "LAYER_2_LEVEL_2_SECTION");
    hierarchy.put("HIER_2_VAL_3", "ZStackLevel_(+1)");
    hierarchy.put("HIER_2_VAL_3_SECTION", "LAYER_2_LEVEL_3_SECTION");

    IniList data = new IniList();
    IniTable ext = new IniTable();
    ext.put(IniTable.HEADER_KEY, "LAYER_2_LEVEL_0_SECTION");
    ext.put("OFFSET_IN_MICROMETERS", "0");
    data.add(ext);
    IniTable z1 = new IniTable();
    z1.put(IniTable.HEADER_KEY, "LAYER_2_LEVEL_1_SECTION");
    z1.put("OFFSET_IN_MICROMETERS", "-2");
    data.add(z1);
    IniTable z2 = new IniTable();
    z2.put(IniTable.HEADER_KEY, "LAYER_2_LEVEL_2_SECTION");
    z2.put("OFFSET_IN_MICROMETERS", "0");
    data.add(z2);
    IniTable z3 = new IniTable();
    z3.put(IniTable.HEADER_KEY, "LAYER_2_LEVEL_3_SECTION");
    z3.put("OFFSET_IN_MICROMETERS", "2");
    data.add(z3);

    MiraxReader reader = new MiraxReader();
    assertEquals(2.0, reader.getFocusStepMicrometers(hierarchy, data));
  }

  @Test
  public void testFocusStepMicrometersDefaultsToNullWithoutStack() {
    IniTable hierarchy = new IniTable();
    hierarchy.put("HIER_COUNT", "2");
    hierarchy.put("HIER_0_NAME", "Slide zoom level");
    hierarchy.put("HIER_1_NAME", "Slide filter level");

    MiraxReader reader = new MiraxReader();
    assertNull(reader.getFocusStepMicrometers(hierarchy, new IniList()));
  }

  @Test
  public void testParseFocusBlocksParsesSingleChannelGroup() throws Exception {
    IniTable hierarchy = new IniTable();
    hierarchy.put("HIER_COUNT", "3");
    hierarchy.put("HIER_0_NAME", "Slide zoom level");
    hierarchy.put("HIER_1_NAME", "Slide filter level");
    hierarchy.put("HIER_1_COUNT", "3");
    hierarchy.put("HIER_2_NAME", "Microscope focus level");
    hierarchy.put("HIER_2_COUNT", "2");
    hierarchy.put("HIER_2_VAL_0", "ZStackLevel_(-1)");
    hierarchy.put("HIER_2_VAL_1", "ZStackLevel_(0)");

    MiraxReader reader = new MiraxReader();
    setField(reader, "fluorescence", true);
    setField(reader, "storedFocusLevels", 2);
    setField(reader, "pyramidDepth", 1);
    setField(reader, "offsets", new java.util.TreeMap<>());

    byte[] bytes = createSingleGroupFocusBlockIndex();
    try (RandomAccessInputStream indexData =
      new RandomAccessInputStream(new ByteArrayHandle(bytes)))
    {
      indexData.order(true);
      Method method = MiraxReader.class.getDeclaredMethod(
        "parseFocusBlocks", RandomAccessInputStream.class, long[].class,
        IniTable.class);
      method.setAccessible(true);

      boolean parsed = (Boolean) method.invoke(
        reader, indexData, new long[] {8L}, hierarchy);
      assertTrue(parsed);
    }

    SortedMap<?, ?> offsets =
      (SortedMap<?, ?>) getField(reader, "offsets");
    assertEquals(2, offsets.size());

    List<?> firstPlane = (List<?>) offsets.get(reader.new TilePointer(0, 10));
    assertNotNull(firstPlane);
    assertEquals(1, firstPlane.size());
    MiraxReader.TilePointer firstPointer =
      (MiraxReader.TilePointer) firstPlane.get(0);
    assertEquals(0, firstPointer.sourceValue);
    assertEquals(100, firstPointer.offset);

    List<?> secondPlane = (List<?>) offsets.get(reader.new TilePointer(0, 20));
    assertNotNull(secondPlane);
    assertEquals(1, secondPlane.size());
    MiraxReader.TilePointer secondPointer =
      (MiraxReader.TilePointer) secondPlane.get(0);
    assertEquals(1, secondPointer.sourceValue);
    assertEquals(200, secondPointer.offset);
  }

  @Test
  public void testParseFocusBlocksSkipsInterleavedNonZLevels()
    throws Exception
  {
    IniTable hierarchy = new IniTable();
    hierarchy.put("HIER_COUNT", "3");
    hierarchy.put("HIER_0_NAME", "Slide zoom level");
    hierarchy.put("HIER_1_NAME", "Slide filter level");
    hierarchy.put("HIER_1_COUNT", "3");
    hierarchy.put("HIER_2_NAME", "Microscope focus level");
    hierarchy.put("HIER_2_COUNT", "4");
    hierarchy.put("HIER_2_VAL_0", "ExtFocusLevel");
    hierarchy.put("HIER_2_VAL_1", "ZStackLevel_(-1)");
    hierarchy.put("HIER_2_VAL_2", "DerivedFocusLevel");
    hierarchy.put("HIER_2_VAL_3", "ZStackLevel_(0)");

    MiraxReader reader = new MiraxReader();
    setField(reader, "fluorescence", true);
    setField(reader, "storedFocusLevels", 4);
    setField(reader, "pyramidDepth", 1);
    setField(reader, "offsets", new java.util.TreeMap<>());

    byte[] bytes = createInterleavedFocusBlockIndex();
    try (RandomAccessInputStream indexData =
      new RandomAccessInputStream(new ByteArrayHandle(bytes)))
    {
      indexData.order(true);
      Method method = MiraxReader.class.getDeclaredMethod(
        "parseFocusBlocks", RandomAccessInputStream.class, long[].class,
        IniTable.class);
      method.setAccessible(true);

      boolean parsed = (Boolean) method.invoke(
        reader, indexData, new long[] {8L}, hierarchy);
      assertTrue(parsed);
    }

    @SuppressWarnings("unchecked")
    SortedMap<MiraxReader.TilePointer, List<MiraxReader.TilePointer>> offsets =
      (SortedMap<MiraxReader.TilePointer, List<MiraxReader.TilePointer>>) getField(
        reader, "offsets");
    assertEquals(2, offsets.size());

    List<MiraxReader.TilePointer> firstPlane =
      offsets.get(reader.new TilePointer(0, 10));
    assertNotNull(firstPlane);
    assertEquals(0, firstPlane.get(0).sourceValue);
    assertEquals(100, firstPlane.get(0).offset);

    List<MiraxReader.TilePointer> secondPlane =
      offsets.get(reader.new TilePointer(0, 20));
    assertNotNull(secondPlane);
    assertEquals(1, secondPlane.get(0).sourceValue);
    assertEquals(200, secondPlane.get(0).offset);

    assertNull(offsets.get(reader.new TilePointer(0, 15)));
  }

  @Test
  public void testParseSingleLayerRootOffsetsSkipsNonZeroResolutions()
    throws Exception
  {
    MiraxReader reader = new MiraxReader();
    setField(reader, "pyramidDepth", 2);
    setField(reader, "offsets", new TreeMap<>());

    byte[] bytes = createSingleLayerRootIndex();
    try (RandomAccessInputStream indexData =
      new RandomAccessInputStream(new ByteArrayHandle(bytes)))
    {
      indexData.order(true);
      Method method = MiraxReader.class.getDeclaredMethod(
        "parseSingleLayerRootOffsets", RandomAccessInputStream.class, long.class,
        int.class);
      method.setAccessible(true);
      method.invoke(reader, indexData, 8L, 1);
    }

    @SuppressWarnings("unchecked")
    SortedMap<MiraxReader.TilePointer, List<MiraxReader.TilePointer>> offsets =
      (SortedMap<MiraxReader.TilePointer, List<MiraxReader.TilePointer>>) getField(
        reader, "offsets");
    assertEquals(1, offsets.size());

    List<MiraxReader.TilePointer> tiles =
      offsets.get(reader.new TilePointer(0, 10));
    assertNotNull(tiles);
    assertEquals(1, tiles.size());
    assertEquals(100, tiles.get(0).offset);
    assertEquals(0, tiles.get(0).sourceValue);

    assertNull(offsets.get(reader.new TilePointer(1, 20)));
  }

  @Test
  public void testTilePointerCompareToReturnsZeroForEqualPointers() {
    MiraxReader reader = new MiraxReader();

    MiraxReader.TilePointer left =
      reader.new TilePointer(0, 1, 100, 10, 5, 2);
    MiraxReader.TilePointer right =
      reader.new TilePointer(0, 1, 100, 10, 5, 2);

    assertEquals(0, left.compareTo(right));
    assertEquals(0, right.compareTo(left));
    assertEquals(left, right);
  }

  @Test
  public void testTilePointerCompareToOrdersByLength() {
    MiraxReader reader = new MiraxReader();

    MiraxReader.TilePointer shorter =
      reader.new TilePointer(0, 1, 100, 10, 5, 2);
    MiraxReader.TilePointer longer =
      reader.new TilePointer(0, 1, 100, 10, 6, 2);

    assertTrue(shorter.compareTo(longer) < 0);
    assertTrue(longer.compareTo(shorter) > 0);
  }

  private static void setField(Object target, String fieldName, Object value)
    throws Exception
  {
    Field field = MiraxReader.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static Object getField(Object target, String fieldName)
    throws Exception
  {
    Field field = MiraxReader.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(target);
  }

  private static void setSuperclassField(Object target, String fieldName,
    Object value) throws Exception
  {
    Class<?> type = target.getClass().getSuperclass();
    while (type != null) {
      try {
        Field field = type.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
        return;
      }
      catch (NoSuchFieldException e) {
        type = type.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName);
  }

  private static byte[] createSingleGroupFocusBlockIndex() {
    ByteBuffer buffer = ByteBuffer.allocate(128).order(ByteOrder.LITTLE_ENDIAN);

    buffer.position(8);
    buffer.putInt(0);
    buffer.putInt(24);
    buffer.putInt(0);
    buffer.putInt(52);

    buffer.position(24);
    buffer.putInt(1);
    buffer.putInt(0);
    buffer.putInt(10);
    buffer.putInt(100);
    buffer.putInt(11);
    buffer.putInt(2);
    buffer.putInt(11);

    buffer.position(52);
    buffer.putInt(1);
    buffer.putInt(0);
    buffer.putInt(20);
    buffer.putInt(200);
    buffer.putInt(22);
    buffer.putInt(3);
    buffer.putInt(21);

    return buffer.array();
  }

  private static byte[] createSingleLayerRootIndex() {
    ByteBuffer buffer = ByteBuffer.allocate(128).order(ByteOrder.LITTLE_ENDIAN);

    buffer.position(8);
    buffer.putInt(32);
    buffer.putInt(64);

    buffer.position(32);
    buffer.putInt(0);
    buffer.putInt(48);

    buffer.position(48);
    buffer.putInt(1);
    buffer.putInt(0);
    buffer.putInt(10);
    buffer.putInt(100);
    buffer.putInt(5);
    buffer.putInt(1);
    buffer.putInt(11);

    buffer.position(64);
    buffer.putInt(0);
    buffer.putInt(80);

    buffer.position(80);
    buffer.putInt(1);
    buffer.putInt(0);
    buffer.putInt(20);
    buffer.putInt(200);
    buffer.putInt(5);
    buffer.putInt(2);
    buffer.putInt(21);

    return buffer.array();
  }

  private static byte[] createInterleavedFocusBlockIndex() {
    ByteBuffer buffer = ByteBuffer.allocate(192).order(ByteOrder.LITTLE_ENDIAN);

    buffer.position(8);
    buffer.putInt(0);
    buffer.putInt(40);
    buffer.putInt(0);
    buffer.putInt(72);
    buffer.putInt(0);
    buffer.putInt(104);
    buffer.putInt(0);
    buffer.putInt(136);

    buffer.position(40);
    buffer.putInt(1);
    buffer.putInt(0);
    buffer.putInt(999);
    buffer.putInt(900);
    buffer.putInt(11);
    buffer.putInt(1);
    buffer.putInt(1000);

    buffer.position(72);
    buffer.putInt(1);
    buffer.putInt(0);
    buffer.putInt(10);
    buffer.putInt(100);
    buffer.putInt(11);
    buffer.putInt(1);
    buffer.putInt(11);

    buffer.position(104);
    buffer.putInt(1);
    buffer.putInt(0);
    buffer.putInt(888);
    buffer.putInt(800);
    buffer.putInt(22);
    buffer.putInt(1);
    buffer.putInt(889);

    buffer.position(136);
    buffer.putInt(1);
    buffer.putInt(0);
    buffer.putInt(20);
    buffer.putInt(200);
    buffer.putInt(22);
    buffer.putInt(1);
    buffer.putInt(21);

    return buffer.array();
  }
}
