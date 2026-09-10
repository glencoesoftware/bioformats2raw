/**
 * Copyright (c) 2026 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.codec.Codec;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShardLockRegistryTest {

  private static final String ARRAY_PATH = "0/0";
  private static final long[] SHARD_COORDINATES = {0, 0, 0, 1, 2};

  /**
   * Test that concurrent requests for the same shard atomically create one
   * shared lock.
   */
  @Test
  public void testAtomicLockCreation() throws Exception {
    int workerCount = 32;
    AtomicInteger locksCreated = new AtomicInteger();
    ShardLockRegistry registry =
      new ShardLockRegistry(() -> {
        locksCreated.incrementAndGet();
        return new Object();
      });
    registry.register(ARRAY_PATH);

    ExecutorService executor = Executors.newFixedThreadPool(workerCount);
    CountDownLatch ready = new CountDownLatch(workerCount);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<Object>> locks = new ArrayList<Future<Object>>();
    try {
      for (int i=0; i<workerCount; i++) {
        locks.add(executor.submit(() -> {
          ready.countDown();
          start.await();
          return registry.getOrCreate(ARRAY_PATH, SHARD_COORDINATES);
        }));
      }

      assertTrue(ready.await(10, TimeUnit.SECONDS));
      start.countDown();

      Object expected = locks.get(0).get(10, TimeUnit.SECONDS);
      for (Future<Object> lock : locks) {
        assertSame(expected, lock.get(10, TimeUnit.SECONDS));
      }
      assertEquals(1, locksCreated.get());
    }
    finally {
      executor.shutdownNow();
    }
  }

  /**
   * Test that touching more shards than there are lock stripes does not retain
   * additional locks.
   */
  @Test
  public void testLockCountIsBounded() {
    int stripeCount = 8;
    AtomicInteger locksCreated = new AtomicInteger();
    ShardLockRegistry registry =
      new ShardLockRegistry(stripeCount, () -> {
        locksCreated.incrementAndGet();
        return new Object();
      });
    registry.register(ARRAY_PATH);

    for (int shard=0; shard<1000; shard++) {
      registry.getOrCreate(ARRAY_PATH, new long[] {shard});
    }

    assertEquals(stripeCount, locksCreated.get());
  }

  /**
   * Test that two operations targeting the same shard cannot enter their
   * synchronized sections concurrently.
   */
  @Test
  public void testSameShardWritesAreSerialized() throws Exception {
    ShardLockRegistry registry = new ShardLockRegistry();
    registry.register(ARRAY_PATH);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch firstWriteEntered = new CountDownLatch(1);
    CountDownLatch releaseFirstWrite = new CountDownLatch(1);
    CountDownLatch attemptingWrite = new CountDownLatch(1);
    CountDownLatch enteredWrite = new CountDownLatch(1);
    AtomicReference<Thread> contender = new AtomicReference<Thread>();
    try {
      Future<?> firstWrite = executor.submit(() ->
        registry.runWithLock(ARRAY_PATH, SHARD_COORDINATES, () -> {
          firstWriteEntered.countDown();
          try {
            releaseFirstWrite.await();
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }));
      assertTrue(firstWriteEntered.await(10, TimeUnit.SECONDS));

      Future<Boolean> secondWrite = executor.submit(() -> {
        contender.set(Thread.currentThread());
        attemptingWrite.countDown();
        return registry.runWithLock(ARRAY_PATH, SHARD_COORDINATES, () ->
          enteredWrite.countDown());
      });

      assertTrue(attemptingWrite.await(10, TimeUnit.SECONDS));
      long deadline =
        System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (contender.get().getState() != Thread.State.BLOCKED &&
        enteredWrite.getCount() != 0 && System.nanoTime() < deadline)
      {
        Thread.yield();
      }
      assertEquals(Thread.State.BLOCKED, contender.get().getState());
      assertEquals(1, enteredWrite.getCount());

      releaseFirstWrite.countDown();
      assertTrue(enteredWrite.await(10, TimeUnit.SECONDS));
      firstWrite.get(10, TimeUnit.SECONDS);
      secondWrite.get(10, TimeUnit.SECONDS);
    }
    finally {
      executor.shutdownNow();
    }
  }

  /**
   * Test that a completed resolution removes its shard locks.
   * @param temporaryDirectory temporary conversion output directory
   */
  @Test
  public void testLocksRemovedAfterResolution(@TempDir Path temporaryDirectory)
    throws Exception
  {
    Path input = Paths.get("image&sizeX=64&sizeY=64.fake");
    Path output = temporaryDirectory.resolve("output");
    AtomicInteger locksCreated = new AtomicInteger();
    ShardLockRegistry registry =
      new ShardLockRegistry(() -> {
        locksCreated.incrementAndGet();
        return new Object();
      });
    Converter converter = new Converter(registry);
    CommandLine.call(converter,
      "--ngff-version", "0.5",
      "--resolutions", "1",
      "--max-workers", "2",
      "--tile-width", "32",
      "--tile-height", "32",
      "--shard-width", "64",
      "--shard-height", "64",
      input.toString(), output.toString());

    FilesystemStore store = new FilesystemStore(output);
    Array array = Array.open(store.resolve("0", "0"));
    Optional<Codec> shardingCodec =
      ArrayMetadata.getShardingIndexedCodec(array.metadata().codecs);
    assertTrue(shardingCodec.isPresent());

    assertEquals(1, locksCreated.get());
    assertFalse(registry.contains(ARRAY_PATH));
  }
}
