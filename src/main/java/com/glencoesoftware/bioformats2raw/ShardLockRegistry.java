/**
 * Copyright (c) 2026 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import dev.zarr.zarrjava.ZarrException;

import ucar.ma2.InvalidRangeException;

/**
 * Stores bounded sets of shared lock stripes for sharded arrays that are
 * currently being written. Shards assigned to the same stripe are serialized;
 * in particular, the same shard is always assigned to the same stripe.
 */
final class ShardLockRegistry {

  /**
   * Keep enough stripes to make unrelated-shard contention unlikely while
   * bounding retained lock state independently of the number of shards.
   */
  private static final int DEFAULT_STRIPE_COUNT = 1024;

  private final ConcurrentMap<String, ConcurrentMap<Integer, Object>> locks =
    new ConcurrentHashMap<String, ConcurrentMap<Integer, Object>>();
  private final int stripeCount;
  private final Supplier<Object> lockFactory;

  ShardLockRegistry() {
    this(DEFAULT_STRIPE_COUNT, Object::new);
  }

  ShardLockRegistry(Supplier<Object> lockFactory) {
    this(DEFAULT_STRIPE_COUNT, lockFactory);
  }

  ShardLockRegistry(int stripeCount, Supplier<Object> lockFactory) {
    if (stripeCount <= 0) {
      throw new IllegalArgumentException("Stripe count must be positive");
    }
    this.stripeCount = stripeCount;
    this.lockFactory = lockFactory;
  }

  void register(String path) {
    locks.put(path, new ConcurrentHashMap<Integer, Object>());
  }

  boolean contains(String path) {
    return locks.containsKey(path);
  }

  Object getOrCreate(String path, long[] coordinates) {
    ConcurrentMap<Integer, Object> arrayLocks = locks.get(path);
    if (arrayLocks == null) {
      return null;
    }
    int stripe = Math.floorMod(Arrays.hashCode(coordinates), stripeCount);
    return arrayLocks.computeIfAbsent(
      stripe, key -> lockFactory.get());
  }

  boolean runWithLock(
    String path, long[] coordinates, ShardOperation operation)
    throws IOException, InvalidRangeException, ZarrException
  {
    Object lock = getOrCreate(path, coordinates);
    if (lock == null) {
      return false;
    }
    synchronized (lock) {
      operation.run();
    }
    return true;
  }

  void remove(String path) {
    locks.remove(path);
  }

  @FunctionalInterface
  interface ShardOperation {

    void run() throws IOException, InvalidRangeException, ZarrException;
  }
}
