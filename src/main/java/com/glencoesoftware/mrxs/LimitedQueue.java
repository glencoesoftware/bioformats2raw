/**
 * Copyright (c) 2019 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */

package com.glencoesoftware.mrxs;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * A limited queue which blocks when <code>offer(...)</code> is attempted
 * @see https://stackoverflow.com/questions/4521983/java-executorservice-that-blocks-on-submission-after-a-certain-queue-size
 */
public class LimitedQueue<E> extends LinkedBlockingQueue<E> 
{
    public LimitedQueue(int maxSize)
    {
        super(maxSize);
    }

    @Override
    public boolean offer(E e)
    {
        // turn offer() and add() into a blocking calls (unless interrupted)
        try {
            put(e);
            return true;
        } catch(InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

}
