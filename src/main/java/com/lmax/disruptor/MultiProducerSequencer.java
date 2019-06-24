/*
 * Copyright 2011 LMAX Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lmax.disruptor;

import java.util.concurrent.locks.LockSupport;

import sun.misc.Unsafe;

import com.lmax.disruptor.util.Util;


/**
 * <p>Coordinator for claiming sequences for access to a data structure while tracking dependent {@link Sequence}s.
 * Suitable for use for sequencing across multiple publisher threads.</p>
 *
 * <p> * Note on {@link Sequencer#getCursor()}:  With this sequencer the cursor value is updated after the call
 * to {@link Sequencer#next()}, to determine the highest available sequence that can be read, then
 * {@link Sequencer#getHighestPublishedSequence(long, long)} should be used.</p>
 */
public final class MultiProducerSequencer extends AbstractSequencer {

    private static final Unsafe UNSAFE = Util.getUnsafe();

    /**
     * 获取数组第一个元素的偏移地址
     */
    private static final long BASE = UNSAFE.arrayBaseOffset(int[].class);

    /**
     * 数组中元素的增量地址
     */
    private static final long SCALE = UNSAFE.arrayIndexScale(int[].class);

    private final Sequence gatingSequenceCache = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

    // availableBuffer tracks the state of each ringbuffer slot
    // see below for more details on the approach
    /**
     * 标记某个槽位是否可消费, 初始每个元素值为-1
     */
    private final int[] availableBuffer;

    /**
     * bufferSize-1, 用于计算某个sequence对应的数组下标; 即: 数组最大下标
     */
    private final int indexMask;

    /**
     * bufferSize为2^n, 则indexShift为n
     */
    private final int indexShift;

    /**
     * Construct a Sequencer with the selected wait strategy and buffer size.
     *
     * @param bufferSize   the size of the buffer that this will sequence over.
     * @param waitStrategy for those waiting on sequences.
     */
    public MultiProducerSequencer(int bufferSize, final WaitStrategy waitStrategy) {
        super(bufferSize, waitStrategy);
        availableBuffer = new int[bufferSize];
        indexMask = bufferSize - 1;
        indexShift = Util.log2(bufferSize);
        initialiseAvailableBuffer();
    }

    /**
     * @see Sequencer#hasAvailableCapacity(int)
     */
    @Override
    public boolean hasAvailableCapacity(final int requiredCapacity) {
        return hasAvailableCapacity(gatingSequences, requiredCapacity, cursor.get());
    }

    private boolean hasAvailableCapacity(Sequence[] gatingSequences, final int requiredCapacity, long cursorValue) {
        long wrapPoint = (cursorValue + requiredCapacity) - bufferSize;
        long cachedGatingSequence = gatingSequenceCache.get();

        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > cursorValue) {
            long minSequence = Util.getMinimumSequence(gatingSequences, cursorValue);
            gatingSequenceCache.set(minSequence);

            if (wrapPoint > minSequence) {
                return false;
            }
        }

        return true;
    }

    /**
     * 强制设定: 写游标
     *
     * @see Sequencer#claim(long)
     */
    @Override
    public void claim(long sequence) {
        cursor.set(sequence);
    }

    /**
     * @see Sequencer#next()
     */
    @Override
    public long next() {
        return next(1);
    }

    /**
     * n: 申请 slot 个数
     *
     * @see Sequencer#next(int)
     */
    @Override
    public long next(int n) {
        if (n < 1 || n > bufferSize) {
            throw new IllegalArgumentException("n must be > 0 and < bufferSize");
        }

        long current;
        long next;

        do {
            // 1.获取当前槽位
            current = cursor.get();
            // 2.申请从current + n到next范围的槽位
            next = current + n;
            // 3.wrapPoint(用来判断要申请的槽位是否已绕环一圈)
            long wrapPoint = next - bufferSize;
            // 4.上次消费者到达的最大槽位缓存
            long cachedGatingSequence = gatingSequenceCache.get();

            // 000--------------------------------032
            //        wrapPoint          nextValue     nextSequence
            // 068-------078--------090------100-101---------[110]
            // C1                    C2           C3
            // wrapPoint = 110 - 32 = 78

            // 5.1 消费者 在C1位置 wrapPoint > cacheGatingSequence, 即: 生产者申请槽位的速度 > 消费者消费槽位速度;
            // 5.2 cachedGatingSequence > current
            // - 从步骤1 -> 步骤4运行期间:
            // 5.2.1 其他生产者通过next()方法修改了 cacheGatingSequence & current
            // 5.2.2 因为cachedGatingSequence是内部成员变量缓存, 需更新为最新消费进度 即: gatingSequence
            if (wrapPoint > cachedGatingSequence || cachedGatingSequence > current) {
                // 6.此刻消费者已消费的最大Sequence
                long gatingSequence = Util.getMinimumSequence(gatingSequences, current);
                // 7.仍然表明槽位不够，则暂停一下，等待消费者消费后空出槽位。
                if (wrapPoint > gatingSequence) {
                    // TODO, should we spin based on the wait strategy?
                    LockSupport.parkNanos(1);
                    continue;
                }
                // 8.槽位够用，则更新gatingSequenceCache，防止误判
                gatingSequenceCache.set(gatingSequence);
            }
            // cas 申请 slot; 成功: 返回;失败: 重试;
            else if (cursor.compareAndSet(current, next)) {
                break;
            }
        } while (true);

        return next;
    }

    /**
     * @see Sequencer#tryNext()
     */
    @Override
    public long tryNext() throws InsufficientCapacityException {
        return tryNext(1);
    }

    /**
     * @see Sequencer#tryNext(int)
     */
    @Override
    public long tryNext(int n) throws InsufficientCapacityException {
        if (n < 1) {
            throw new IllegalArgumentException("n must be > 0");
        }

        long current;
        long next;

        do {
            current = cursor.get();
            next = current + n;

            if (!hasAvailableCapacity(gatingSequences, n, current)) {
                throw InsufficientCapacityException.INSTANCE;
            }
        }
        while (!cursor.compareAndSet(current, next));

        return next;
    }

    /**
     * 剩余容量
     *
     * @see Sequencer#remainingCapacity()
     */
    @Override
    public long remainingCapacity() {
        // gatingSequences  消费者
        // cursor   生成者
        long consumed = Util.getMinimumSequence(gatingSequences, cursor.get());
        long produced = cursor.get();

        // --------------- 64 ---------------
        // ==== 14 ------------------- 57 ===

        return getBufferSize() - (produced - consumed);
    }

    private void initialiseAvailableBuffer() {
        for (int i = availableBuffer.length - 1; i != 0; i--) {
            setAvailableBufferValue(i, -1);
        }

        setAvailableBufferValue(0, -1);
    }

    /**
     * @see Sequencer#publish(long)
     */
    @Override
    public void publish(final long sequence) {
        setAvailable(sequence);
        waitStrategy.signalAllWhenBlocking();
    }

    /**
     * @see Sequencer#publish(long, long)
     */
    @Override
    public void publish(long lo, long hi) {
        for (long l = lo; l <= hi; l++) {
            setAvailable(l);
        }
        waitStrategy.signalAllWhenBlocking();
    }

    /**
     * The below methods work on the availableBuffer flag.
     * <p>
     * The prime reason is to avoid a shared sequence object between publisher threads.
     * (Keeping single pointers tracking start and end would require coordination
     * between the threads).
     * <p>
     * --  Firstly we have the constraint that the delta between the cursor and minimum
     * gating sequence will never be larger than the buffer size (the code in
     * next/tryNext in the Sequence takes care of that).
     * -- Given that; take the sequence value and mask off the lower portion of the
     * sequence as the index into the buffer (indexMask). (aka modulo operator)
     * -- The upper portion of the sequence becomes the value to check for availability.
     * ie: it tells us how many times around the ring buffer we've been (aka division)
     * -- Because we can't wrap without the gating sequences moving forward (i.e. the
     * minimum gating sequence is effectively our last available position in the
     * buffer), when we have new data and successfully claimed a slot we can simply
     * write over the top.
     */
    private void setAvailable(final long sequence) {
        setAvailableBufferValue(calculateIndex(sequence), calculateAvailabilityFlag(sequence));
    }

    private void setAvailableBufferValue(int index, int flag) {
        long bufferAddress = (index * SCALE) + BASE;
        // putOrderedInt 提供Store/Store内存屏障, 防止这条指令和前面
        // 的指令重排序，但putOrderedInt不保证其他线程可以立刻看到写入的新值，
        //	即putOrderedInt不保证可见性
        UNSAFE.putOrderedInt(availableBuffer, bufferAddress, flag);
    }

    /**
     * @see Sequencer#isAvailable(long)
     */
    @Override
    public boolean isAvailable(long sequence) {
        int index = calculateIndex(sequence);
        int flag = calculateAvailabilityFlag(sequence);
        long bufferAddress = (index * SCALE) + BASE;
        return UNSAFE.getIntVolatile(availableBuffer, bufferAddress) == flag;
    }

    @Override
    public long getHighestPublishedSequence(long lowerBound, long availableSequence) {
        for (long sequence = lowerBound; sequence <= availableSequence; sequence++) {
            if (!isAvailable(sequence)) {
                return sequence - 1;
            }
        }

        return availableSequence;
    }

    /**
     * 计算是否可用的标志，实际上是sequence/bufferSize，使用移位运算提高执行效率
     *
     * @param sequence
     * @return 0
     */
    private int calculateAvailabilityFlag(final long sequence) {
        // return 0
        return (int) (sequence >>> indexShift);
    }

    private int calculateIndex(final long sequence) {
        return ((int) sequence) & indexMask;
    }
}
