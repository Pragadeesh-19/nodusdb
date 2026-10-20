package io.nodusdb.kernel.memory;

import org.graalvm.word.Pointer;
import org.graalvm.word.WordFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.ref.Reference;
import java.nio.ByteOrder;

public final class NativeMemory {

    public static final long NOT_NATIVE = 0;

    private static final String IMAGE_CODE_PROPERTY = "org.graalvm.nativeimage.imagecode";
    private static final boolean POINTERS = System.getProperty(IMAGE_CODE_PROPERTY) != null;

    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;
    private static final ValueLayout.OfLong LONG_UNALIGNED = ValueLayout.JAVA_LONG_UNALIGNED;
    private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT;
    private static final ValueLayout.OfInt INT_UNALIGNED = ValueLayout.JAVA_INT_UNALIGNED;
    private static final ValueLayout.OfInt INT_LITTLE_ENDIAN = INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LONG_LITTLE_ENDIAN = LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;
    private static final boolean LITTLE_ENDIAN_PLATFORM = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

    private NativeMemory() {
    }

    public static boolean usesPointers() {
        return POINTERS;
    }

    public static long address(MemorySegment segment) {
        if (POINTERS && segment.isNative() && !segment.isReadOnly() && segment.scope().isAlive()) {
            return segment.address();
        }
        return NOT_NATIVE;
    }

    public static long readableAddress(MemorySegment segment) {
        if (POINTERS && segment.isNative() && segment.scope().isAlive()) {
            return segment.address();
        }
        return NOT_NATIVE;
    }

    public static long getLong(MemorySegment segment, long address, long size, long offset) {
        if (POINTERS && address != NOT_NATIVE) {
            if (offset < 0 || offset > size - Long.BYTES) {
                throw outOfBounds(offset, Long.BYTES, size);
            }
            long value = WordFactory.<Pointer>pointer(address + offset).readLong(0);
            Reference.reachabilityFence(segment);
            return value;
        }
        return Slow.getLong(segment, offset);
    }

    public static void setLong(MemorySegment segment, long address, long size, long offset, long value) {
        if (POINTERS && address != NOT_NATIVE) {
            if (offset < 0 || offset > size - Long.BYTES) {
                throw outOfBounds(offset, Long.BYTES, size);
            }
            WordFactory.<Pointer>pointer(address + offset).writeLong(0, value);
            Reference.reachabilityFence(segment);
            return;
        }
        Slow.setLong(segment, offset, value);
    }

    public static long getLongUnaligned(MemorySegment segment, long address, long size, long offset) {
        if (POINTERS && address != NOT_NATIVE) {
            if (offset < 0 || offset > size - Long.BYTES) {
                throw outOfBounds(offset, Long.BYTES, size);
            }
            long value = WordFactory.<Pointer>pointer(address + offset).readLong(0);
            Reference.reachabilityFence(segment);
            return value;
        }
        return Slow.getLongUnaligned(segment, offset);
    }

    public static void setLongUnaligned(MemorySegment segment, long address, long size, long offset, long value) {
        if (POINTERS && address != NOT_NATIVE) {
            if (offset < 0 || offset > size - Long.BYTES) {
                throw outOfBounds(offset, Long.BYTES, size);
            }
            WordFactory.<Pointer>pointer(address + offset).writeLong(0, value);
            Reference.reachabilityFence(segment);
            return;
        }
        Slow.setLongUnaligned(segment, offset, value);
    }

    public static int getInt(MemorySegment segment, long address, long size, long offset) {
        if (POINTERS && address != NOT_NATIVE) {
            if (offset < 0 || offset > size - Integer.BYTES) {
                throw outOfBounds(offset, Integer.BYTES, size);
            }
            int value = WordFactory.<Pointer>pointer(address + offset).readInt(0);
            Reference.reachabilityFence(segment);
            return value;
        }
        return Slow.getInt(segment, offset);
    }

    public static void setInt(MemorySegment segment, long address, long size, long offset, int value) {
        if (POINTERS && address != NOT_NATIVE) {
            if (offset < 0 || offset > size - Integer.BYTES) {
                throw outOfBounds(offset, Integer.BYTES, size);
            }
            WordFactory.<Pointer>pointer(address + offset).writeInt(0, value);
            Reference.reachabilityFence(segment);
            return;
        }
        Slow.setInt(segment, offset, value);
    }

    public static int getIntUnaligned(MemorySegment segment, long address, long size, long offset) {
        if (POINTERS && address != NOT_NATIVE) {
            if (offset < 0 || offset > size - Integer.BYTES) {
                throw outOfBounds(offset, Integer.BYTES, size);
            }
            int value = WordFactory.<Pointer>pointer(address + offset).readInt(0);
            Reference.reachabilityFence(segment);
            return value;
        }
        return Slow.getIntUnaligned(segment, offset);
    }

    public static int getIntLittleEndian(MemorySegment segment, long address, long size, long offset) {
        if (POINTERS && address != NOT_NATIVE) {
            if (offset < 0 || offset > size - Integer.BYTES) {
                throw outOfBounds(offset, Integer.BYTES, size);
            }
            int value = WordFactory.<Pointer>pointer(address + offset).readInt(0);
            Reference.reachabilityFence(segment);
            return LITTLE_ENDIAN_PLATFORM ? value : Integer.reverseBytes(value);
        }
        return Slow.getIntLittleEndian(segment, offset);
    }

    public static void setIntLittleEndian(MemorySegment segment, long address, long size, long offset, int value) {
        if (POINTERS && address != NOT_NATIVE) {
            if (offset < 0 || offset > size - Integer.BYTES) {
                throw outOfBounds(offset, Integer.BYTES, size);
            }
            WordFactory.<Pointer>pointer(address + offset)
                    .writeInt(0, LITTLE_ENDIAN_PLATFORM ? value : Integer.reverseBytes(value));
            Reference.reachabilityFence(segment);
            return;
        }
        Slow.setIntLittleEndian(segment, offset, value);
    }

    public static void setLongLittleEndian(MemorySegment segment, long address, long size, long offset, long value) {
        if (POINTERS && address != NOT_NATIVE) {
            if (offset < 0 || offset > size - Long.BYTES) {
                throw outOfBounds(offset, Long.BYTES, size);
            }
            WordFactory.<Pointer>pointer(address + offset)
                    .writeLong(0, LITTLE_ENDIAN_PLATFORM ? value : Long.reverseBytes(value));
            Reference.reachabilityFence(segment);
            return;
        }
        Slow.setLongLittleEndian(segment, offset, value);
    }

    public static byte getByte(MemorySegment segment, long address, long size, long offset) {
        if (POINTERS && address != NOT_NATIVE) {
            if (offset < 0 || offset >= size) {
                throw outOfBounds(offset, Byte.BYTES, size);
            }
            byte value = WordFactory.<Pointer>pointer(address + offset).readByte(0);
            Reference.reachabilityFence(segment);
            return value;
        }
        return Slow.getByte(segment, offset);
    }

    public static void setByte(MemorySegment segment, long address, long size, long offset, byte value) {
        if (POINTERS && address != NOT_NATIVE) {
            if (offset < 0 || offset >= size) {
                throw outOfBounds(offset, Byte.BYTES, size);
            }
            WordFactory.<Pointer>pointer(address + offset).writeByte(0, value);
            Reference.reachabilityFence(segment);
            return;
        }
        Slow.setByte(segment, offset, value);
    }

    public static long getLong(MemorySegment segment, long offset) {
        return getLong(segment, readableAddress(segment), segment.byteSize(), offset);
    }

    public static void setLong(MemorySegment segment, long offset, long value) {
        setLong(segment, address(segment), segment.byteSize(), offset, value);
    }

    public static long getLongUnaligned(MemorySegment segment, long offset) {
        return getLongUnaligned(segment, readableAddress(segment), segment.byteSize(), offset);
    }

    public static void setLongUnaligned(MemorySegment segment, long offset, long value) {
        setLongUnaligned(segment, address(segment), segment.byteSize(), offset, value);
    }

    public static int getInt(MemorySegment segment, long offset) {
        return getInt(segment, readableAddress(segment), segment.byteSize(), offset);
    }

    public static void setInt(MemorySegment segment, long offset, int value) {
        setInt(segment, address(segment), segment.byteSize(), offset, value);
    }

    public static int getIntUnaligned(MemorySegment segment, long offset) {
        return getIntUnaligned(segment, readableAddress(segment), segment.byteSize(), offset);
    }

    public static int getIntLittleEndian(MemorySegment segment, long offset) {
        return getIntLittleEndian(segment, readableAddress(segment), segment.byteSize(), offset);
    }

    public static byte getByte(MemorySegment segment, long offset) {
        return getByte(segment, readableAddress(segment), segment.byteSize(), offset);
    }

    public static void setByte(MemorySegment segment, long offset, byte value) {
        setByte(segment, address(segment), segment.byteSize(), offset, value);
    }

    public static long getLongAtIndex(MemorySegment segment, long index) {
        return getLong(segment, scaled(index, 3));
    }

    public static void setLongAtIndex(MemorySegment segment, long index, long value) {
        setLong(segment, scaled(index, 3), value);
    }

    public static long getLongUnalignedAtIndex(MemorySegment segment, long index) {
        return getLongUnaligned(segment, scaled(index, 3));
    }

    public static int getIntAtIndex(MemorySegment segment, long index) {
        return getInt(segment, scaled(index, 2));
    }

    public static void setIntAtIndex(MemorySegment segment, long index, int value) {
        setInt(segment, scaled(index, 2), value);
    }

    public static int getIntUnalignedAtIndex(MemorySegment segment, long index) {
        return getIntUnaligned(segment, scaled(index, 2));
    }

    private static long scaled(long index, int shift) {
        if (index < 0 || index > (Long.MAX_VALUE >> shift)) {
            throw new IndexOutOfBoundsException("index " + index + " out of range");
        }
        return index << shift;
    }

    private static IndexOutOfBoundsException outOfBounds(long offset, long width, long size) {
        return new IndexOutOfBoundsException(
                "access of " + width + " bytes at offset " + offset + " is outside a segment of " + size + " bytes");
    }

    private static final class Slow {

        private Slow() {
        }

        static long getLong(MemorySegment segment, long offset) {
            return segment.get(LONG, offset);
        }

        static void setLong(MemorySegment segment, long offset, long value) {
            segment.set(LONG, offset, value);
        }

        static long getLongUnaligned(MemorySegment segment, long offset) {
            return segment.get(LONG_UNALIGNED, offset);
        }

        static void setLongUnaligned(MemorySegment segment, long offset, long value) {
            segment.set(LONG_UNALIGNED, offset, value);
        }

        static int getInt(MemorySegment segment, long offset) {
            return segment.get(INT, offset);
        }

        static void setInt(MemorySegment segment, long offset, int value) {
            segment.set(INT, offset, value);
        }

        static int getIntUnaligned(MemorySegment segment, long offset) {
            return segment.get(INT_UNALIGNED, offset);
        }

        static int getIntLittleEndian(MemorySegment segment, long offset) {
            return segment.get(INT_LITTLE_ENDIAN, offset);
        }

        static void setIntLittleEndian(MemorySegment segment, long offset, int value) {
            segment.set(INT_LITTLE_ENDIAN, offset, value);
        }

        static void setLongLittleEndian(MemorySegment segment, long offset, long value) {
            segment.set(LONG_LITTLE_ENDIAN, offset, value);
        }

        static byte getByte(MemorySegment segment, long offset) {
            return segment.get(BYTE, offset);
        }

        static void setByte(MemorySegment segment, long offset, byte value) {
            segment.set(BYTE, offset, value);
        }
    }
}
