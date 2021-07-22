package core2.extensions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

import java.nio.ByteBuffer;
import java.util.UUID;

public class UuidVector extends ExtensionTypeVector<FixedSizeBinaryVector> {
    private final Field field;

    public UuidVector(String name, BufferAllocator allocator, FieldType fieldType, FixedSizeBinaryVector underlyingVector) {
        super(name, allocator, underlyingVector);
        this.field = new Field(name, fieldType, null);
    }

    public UuidVector(Field field, BufferAllocator allocator, FixedSizeBinaryVector underlyingVector) {
        super(field, allocator, underlyingVector);
        this.field = field;
    }

    @Override
    public UUID getObject(int index) {
        ByteBuffer bb = ByteBuffer.wrap(getUnderlyingVector().getObject(index));
        return new UUID(bb.getLong(), bb.getLong());
    }

    @Override
    public Field getField() {
        return field;
    }

    public void setNull(int index) {
        getUnderlyingVector().setNull(index);
    }

    public void setSafe(int index, UUID uuid) {
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        getUnderlyingVector().setSafe(index, bb.array());
    }

    @Override
    public int hashCode(int index) {
        return getUnderlyingVector().hashCode(index);
    }

    @Override
    public int hashCode(int index, ArrowBufHasher hasher) {
        return getUnderlyingVector().hashCode(index, hasher);
    }

    @Override
    public void copyFromSafe(int fromIndex, int thisIndex, ValueVector from) {
        getUnderlyingVector().copyFromSafe(fromIndex, thisIndex, ((UuidVector) from).getUnderlyingVector());
    }

    @Override
    public TransferPair makeTransferPair(ValueVector target) {
        ValueVector targetUnderlying = ((UuidVector) target).getUnderlyingVector();
        TransferPair tp = getUnderlyingVector().makeTransferPair(targetUnderlying);

        return new TransferPair() {
            @Override
            public void transfer() {
                tp.transfer();
            }

            @Override
            public void splitAndTransfer(int startIndex, int length) {
                tp.splitAndTransfer(startIndex, length);
            }

            @Override
            public ValueVector getTo() {
                return target;
            }

            @Override
            public void copyValueSafe(int from, int to) {
                tp.copyValueSafe(from, to);
            }
        };
    }
}
