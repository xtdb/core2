package core2.extensions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.FieldType;

public class UuidType extends ArrowType.ExtensionType {
    public static final FixedSizeBinary STORAGE_TYPE = new FixedSizeBinary(16);

    static {
        ExtensionTypeRegistry.register(new UuidType());
    }

    @Override
    public ArrowType storageType() {
        return STORAGE_TYPE;
    }

    @Override
    public String extensionName() {
        return "uuid";
    }

    @Override
    public boolean extensionEquals(ExtensionType other) {
        return other instanceof UuidType;
    }

    @Override
    public String serialize() {
        return "";
    }

    @Override
    public ArrowType deserialize(ArrowType storageType, String serializedData) {
        if (!storageType.equals(STORAGE_TYPE)) {
            throw new UnsupportedOperationException("Cannot construct UuidType from underlying type " + storageType);
        } else {
            return new UuidType();
        }
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
        return new UuidVector(name, allocator, fieldType, new FixedSizeBinaryVector(name, allocator, 16));
    }
}
