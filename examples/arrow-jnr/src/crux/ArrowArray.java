package crux;

import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.Struct;
import jnr.ffi.annotations.Delegate;

public class ArrowArray extends Struct {
    public static final int ARROW_FLAG_ORDERED = 1;
    public static final int ARROW_FLAG_NULLABLE = 2;
    public static final int ARROW_FLAG_KEYS_SORTED = 4;

    public ArrowArray(Runtime runtime) {
        super(runtime);
    }

    public static interface Release {
        @Delegate
        public void call(jnr.ffi.Pointer arrow_array);
    }

    // Type description
    public final Pointer format = new Pointer();
    public final Pointer name = new Pointer();
    public final Pointer metadata = new Pointer();

    // Data description
    public final int64_t flags = new int64_t();
    public final int64_t length = new int64_t();
    public final int64_t null_count = new int64_t();
    public final int64_t offset = new int64_t();
    public final int64_t n_buffers = new int64_t();
    public final int64_t n_children = new int64_t();
    public final Pointer buffers = new Pointer();
    public final Pointer children = new Pointer();
    public final Pointer dictionary = new Pointer();

    // Release callback
    public final Function<Release> release = function(Release.class);
    // Opaque producer-specific data
    public final Pointer private_data = new Pointer();
}
