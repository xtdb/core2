package crux;

import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.Struct;
import jnr.ffi.annotations.Delegate;

public class ArrowSchema extends Struct {
    public static final int ARROW_FLAG_DICTIONARY_ORDERED = 1;
    public static final int ARROW_FLAG_NULLABLE = 2;
    public static final int ARROW_FLAG_MAP_KEYS_SORTED = 4;

    public ArrowSchema(Runtime runtime) {
        super(runtime);
    }

    public static interface Release {
        @Delegate
        public void call(jnr.ffi.Pointer arrow_schema);
    }

    // Array type description
    public final Pointer format = new Pointer();
    public final Pointer name = new Pointer();
    public final Pointer metadata = new Pointer();
    public final int64_t flags = new int64_t();
    public final int64_t n_children = new int64_t();
    public final Pointer children = new Pointer();
    public final Pointer dictionary = new Pointer();

    // Release callback
    public final Function<Release> release = function(Release.class);
    // Opaque producer-specific data
    public final Pointer private_data = new Pointer();
}
