use crate::error::StorageError;
use rocksdb::{BlockBasedOptions, DBCompressionType, Options, SliceTransform, DB};
use std::sync::Arc;

/// The storage subsystem, used to manage low-level storage of tables and atomicity
/// via rockdb's write batch operations.
/// Adding/Removing tables etc should happen via the catalog, at this abstraction level a table has
/// no name, its just referenced via a u32
pub struct Storage {
    #[allow(dead_code)]
    db: Arc<DB>,
}

impl Storage {
    pub fn new(path: &str) -> Result<Self, StorageError> {
        let mut options = Options::default();
        let mut block_options = BlockBasedOptions::default();
        // These options are non-negotiable
        options.set_prefix_extractor(SliceTransform::create_fixed_prefix(
            std::mem::size_of::<u32>(),
        ));
        options.create_if_missing(true);
        //options.set_merge_operator("frequency_merge", frequency_merge, Some(frequency_merge));
        //options.set_compaction_filter("compaction_filter", compaction_filter);

        // These options are "tunable"
        block_options.set_lru_cache(2 * 1024 * 1024 * 1024);
        block_options.set_format_version(3);
        block_options.set_bloom_filter(10, false);
        block_options.set_cache_index_and_filter_blocks(true);
        options.set_block_based_table_factory(&block_options);
        options.set_keep_log_file_num(3);
        options.increase_parallelism(8);
        options.set_min_write_buffer_number(3);
        options.set_max_write_buffer_number(5);
        options.set_advise_random_on_open(false);
        // We want to keep the first layer pretty small it's probably sets an upper limit on our
        // "delete"(negative merge) tuples from our compaction before they're merged/compacted away
        options.set_write_buffer_size(64 * 1024 * 1024);
        options.set_max_bytes_for_level_base(640 * 1024 * 1024);
        options.set_target_file_size_base(64 * 1024 * 1024);
        options.set_compression_per_level(&[
            DBCompressionType::None, // 640mb
            DBCompressionType::None, // 6.4gb
            DBCompressionType::Zlib, // 64 gb
            DBCompressionType::Zlib, // 640gb
            DBCompressionType::Zlib,
            DBCompressionType::Zlib,
            DBCompressionType::Zlib,
        ]);

        let db = Arc::from(DB::open(&options, path)?);

        Ok(Storage { db })
    }
}

// STORAGE IMPLEMENTATION DETAILS
// Everything stored by incresql will be stored in a table(and exposed to the users somewhere in the
// catalog), this makes debugging super easy, as well as operational concerns (where's all my disk
// space gone).
// This does mean that all state will be stored in terms of datums but with efficient json formats
// this shouldn't be too much of an issue, we can always drop down to a bytea datum in the worst
// case.
//
// Table Format...
// A table can be broken down into index and log sections
// The index section is data sorted by its tuples with MVCC type semantics based on timestamps -
// this section is used for point in time lookups/scans.
// The log section is a (partial) copy of the data stored in write order - this allows our
// incremental operators to keep track of where they're up to.
// On disk these are stored as..
// Index section
// key = <prefix>:<tuple-pk>:<-timestamp>, value = <tuple-rest><freq>
//   Here timestamps are stored in reverse order, this allows efficiently finding the correct rows
//   during forward iteration. We do however have a special case, the most recent record for each
//   tuple-pk is stored as:
// key = <prefix>:<tuple-pk>:<0>, value = <tuple-rest><timestamp><freq>
//   This allows any point reads of the latest values(as used by state lookups for incremental
//   operators) be able to be done using rocksdb point lookups(and be able to make use of rocks
//   bloom filters).  As inserts also require reading the previous values this helps out here too,
//   An insert will cause the previous value to be rewritten into the standard format
//
// Log section
// key = <prefix+1>:<timestamp>:<tuple>, value = <freq delta>
