// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Keep this allocation regression in its own integration-test binary so other
//! tests cannot change the process-wide allocation count while it is measured.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, Literal, Manifest, ManifestWriterBuilder,
    NestedField, PartitionSpec, PrimitiveType, Schema, Struct, Transform, Type,
};

static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size() as i64, Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(pointer, layout, new_size) };
        if !result.is_null() {
            LIVE_BYTES.fetch_add(new_size as i64 - layout.size() as i64, Ordering::Relaxed);
        }
        result
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[test]
fn completed_manifest_writes_release_partition_field_names() {
    const ENTRIES_PER_WRITE: usize = 100;
    const WARMUP_WRITES: usize = 10;
    const MEASURED_WRITES: usize = 100;
    const MAX_RETAINED_BYTES: i64 = 64 * 1024;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::optional(
                    1,
                    "last_modified",
                    Type::Primitive(PrimitiveType::Timestamp),
                ))])
                .build()
                .unwrap(),
        );
        let spec = PartitionSpec::builder(schema.clone())
            .add_partition_field("last_modified", "last_modified_day", Transform::Day)
            .unwrap()
            .build()
            .unwrap();
        let files = [Some(Literal::date(20_000)), None].map(|partition| {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path("s3://fixture/data.parquet".to_owned())
                .file_format(DataFileFormat::Parquet)
                .partition(Struct::from_iter([partition]))
                .record_count(1)
                .file_size_in_bytes(100)
                .build()
                .unwrap()
        });
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("manifest.avro");
        let io = FileIO::new_with_fs();
        let mut baseline = 0;

        for write_index in 0..WARMUP_WRITES + MEASURED_WRITES {
            let output = io.new_output(path.to_str().unwrap()).unwrap();
            let mut writer =
                ManifestWriterBuilder::new(output, Some(1), schema.clone(), spec.clone())
                    .build_v2_data();
            for entry_index in 0..ENTRIES_PER_WRITE {
                writer.add_file(files[entry_index % 2].clone(), 1).unwrap();
            }
            drop(writer.write_manifest_file().await.unwrap());

            let bytes = std::fs::read(&path).unwrap();
            let manifest = Manifest::parse_avro(&bytes).unwrap();
            assert_eq!(manifest.entries().len(), ENTRIES_PER_WRITE);
            for (index, entry) in manifest.entries().iter().enumerate() {
                assert_eq!(entry.data_file().partition(), files[index % 2].partition());
            }
            drop(manifest);
            drop(bytes);
            std::fs::remove_file(&path).unwrap();
            tokio::task::yield_now().await;

            if write_index + 1 == WARMUP_WRITES {
                baseline = LIVE_BYTES.load(Ordering::Relaxed);
            }
        }

        // Leaking the 17-byte name once per entry retains 170,000 bytes here.
        // Allow unrelated lazy/runtime allocations while rejecting that growth.
        let retained = LIVE_BYTES.load(Ordering::Relaxed) - baseline;
        assert!(
            retained < MAX_RETAINED_BYTES,
            "completed manifest writes retained {retained} bytes after warmup"
        );
    });
}
