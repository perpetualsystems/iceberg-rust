// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! This module contains the writer for data file format supported by iceberg: parquet, orc.

use arrow_array::RecordBatch;
use futures::Future;

use super::CurrentFileStatus;
use crate::Result;
use crate::spec::DataFileBuilder;

mod parquet_writer;
pub use parquet_writer::{ParquetWriter, ParquetWriterBuilder};

use crate::io::OutputFile;

pub mod location_generator;
/// Module providing writers that can automatically roll over to new files based on size thresholds.
pub mod rolling_writer;

type DefaultOutput = Vec<DataFileBuilder>;

/// File writer builder trait.
pub trait FileWriterBuilder<O = DefaultOutput>: Clone + Send + Sync + 'static {
    /// The associated file writer type.
    type R: FileWriter<O>;
    /// Build file writer.
    fn build(&self, output_file: OutputFile) -> impl Future<Output = Result<Self::R>> + Send;
}

/// File writer focus on writing record batch to different physical file format.(Such as parquet. orc)
pub trait FileWriter<O = DefaultOutput>: Send + CurrentFileStatus + 'static {
    /// Write record batch to file.
    fn write(&mut self, batch: &RecordBatch) -> impl Future<Output = Result<()>> + Send;
    /// Close file writer.
    fn close(self) -> impl Future<Output = Result<O>> + Send;
}

/// Size estimates for the current in-progress row group.
///
/// `#[non_exhaustive]` — fields may be added without breaking callers.
#[non_exhaustive]
#[derive(Debug, Default, Clone, Copy)]
pub struct RowGroupSizeEstimate {
    /// Post-compression byte count from `AsyncArrowWriter::in_progress_size()`.
    /// Already included in `CurrentFileStatus::current_written_size()` — do not double-count.
    pub compressed: usize,
    /// Arrow buffer capacity from `RecordBatch::get_array_memory_size()`, accumulated per batch.
    /// Resets on explicit `flush_row_group()` only; implicit row-count-limit flushes do not reset it.
    /// May overcount sliced batches (`.capacity()`, not logical size).
    pub arrow_memory: usize,
}

/// Row-group-level flush control for file writers that support it.
///
/// Not object-safe due to RPITIT in `flush_row_group` — use generic bounds (`W: RowGroupFlushable`).
///
/// `in_progress_row_group_bytes()` is O(physical column count); avoid calling inside inner loops.
pub trait RowGroupFlushable: Send {
    /// Returns size estimates for the current in-progress row group, or all-zeros if none is open.
    fn in_progress_row_group_bytes(&self) -> RowGroupSizeEstimate;
    /// Flushes the current in-progress row group.
    fn flush_row_group(&mut self) -> impl Future<Output = Result<()>> + Send;
}
