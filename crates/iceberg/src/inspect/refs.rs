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

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::builder::{PrimitiveBuilder, StringBuilder};
use arrow_array::types::{Int32Type, Int64Type};
use futures::{StreamExt, stream};

use crate::Result;
use crate::arrow::schema_to_arrow_schema;
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{NestedField, PrimitiveType, SnapshotRetention, Type};
use crate::table::Table;

/// Refs table.
pub struct RefsTable<'a> {
    table: &'a Table,
}

impl<'a> RefsTable<'a> {
    /// Create a new Refs table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Returns the iceberg schema of the refs table.
    pub fn schema(&self) -> crate::spec::Schema {
        let fields = vec![
            NestedField::new(1, "name", Type::Primitive(PrimitiveType::String), true),
            NestedField::new(2, "type", Type::Primitive(PrimitiveType::String), true),
            NestedField::new(3, "snapshot_id", Type::Primitive(PrimitiveType::Long), true),
            NestedField::new(
                4,
                "max_reference_age_in_ms",
                Type::Primitive(PrimitiveType::Long),
                false,
            ),
            NestedField::new(
                5,
                "min_snapshots_to_keep",
                Type::Primitive(PrimitiveType::Int),
                false,
            ),
            NestedField::new(
                6,
                "max_snapshot_age_in_ms",
                Type::Primitive(PrimitiveType::Long),
                false,
            ),
        ];

        crate::spec::Schema::builder()
            .with_fields(fields.into_iter().map(|f| f.into()))
            .build()
            .unwrap()
    }

    /// Scans the refs table.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        let schema = schema_to_arrow_schema(&self.schema())?;

        let mut snapshot_name = StringBuilder::new();
        let mut snapshot_type = StringBuilder::new();
        let mut snapshot_id = PrimitiveBuilder::<Int64Type>::new();
        let mut max_reference_age_in_ms = PrimitiveBuilder::<Int64Type>::new();
        let mut min_snapshots_to_keep = PrimitiveBuilder::<Int32Type>::new();
        let mut max_snapshot_age_in_ms = PrimitiveBuilder::<Int64Type>::new();

        for (name, snapshot_reference) in &self.table.metadata().refs {
            snapshot_name.append_value(name);
            snapshot_type.append_value(if snapshot_reference.is_branch() {
                "BRANCH"
            } else {
                "TAG"
            });
            snapshot_id.append_value(snapshot_reference.snapshot_id);

            match &snapshot_reference.retention {
                SnapshotRetention::Branch {
                    min_snapshots_to_keep: min_snaps,
                    max_snapshot_age_ms: max_snap_age,
                    max_ref_age_ms: max_ref_age,
                } => {
                    max_reference_age_in_ms.append_option(*max_ref_age);
                    min_snapshots_to_keep.append_option(*min_snaps);
                    max_snapshot_age_in_ms.append_option(*max_snap_age);
                }
                SnapshotRetention::Tag {
                    max_ref_age_ms: max_ref_age,
                } => {
                    max_reference_age_in_ms.append_option(*max_ref_age);
                    min_snapshots_to_keep.append_null();
                    max_snapshot_age_in_ms.append_null();
                }
            }
        }

        let batch = RecordBatch::try_new(Arc::new(schema), vec![
            Arc::new(snapshot_name.finish()),
            Arc::new(snapshot_type.finish()),
            Arc::new(snapshot_id.finish()),
            Arc::new(max_reference_age_in_ms.finish()),
            Arc::new(min_snapshots_to_keep.finish()),
            Arc::new(max_snapshot_age_in_ms.finish()),
        ])?;
        Ok(stream::iter(vec![Ok(batch)]).boxed())
    }
}

#[cfg(test)]
mod tests {
    use expect_test::expect;

    use crate::inspect::metadata_table::tests::check_record_batches;
    use crate::scan::tests::TableTestFixture;

    #[tokio::test]
    async fn test_refs_table() {
        let table = TableTestFixture::new().table;

        let batch_stream = table.inspect().refs().scan().await.unwrap();

        check_record_batches(
            batch_stream,
            expect![[r#"
                Field { name: "name", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1"} },
                Field { name: "type", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "2"} },
                Field { name: "snapshot_id", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "3"} },
                Field { name: "max_reference_age_in_ms", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "4"} },
                Field { name: "min_snapshots_to_keep", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "5"} },
                Field { name: "max_snapshot_age_in_ms", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "6"} }"#]],
            expect![[r#"
                name: StringArray
                [
                  "main",
                  "test",
                ],
                type: StringArray
                [
                  "BRANCH",
                  "TAG",
                ],
                snapshot_id: PrimitiveArray<Int64>
                [
                  3055729675574597004,
                  3051729675574597004,
                ],
                max_reference_age_in_ms: PrimitiveArray<Int64>
                [
                  null,
                  10000000,
                ],
                min_snapshots_to_keep: PrimitiveArray<Int32>
                [
                  null,
                  null,
                ],
                max_snapshot_age_in_ms: PrimitiveArray<Int64>
                [
                  null,
                  null,
                ]"#]],
            &[],
            Some("name")
        ).await;
    }
}
