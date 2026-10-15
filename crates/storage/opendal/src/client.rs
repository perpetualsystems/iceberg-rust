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

//! HTTP client options for OpenDAL storage.

use std::sync::Arc;

use iceberg::{Error, ErrorKind, Result};
use once_cell::sync::OnceCell;
use opendal::{HttpTransporter, Operator};
use opendal_http_transport_reqwest::ReqwestTransport;
use serde::{Deserialize, Serialize};

/// HTTP client options for OpenDAL storage.
///
/// Clones share one lazily initialized connection pool across factories,
/// storages, and operators. Serialization preserves the settings and omits the
/// runtime client; deserialization creates a fresh pool on first use.
/// Defaults use OpenDAL's existing HTTP transport.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ClientOptions {
    pool_max_idle_per_host: Option<usize>,
    #[serde(skip)]
    client: Arc<OnceCell<HttpTransporter>>,
}

impl ClientOptions {
    /// Limit idle connections retained per host. Zero disables idle pooling.
    /// Active requests are not limited.
    #[must_use]
    pub fn with_pool_max_idle_per_host(mut self, max: usize) -> Self {
        self.pool_max_idle_per_host = Some(max);
        // Changing settings must not reuse a client built with the old settings.
        self.client = Arc::default();
        self
    }

    pub(crate) fn apply(&self, operator: Operator) -> Result<Operator> {
        let Some(max) = self.pool_max_idle_per_host else {
            return Ok(operator);
        };
        let client = self.client.get_or_try_init(|| {
            let client = reqwest::Client::builder()
                .pool_max_idle_per_host(max)
                .build()
                .map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "Failed to build storage HTTP client")
                        .with_source(err)
                })?;
            Ok::<_, Error>(HttpTransporter::new(ReqwestTransport::new(client)))
        })?;
        let context = operator
            .context()
            .clone()
            .with_http_transport(client.clone());
        Ok(operator.with_context(context))
    }
}
