// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use phf::{Set, phf_set};
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use solace_rs::async_support::{AsyncSession, AsyncSessionBuilder};
use solace_rs::context::Context;
use solace_rs::SolaceLogLevel;
use with_options::WithOptions;

use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult;

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct SolaceCommon {
    /// Solace broker URL (e.g. tcp://localhost:55555 or tcps://broker.example.com:55443)
    #[serde(rename = "solace.url")]
    pub url: String,

    /// Message VPN name. Defaults to "default".
    #[serde(rename = "solace.vpn_name")]
    pub vpn_name: Option<String>,

    /// Username for authentication
    #[serde(rename = "solace.username")]
    pub username: Option<String>,

    /// Password for authentication
    #[serde(rename = "solace.password")]
    pub password: Option<String>,

    /// Client name for the session (optional, auto-generated if not set)
    #[serde(rename = "solace.client_name")]
    pub client_name: Option<String>,

    /// Path to SSL trust store directory for TLS connections
    #[serde(rename = "solace.ssl_trust_store_dir")]
    pub ssl_trust_store_dir: Option<String>,

    /// Number of reconnect retries. -1 for infinite retries. Defaults to -1.
    #[serde(rename = "solace.reconnect_retries")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub reconnect_retries: Option<i64>,

    /// Wait time between reconnect attempts in milliseconds. Defaults to 3000.
    #[serde(rename = "solace.reconnect_retry_wait_ms")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub reconnect_retry_wait_ms: Option<u64>,

    /// Connection timeout in milliseconds. Defaults to 30000.
    #[serde(rename = "solace.connect_timeout_ms")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub connect_timeout_ms: Option<u64>,
}

impl EnforceSecret for SolaceCommon {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "solace.password",
    };
}

impl SolaceCommon {
    /// Build an async Solace session from the configured properties.
    ///
    /// `client_name_override` takes precedence over `self.client_name` when provided.
    /// This allows callers (e.g. parallel consumer readers) to bind with a per-split
    /// unique name to avoid broker rejection of duplicate client names.
    pub fn build_async_session(
        &self,
        client_name_override: Option<&str>,
    ) -> ConnectorResult<(Context, AsyncSession)> {
        let context = Context::new(SolaceLogLevel::Warning)
            .map_err(|e| anyhow::anyhow!("failed to create Solace context: {e}"))?;

        let vpn = self
            .vpn_name
            .as_deref()
            .unwrap_or("default");
        let username = self.username.as_deref().unwrap_or("");
        let password = self.password.as_deref().unwrap_or("");

        let mut builder = AsyncSessionBuilder::new(&context)
            .host_name(self.url.as_str())
            .vpn_name(vpn)
            .username(username)
            .password(password)
            .reconnect_retries(self.reconnect_retries.unwrap_or(-1))
            .reapply_subscriptions(true);

        // Apply client name: override wins over configured name; omit if neither is set
        // so the broker auto-assigns one.
        let effective_name = client_name_override.or(self.client_name.as_deref());
        if let Some(name) = effective_name {
            builder = builder.client_name(name);
        }

        if let Some(wait_ms) = self.reconnect_retry_wait_ms {
            builder = builder.reconnect_retry_wait_ms(wait_ms);
        }
        if let Some(timeout_ms) = self.connect_timeout_ms {
            builder = builder.connect_timeout_ms(timeout_ms);
        }
        if let Some(ref dir) = self.ssl_trust_store_dir {
            builder = builder.ssl_trust_store_dir(dir.as_str());
        }

        let session = builder
            .build()
            .map_err(|e| anyhow::anyhow!("failed to build Solace async session: {e}"))?;

        Ok((context, session))
    }
}
