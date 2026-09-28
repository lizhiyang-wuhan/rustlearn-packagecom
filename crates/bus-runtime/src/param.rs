//! 参数存储：get/set 语义 + watch 热更新的运行时实现。

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use bus_api::BusError;
use bus_api::event::Value;
use bus_api::param::{ParamClient, ParamProvider};
use tokio::sync::watch;

/// 单个参数条目：一个 watch 发送端，值变更即通知所有订阅者。
struct ParamEntry {
    tx: watch::Sender<Value>,
}

/// 参数表：`module -> (key -> entry)`。读多写少、临界区极短，用 `std::sync::RwLock`。
#[derive(Default)]
struct ParamInner {
    modules: RwLock<HashMap<String, HashMap<String, ParamEntry>>>,
}

/// 进程内参数存储。同时实现 [`ParamClient`]（读写方）与 [`ParamProvider`]（持有方注册）。
#[derive(Clone, Default)]
pub struct ParamStore {
    inner: Arc<ParamInner>,
}

impl ParamStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// 作为客户端能力对象暴露。
    pub fn as_client(&self) -> Arc<dyn ParamClient> {
        Arc::new(self.clone())
    }

    /// 作为参数注册端能力对象暴露。
    pub fn as_provider(&self) -> Arc<dyn ParamProvider> {
        Arc::new(self.clone())
    }
}

#[async_trait]
impl ParamClient for ParamStore {
    async fn get(&self, module: &str, key: &str) -> bus_api::Result<Value> {
        let modules = self.inner.modules.read().unwrap();
        let entry = modules
            .get(module)
            .and_then(|m| m.get(key))
            .ok_or_else(|| BusError::ParamNotFound {
                module: module.to_string(),
                key: key.to_string(),
            })?;
        Ok(entry.tx.borrow().clone())
    }

    async fn set(&self, module: &str, key: &str, value: Value) -> bus_api::Result<()> {
        let modules = self.inner.modules.read().unwrap();
        let entry = modules
            .get(module)
            .and_then(|m| m.get(key))
            .ok_or_else(|| BusError::ParamNotFound {
                module: module.to_string(),
                key: key.to_string(),
            })?;
        // send_modify 一定成功（不因无接收者而失败），并即时唤醒所有 watch::changed()
        entry.tx.send_modify(|slot| *slot = value);
        Ok(())
    }

    async fn list(&self, module: &str) -> bus_api::Result<Vec<String>> {
        let modules = self.inner.modules.read().unwrap();
        match modules.get(module) {
            Some(map) => {
                let mut keys: Vec<String> = map.keys().cloned().collect();
                keys.sort();
                Ok(keys)
            }
            None => Ok(Vec::new()),
        }
    }

    fn watch(&self, module: &str, key: &str) -> bus_api::Result<watch::Receiver<Value>> {
        let modules = self.inner.modules.read().unwrap();
        let entry = modules
            .get(module)
            .and_then(|m| m.get(key))
            .ok_or_else(|| BusError::ParamNotFound {
                module: module.to_string(),
                key: key.to_string(),
            })?;
        Ok(entry.tx.subscribe())
    }
}

impl ParamProvider for ParamStore {
    fn declare(&self, module: &str, key: &str, initial: Value) -> bus_api::Result<watch::Receiver<Value>> {
        let (tx, rx) = watch::channel(initial);
        let mut modules = self.inner.modules.write().unwrap();
        modules
            .entry(module.to_string())
            .or_default()
            .insert(key.to_string(), ParamEntry { tx });
        Ok(rx)
    }
}
