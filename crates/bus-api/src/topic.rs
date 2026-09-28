//! Topic：总线上所有通信的路由键。

use std::fmt;

/// 主题（路由键）newtype。
///
/// 约定：
/// - 事件/服务 topic 使用点分命名，如 `sensor.temperature`、`sensor.stats`；
/// - 参数 topic 使用保留前缀 `param://`（见 [`crate::param`]）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Topic(String);

impl Topic {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<S: Into<String>> From<S> for Topic {
    fn from(s: S) -> Self {
        Self(s.into())
    }
}
