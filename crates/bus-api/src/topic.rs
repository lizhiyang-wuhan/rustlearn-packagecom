//! Topic：总线上所有通信的路由键。

use std::fmt;

/// 主题（路由键）newtype。
///
/// 约定：
/// - 事件/服务 topic 使用点分命名，如 `sensor.temperature`、`sensor.stats`；
/// - 参数 topic 使用保留前缀 `param://`（见 [`crate::param`]）。
///
/// # 语法解读：newtype 模式（`struct Topic(String)`）
///
/// 用一个单元结构体包住 `String`，得到一个**与 `String` 不同的新类型**。好处：
/// - 类型安全：函数签名写 `Topic` 而非 `String`，编译器不会让你把"模块名"误传给"topic"。
/// - 可为它实现特定 trait（下面的 `Display`）而不污染 `String`。
/// - 封装表示：将来若要把内部换成 `Arc<str>` 或加上校验，只改本文件，调用方无感。
///
/// # 派生的一堆 trait 为何都需要
///
/// `#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]` 不是随手加的，每个都对应实际用途：
/// - `Hash + Eq`：`Topic` 要作为 `HashMap<Topic, _>` 的键（路由表/广播表），必须可哈希、可判等。
/// - `Clone`：事件分发、订阅会话复制都需要廉价 clone topic。
/// - `Debug`：日志与错误信息里打印 topic。
/// - `PartialOrd + Ord`：支持排序（如 `list` 参数 key 时排序、或用 `BTreeMap`）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Topic(String);

impl Topic {
    /// 构造 topic。`impl Into<String>` 让 `&str`/`String` 都能直接传入。
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// 以 `&str` 形式借出内部字符串（不暴露 `String` 所有权，保持封装）。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// # 语法解读：`impl<S: Into<String>> From<S> for Topic`
///
/// 这是一个**泛型 From 实现**（类 blanket）：为所有能 `Into<String>` 的类型 `S`
/// 一次性实现 `From<S> for Topic`。效果是 `"sensor.stats".into()`、`String::from(x).into()`
/// 都能直接得到 `Topic`。正因为有了它，各处签名才能用 `impl Into<Topic>` 接收字面量。
/// 注：`S` 这里是泛型参数，约束 `Into<String>` 限定了只有能转字符串的类型才适用。
impl<S: Into<String>> From<S> for Topic {
    fn from(s: S) -> Self {
        Self(s.into())
    }
}
