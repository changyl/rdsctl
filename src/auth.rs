// rdsctl — 请求级鉴权上下文(S-安全)
//
// 每个 HTTP 请求经 http.rs 鉴权后,把当前用户与其有效权限写入线程本地;
// 管理器操作(创建/销毁/扩容/启停/取消)与审计从此处取"真实操作人"。
// 说明:异步子任务(调度器/巡检)不带请求上下文,审计归属显示 system/sweeper,
// 交互类操作由入口处(线程本地)绑定真实用户。

use std::cell::RefCell;

use serde_json::json;

pub struct Actor {
    pub user: String,
    pub perms: Vec<String>,
}

thread_local! {
    static ACTOR: RefCell<Option<Actor>> = const { RefCell::new(None) };
}

/// 请求进入时设置当前操作人(由 http 鉴权后调用)
pub fn set(user: String, perms: Vec<String>) {
    ACTOR.with(|a| *a.borrow_mut() = Some(Actor { user, perms }));
}

/// 请求处理结束清理
pub fn clear() {
    ACTOR.with(|a| *a.borrow_mut() = None);
}

/// 当前用户(无上下文时返回 "system")
pub fn current_user() -> String {
    ACTOR.with(|a| {
        a.borrow()
            .as_ref()
            .map(|x| x.user.clone())
            .unwrap_or_else(|| "system".to_string())
    })
}

impl Actor {
    /// 当前会话信息(/api/auth/me)
    pub fn snapshot_json() -> String {
        let (user, perms) = ACTOR.with(|a| {
            match a.borrow().as_ref() {
                Some(x) => (x.user.clone(), x.perms.clone()),
                None => (String::new(), Vec::new()),
            }
        });
        json!({ "user": user, "perms": perms }).to_string()
    }
}

/// 是否具备某权限(super 语义由后端 auth_effective 展开为全量权限)
pub fn has_perm(perm: &str) -> bool {
    ACTOR.with(|a| {
        a.borrow()
            .as_ref()
            .map(|x| x.perms.iter().any(|p| p == perm))
            .unwrap_or(false)
    })
}
