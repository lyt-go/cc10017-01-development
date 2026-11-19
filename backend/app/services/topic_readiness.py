"""主题上线就绪数据包：把主题配置与接收端版本收在同一个发布边界里。

设计约束（对应上线前的可核对证据）：

1. 主题配置与接收端版本必须在同一次写入里一起产生，不能先写主题、再补接收端，
   否则主题先可见而消费组仍停在草稿，下游会启动空消费链路。
2. 任何时刻同一个主题只允许存在一份「可消费」的就绪包；新包完成时才替代旧包，
   发布中断时旧包继续可用。
3. 发布中断后的重试只续做未完成批次，已经通过的检查项保持原样，不重建、不回退。
4. 补报版本与已生效接收版本在同一发布窗口相遇时，需要显式选择确认先后；
   两种做法会生成不同版本顺序的就绪包，但同一主题仍然只有一份可消费。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store

MODULE = "topic_readiness"
TOTAL_BATCHES = 3
BATCH_LABELS = {
    1: "主题配置批",
    2: "接收端配置批",
    3: "发布边界批",
}
STATUS_READY = "就绪"
STATUS_FAILED = "校验失败"
STATUS_INTERRUPTED = "检查中断"
STATUS_CONFLICT = "冲突待裁决"

SOURCE_FIRST = "首次上线"
SOURCE_BACKFILL = "补报"
SOURCES = [SOURCE_FIRST, SOURCE_BACKFILL]

POLICY_TOPIC_FIRST = "主题优先"
POLICY_RECEIVER_FIRST = "接收端优先"
POLICY_ALIASES = {
    "": "",
    "自动判定": "",
    "自动": "",
    POLICY_TOPIC_FIRST: POLICY_TOPIC_FIRST,
    "先主题后接收": POLICY_TOPIC_FIRST,
    "主题版本优先": POLICY_TOPIC_FIRST,
    POLICY_RECEIVER_FIRST: POLICY_RECEIVER_FIRST,
    "先接收端后主题": POLICY_RECEIVER_FIRST,
    "接收版本优先": POLICY_RECEIVER_FIRST,
}

REQUIRED_TOPIC_FIELDS = ["主题编号", "主题名称", "消息格式"]

# 演示用的业务场景映射：视图按业务场景分组展示。
SCENARIO_MAP = {
    "TOPI-0001": "订单实时入湖",
    "TOPI-0002": "用户行为分析",
    "TOPI-0003": "支付结果通知",
}

CHECK_LABELS = {
    "topic_config": "主题配置完整性",
    "topic_version": "主题版本已登记",
    "receiver_config": "接收端配置完整性",
    "consumer_status": "消费组状态已生效",
    "release_boundary": "主题与接收端同一发布边界",
    "single_package": "唯一可消费就绪包",
    "backfill_conflict": "补报冲突判定",
}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def normalize_policy(value: str | None) -> str:
    return POLICY_ALIASES.get(str(value or "").strip(), "")


class TopicReadinessService:
    """生成、续做和查询主题上线就绪数据包。"""

    # ───────────────────────────────
    # 查询
    # ───────────────────────────────

    def list_packages(
        self,
        *,
        scenario: str | None = None,
        source: str | None = None,
        status: str | None = None,
        keyword: str | None = None,
        sort: str = "version_desc",
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int, list[dict[str, Any]]]:
        rows = list(store.rows(MODULE))
        scenarios = self._scenarios()
        if scenario:
            rows = [r for r in rows if scenario.casefold() in str(r.get("business_scenario", "")).casefold()]
        if source:
            rows = [r for r in rows if r.get("source") == source]
        if status:
            rows = [r for r in rows if r.get("status") == status]
        if keyword:
            needle = keyword.casefold()
            rows = [
                r for r in rows
                if needle in str(r.get("package_no", "")).casefold()
                or needle in str(r.get("topic_code", "")).casefold()
                or needle in str(r.get("topic_name", "")).casefold()
                or needle in str(r.get("consumer_group", "")).casefold()
                or needle in str(r.get("connector_code", "")).casefold()
            ]
        rows.sort(key=lambda r: int(r.get("id", 0)), reverse=(sort != "version_asc"))
        total = len(rows)
        start = (page - 1) * size
        return [self.decorate(r) for r in rows[start:start + size]], total, scenarios

    def get_package(self, entry_id: int) -> dict[str, Any] | None:
        for row in store.rows(MODULE):
            if int(row.get("id", 0)) == int(entry_id):
                return row
        return None

    def latest_for_topic(self, topic_code: str, package_version: str | None = None) -> dict[str, Any] | None:
        rows = [r for r in store.rows(MODULE) if r.get("topic_code") == topic_code]
        if package_version:
            rows = [r for r in rows if r.get("package_version") == package_version]
        if not rows:
            return None
        return max(rows, key=lambda r: int(r.get("id", 0)))

    def latest_for_connector(self, connector_id: int) -> dict[str, Any] | None:
        connector = store.find("kafka_connector", connector_id)
        if connector is None:
            return None
        topic_code = self._topic_code_for_connector(connector)
        if not topic_code:
            code = str(connector.get("配置编号", ""))
            topic_code = code.replace("KAFK", "TOPI") if code.startswith("KAFK") else ""
        return self.latest_for_topic(topic_code) if topic_code else None

    def get_receiver(self, topic_code: str) -> dict[str, Any] | None:
        """返回主题对应的接收端接入配置。"""
        rows = store.rows("kafka_connector")
        suffix = topic_code.rsplit("-", 1)[-1] if "-" in topic_code else topic_code
        for row in rows:
            if row.get("配置编号") == f"KAFK-{suffix}":
                return row
        for row in rows:
            if topic_code and topic_code in str(row.get("死信队列", "")):
                return row
        return None

    # ───────────────────────────────
    # 生成与续做
    # ───────────────────────────────

    def check(
        self,
        topic_code: str,
        *,
        source: str = SOURCE_FIRST,
        scenario: str | None = None,
        conflict_policy: str | None = None,
        batch_limit: int | None = None,
        topic_version: str | None = None,
        receiver_version: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """对订单主题配置发起上线检查并写入就绪包（或续做未完成批次）。"""
        topic = self._find_topic(topic_code)
        if topic is None:
            return None, f"数据主题 {topic_code} 不存在或已归档"
        source = source if source in SOURCES else SOURCE_FIRST
        policy = normalize_policy(conflict_policy)

        # 同一主题、同来源上一次检查被中断时，直接续做，不新建版本。
        interrupted = self._find_interrupted(topic_code, source)
        if interrupted is not None:
            return self.resume(interrupted["id"], batch_limit=batch_limit, conflict_policy=policy)

        # 先把主题与接收端两份快照取齐，再一次性写入就绪包：
        # 这就是“同一发布边界一起可见”的写入点。
        receiver = self.get_receiver(topic_code)
        snapshots = self._snapshot(topic, receiver, topic_version, receiver_version)
        baseline = self._latest_consumable(topic_code)
        conflict = self._detect_conflict(source, snapshots, baseline)

        package_id = self._next_id()
        package_no = f"READY-{package_id:04d}"
        package = {
            "id": package_id,
            "package_no": package_no,
            "package_version": f"RV-{package_id:04d}",
            "business_scenario": scenario or SCENARIO_MAP.get(topic_code, f"{topic.get('主题名称', topic_code)}接入"),
            "source": source,
            "topic_code": topic_code,
            "topic_name": topic.get("主题名称"),
            "topic_status": topic.get("status"),
            "topic_version": snapshots["topic_version"],
            "receiver_version": snapshots["receiver_version"],
            "receiver_effective_version": (baseline or {}).get("receiver_version"),
            "connector_code": receiver.get("配置编号") if receiver else None,
            "consumer_group": receiver.get("消费组名") if receiver else None,
            "consumer_status": receiver.get("status") if receiver else None,
            "release_boundary": f"PUB-{topic_code}-{package_id:04d}",
            "boundary_visibility": "主题配置与接收端版本在同一发布边界一起可见",
            "version_order": None,
            "overview": "",
            "conflict": conflict,
            "checks": [],
            "completed_batches": [],
            "current_batch": 1,
            "status": STATUS_INTERRUPTED,
            "consumable": False,
            "superseded_by": None,
            "created_at": _now(),
            "updated_at": _now(),
            "resumed_from": None,
            "inputs": {
                "topic_code": topic_code,
                "source": source,
                "conflict_policy": policy,
                "topic_version": snapshots["topic_version"],
                "receiver_version": snapshots["receiver_version"],
            },
            "_snapshots": snapshots,
        }
        checks = self._build_checks(package, source, conflict, policy)
        completed = self._limit_batches(batch_limit)
        store.rows(MODULE).append(package)
        self._apply_progress(package, checks, completed)
        self._finalize(package)
        return self.decorate(package), self._message(package)

    def resolve_conflict(
        self, package_id: int, conflict_policy: str | None
    ) -> tuple[dict[str, Any] | None, str]:
        """对补报冲突做裁决：记录版本先后，同一主题仍只保留一份可消费就绪包。"""
        package = self.get_package(package_id)
        if package is None:
            return None, f"就绪包 {package_id} 不存在"
        if not package.get("conflict"):
            return None, f"就绪包 {package.get('package_no')} 没有补报冲突需要裁决"
        policy = normalize_policy(conflict_policy)
        if not policy:
            return None, "请选择「主题优先」或「接收端优先」后再裁决"
        package.setdefault("inputs", {})["conflict_policy"] = policy
        for check in package.get("checks", []):
            if check.get("key") == "backfill_conflict":
                conflict = package.get("conflict") or {}
                check["status"] = "通过"
                check["detail"] = (
                    f"{conflict.get('summary', '补报版本与已生效接收版本相遇')}；"
                    f"已按「{policy}」记录版本先后："
                    f"{self._version_order(str(package.get('topic_version')), str(package.get('receiver_version')), policy)}"
                )
                check["checked_at"] = _now()
        self._finalize(package)
        return self.decorate(package), self._message(package)

    def resume(
        self,
        package_id: int,
        *,
        batch_limit: int | None = None,
        conflict_policy: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """续做检查中断的就绪包：只执行未完成批次，已完成检查项原样保留。"""
        package = self.get_package(package_id)
        if package is None:
            return None, f"就绪包 {package_id} 不存在"
        if package.get("status") != STATUS_INTERRUPTED:
            return self.decorate(package), f"就绪包 {package.get('package_no')} 没有未完成批次"

        inputs = dict(package.get("inputs") or {})
        policy = normalize_policy(conflict_policy) or normalize_policy(inputs.get("conflict_policy"))
        inputs["conflict_policy"] = policy
        package["inputs"] = inputs

        topic = self._find_topic(str(package.get("topic_code")))
        if topic is None:
            return None, f"数据主题 {package.get('topic_code')} 不存在或已归档"
        receiver = self.get_receiver(str(package.get("topic_code")))
        snapshots = self._snapshot(
            topic, receiver, inputs.get("topic_version"), inputs.get("receiver_version"))
        package["_snapshots"] = snapshots
        package["topic_status"] = topic.get("status")
        package["consumer_status"] = receiver.get("status") if receiver else None
        package["consumer_group"] = receiver.get("消费组名") if receiver else None
        package["connector_code"] = receiver.get("配置编号") if receiver else None
        baseline = self._latest_consumable(str(package.get("topic_code")), exclude_id=package_id)
        conflict = self._detect_conflict(str(inputs.get("source")), snapshots, baseline)
        package["conflict"] = conflict
        package["receiver_effective_version"] = (baseline or {}).get("receiver_version")

        rebuilt = self._build_checks(package, str(inputs.get("source")), conflict, policy)
        completed = set(int(b) for b in package.get("completed_batches", []))
        kept: list[dict[str, Any]] = []
        for existing in package.get("checks", []):
            if int(existing.get("batch", 0)) in completed:
                # 已完成批次只补齐缺失的检查项，绝不重跑或改写结论。
                kept.append(existing)
        for item in rebuilt:
            if int(item["batch"]) not in completed:
                kept.append(item)
        order = list(CHECK_LABELS)
        kept.sort(key=lambda c: (int(c.get("batch", 0)), order.index(c.get("key", "")) if c.get("key") in order else 99))

        pending_batches = [b for b in range(1, TOTAL_BATCHES + 1) if b not in completed]
        run_count = len(pending_batches)
        if batch_limit is not None:
            run_count = min(max(int(batch_limit), 0), run_count)
        new_completed = sorted(completed | set(pending_batches[:run_count]))

        package["checks"] = kept
        package["updated_at"] = _now()
        self._apply_progress(package, kept, len(new_completed), completed_batches=new_completed)
        self._finalize(package)
        return self.decorate(package), self._message(package)

    def activate_for_topic(self, topic: dict[str, Any]) -> dict[str, Any] | None:
        """主题确认上线时，把同一主题最新一份就绪包标记为可消费。"""
        topic_code = str(topic.get("主题编号") or "")
        if not topic_code:
            return None
        receiver = self.get_receiver(topic_code)
        latest_ready = self._latest_ready(topic_code)
        if latest_ready is not None:
            latest_ready["topic_status"] = topic.get("status")
            latest_ready["consumer_status"] = receiver.get("status") if receiver else None
            self._refresh_consumer_fields(latest_ready, receiver)
            self._activate(latest_ready)
            return latest_ready
        return None

    def on_receiver_ready(self, connector: dict[str, Any]) -> dict[str, Any] | None:
        """消息接入把检查结果写回就绪包，并据此建立消费组就绪记录。

        只有能关联到已注册主题的接入配置才参与跨模块交接，避免误伤普通配置。
        """
        topic_code = self._topic_code_for_connector(connector)
        if not topic_code or self._find_topic(topic_code) is None:
            return None
        connector["接收端版本"] = str(connector.get("接收端版本") or "v1")

        latest = self.latest_for_topic(topic_code)
        package: dict[str, Any] | None
        if latest is not None and latest.get("status") == STATUS_INTERRUPTED:
            package, _ = self.resume(latest["id"])
        elif latest is not None and latest.get("status") == STATUS_CONFLICT:
            # 冲突未裁决时只刷新接收端快照，不抢先产生新版本。
            latest["consumer_status"] = connector.get("status")
            latest["consumer_group"] = connector.get("消费组名")
            latest["connector_code"] = connector.get("配置编号")
            latest["updated_at"] = _now()
            package = latest
        else:
            source = str((latest or {}).get("source") or SOURCE_FIRST)
            package, _ = self.check(topic_code, source=source)
        if package is None:
            return None

        records = store.rows("consumer_readiness_records")
        records.append({
            "id": max((int(r.get("id", 0)) for r in records), default=0) + 1,
            "connector_code": connector.get("配置编号"),
            "consumer_group": connector.get("消费组名"),
            "topic_code": topic_code,
            "package_no": package.get("package_no"),
            "package_version": package.get("package_version"),
            "topic_version": package.get("topic_version"),
            "receiver_version": package.get("receiver_version"),
            "status": package.get("status"),
            "consumable": package.get("consumable"),
            "created_at": _now(),
        })
        connector["_readiness_package_no"] = package.get("package_no")
        connector["_readiness_package_version"] = package.get("package_version")
        return package

    def _refresh_consumer_fields(self, package: dict[str, Any], receiver: dict[str, Any] | None) -> None:
        package["connector_code"] = receiver.get("配置编号") if receiver else package.get("connector_code")
        package["consumer_group"] = receiver.get("消费组名") if receiver else package.get("consumer_group")
        package["consumer_status"] = receiver.get("status") if receiver else package.get("consumer_status")

    # ───────────────────────────────
    # 检查项
    # ───────────────────────────────

    def _build_checks(
        self,
        package: dict[str, Any],
        source: str,
        conflict: dict[str, Any] | None,
        policy: str,
    ) -> list[dict[str, Any]]:
        snapshots = package.get("_snapshots") or self._snapshot_from_package(package)
        topic = snapshots["topic"]
        receiver = snapshots["receiver"]
        topic_version = snapshots["topic_version"]
        receiver_version = snapshots["receiver_version"]
        topic_code = str(package.get("topic_code"))
        checked_at = _now()

        def item(key: str, batch: int, ok: bool, detail: str, status: str | None = None) -> dict[str, Any]:
            return {
                "key": key,
                "label": CHECK_LABELS[key],
                "batch": batch,
                "status": status or ("通过" if ok else "失败"),
                "detail": detail,
                "topic_version": topic_version,
                "receiver_version": receiver_version,
                "checked_at": checked_at,
            }

        missing = [f for f in REQUIRED_TOPIC_FIELDS if not str(topic.get(f) or "").strip()]
        checks = [
            item(
                "topic_config", 1, not missing,
                "主题编号、主题名称、消息格式完整" if not missing else f"缺少必填字段：{'、'.join(missing)}",
            ),
            item(
                "topic_version", 1, bool(topic_version),
                f"主题版本 {topic_version} 已保留在就绪包" if topic_version else "主题版本为空，无法回查",
            ),
            item(
                "receiver_config", 2, receiver is not None and bool(receiver.get("消费组名")) and bool(receiver_version),
                (
                    f"接收端 {receiver.get('配置编号')} / 消费组 {receiver.get('消费组名')}，接收端版本 {receiver_version}"
                    if receiver else "未找到主题对应的接收端接入配置"
                ),
            ),
            item(
                "consumer_status", 2, receiver is not None and receiver.get("status") == "已生效",
                (
                    "消费组已生效，主题上线后可直接消费"
                    if receiver is not None and receiver.get("status") == "已生效"
                    else f"消费组当前为「{receiver.get('status') if receiver else '未配置'}」，不得先让主题可见"
                ),
            ),
        ]

        boundary_ok = topic is not None and receiver is not None
        checks.append(item(
            "release_boundary", 3, boundary_ok,
            (
                f"{package.get('release_boundary')}：主题配置与接收端版本同批写入，一起可见"
                if boundary_ok else "主题或接收端缺失，无法形成同一发布边界"
            ),
        ))
        checks.append(item(
            "single_package", 3, True,
            "完成后同一主题仅保留一份可消费就绪包，旧版本只做历史留存",
        ))

        if source == SOURCE_BACKFILL and conflict is not None:
            if not policy:
                status = "待裁决"
                detail = (
                    f"{conflict['summary']}；请选择「主题优先」或「接收端优先」后重新检查，"
                    "任一时刻仍只保留一份可消费就绪包"
                )
            else:
                status = "通过"
                detail = (
                    f"{conflict['summary']}；已按「{policy}」记录版本先后："
                    f"{self._version_order(topic_version, receiver_version, policy)}"
                )
        else:
            status = "通过"
            detail = "首次上线，不涉及补报版本与已生效接收版本的先后冲突"
        checks.append(item("backfill_conflict", 3, True, detail, status=status))
        return checks

    def _snapshot(
        self,
        topic: dict[str, Any],
        receiver: dict[str, Any] | None,
        topic_version: str | None = None,
        receiver_version: str | None = None,
    ) -> dict[str, Any]:
        return {
            "topic": topic,
            "receiver": receiver,
            "topic_version": str(topic_version or self._topic_version(topic)),
            "receiver_version": str(
                receiver_version or (receiver or {}).get("接收端版本") or "v1"),
        }

    def _topic_version(self, topic: dict[str, Any]) -> str:
        """主题版本优先取注册的 Schema 版本，其次取已生效演进规则的版本。"""
        declared = str(topic.get("Schema版本") or "").strip()
        if declared:
            return declared
        for rule in store.rows("schema_evolve"):
            if rule.get("关联主题") == topic.get("主题编号") and rule.get("status") == "已生效":
                version = str(rule.get("生效版本") or "").strip()
                if version:
                    return version
        return "v1"

    def _snapshot_from_package(self, package: dict[str, Any]) -> dict[str, Any]:
        return {
            "topic": self._find_topic(str(package.get("topic_code"))) or {},
            "receiver": self.get_receiver(str(package.get("topic_code"))),
            "topic_version": package.get("topic_version") or "",
            "receiver_version": package.get("receiver_version") or "",
        }

    # ───────────────────────────────
    # 批次与状态
    # ───────────────────────────────

    def _limit_batches(self, batch_limit: int | None) -> int:
        if batch_limit is None:
            return TOTAL_BATCHES
        return min(max(int(batch_limit), 0), TOTAL_BATCHES)

    def _apply_progress(
        self,
        package: dict[str, Any],
        checks: list[dict[str, Any]],
        completed_count: int,
        completed_batches: list[int] | None = None,
    ) -> None:
        if completed_batches is None:
            completed_batches = list(range(1, completed_count + 1))
        completed_set = set(completed_batches)
        for check in checks:
            if int(check.get("batch", 0)) not in completed_set:
                check["status"] = "待检查"
                check["detail"] = "批次未执行，续做时只补齐该检查项"
                check["checked_at"] = None
        package["checks"] = checks
        package["completed_batches"] = sorted(completed_set)
        package["current_batch"] = (max(completed_set) + 1) if completed_set else 1
        if package["current_batch"] > TOTAL_BATCHES:
            package["current_batch"] = TOTAL_BATCHES

    def _finalize(self, package: dict[str, Any]) -> None:
        checks = package.get("checks", [])
        statuses = [c.get("status") for c in checks]
        conflict = package.get("conflict")
        policy = normalize_policy((package.get("inputs") or {}).get("conflict_policy"))
        if any(status == "待检查" for status in statuses):
            package["status"] = STATUS_INTERRUPTED
            package["consumable"] = False
            package["version_order"] = None
            package["overview"] = self._interrupted_overview(package)
        elif conflict is not None and not policy:
            package["status"] = STATUS_CONFLICT
            package["consumable"] = False
            package["version_order"] = None
            package["overview"] = (
                f"主题 {package.get('topic_version')} 与接收端 {package.get('receiver_version')} "
                "在同一发布窗口相遇，等待确认先后"
            )
        elif any(status in ("失败", "待裁决") for status in statuses):
            package["status"] = STATUS_FAILED
            package["consumable"] = False
            package["version_order"] = None
            package["overview"] = self._failed_overview(package)
        else:
            package["status"] = STATUS_READY
            package["version_order"] = self._version_order(
                str(package.get("topic_version")), str(package.get("receiver_version")), policy)
            package["overview"] = self._ready_overview(package)
            if self._can_consume(package):
                self._activate(package)
            else:
                package["consumable"] = False
        package["updated_at"] = _now()

    def _activate(self, package: dict[str, Any]) -> None:
        if not self._can_consume(package):
            package["consumable"] = False
            return
        topic_code = str(package.get("topic_code"))
        for other in store.rows(MODULE):
            if int(other.get("id", 0)) == int(package.get("id", 0)):
                continue
            if other.get("topic_code") == topic_code and other.get("consumable"):
                other["consumable"] = False
                other["superseded_by"] = package.get("package_no")
                other["updated_at"] = _now()
        package["consumable"] = True
        package["superseded_by"] = None

    def guard_online(self, topic: dict[str, Any]) -> tuple[bool, str, dict[str, Any] | None]:
        """确认上线前的就绪闸门：消费组未就绪时不允许主题先可见。

        - 没有接收端配置的主题保持原有行为（不参与跨模块交接）。
        - 有接收端配置时必须存在「就绪」的就绪包，否则拒绝上线并说明卡在哪一项。
        """
        topic_code = str(topic.get("主题编号") or "")
        receiver = self.get_receiver(topic_code)
        if receiver is None:
            return True, "", None
        latest = self._latest_ready(topic_code)
        if latest is None:
            latest = self.latest_for_topic(topic_code)
        if latest is None:
            package, message = self.check(topic_code)
            if package is None or package.get("status") != STATUS_READY:
                return False, message, package
            return True, message, package
        if latest.get("status") != STATUS_READY:
            return False, (
                f"主题 {topic_code} 的上线就绪包 {latest.get('package_no')} 状态为"
                f"「{latest.get('status')}」：{latest.get('overview')}"
            ), latest
        return True, f"上线就绪包 {latest.get('package_no')} 已就绪", latest

    def _can_consume(self, package: dict[str, Any]) -> bool:
        return (
            package.get("status") == STATUS_READY
            and package.get("topic_status") == "已上线"
            and package.get("consumer_status") == "已生效"
        )

    # ───────────────────────────────
    # 冲突与版本
    # ───────────────────────────────

    def _detect_conflict(
        self,
        source: str,
        snapshots: dict[str, Any],
        baseline: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if source != SOURCE_BACKFILL or baseline is None:
            return None
        topic_version = str(snapshots.get("topic_version") or "")
        receiver_version = str(snapshots.get("receiver_version") or "")
        baseline_topic = str(baseline.get("topic_version") or "")
        baseline_receiver = str(baseline.get("receiver_version") or "")
        # 补报版本与已生效接收版本在同一窗口相遇：只要主题版本相对上一份可消费包发生变化，
        # 就要显式确认“先主题后接收”还是“先接收后主题”，否则消费组先读到的主题范围会变。
        if topic_version == baseline_topic:
            return None
        return {
            "detected": True,
            "baseline_package_no": baseline.get("package_no"),
            "baseline_topic_version": baseline_topic,
            "baseline_receiver_version": baseline_receiver,
            "topic_version": topic_version,
            "receiver_version": receiver_version,
            "summary": (
                f"补报版本 {topic_version} 与已生效接收版本 {baseline_receiver or receiver_version} "
                "在同一发布窗口相遇，确认先后会改变消费组先读到的主题范围"
            ),
            "options": [POLICY_TOPIC_FIRST, POLICY_RECEIVER_FIRST],
        }

    def _version_order(self, topic_version: str, receiver_version: str, policy: str) -> str:
        if policy == POLICY_RECEIVER_FIRST:
            return f"接收端版本 {receiver_version} → 主题版本 {topic_version}"
        return f"主题版本 {topic_version} → 接收端版本 {receiver_version}"

    # ───────────────────────────────
    # 文案
    # ───────────────────────────────

    def _ready_overview(self, package: dict[str, Any]) -> str:
        return (
            f"{len(package.get('checks', []))} 项检查通过，主题 {package.get('topic_version')} "
            f"与接收端 {package.get('receiver_version')} 绑定在同一发布边界 {package.get('release_boundary')}"
        )

    def _failed_overview(self, package: dict[str, Any]) -> str:
        failed = [c for c in package.get("checks", []) if c.get("status") == "失败"]
        if any(c.get("key") == "receiver_config" for c in failed):
            return "未找到主题对应的接收端接入配置，主题不得先可见，否则下游会启动空消费链路"
        if any(c.get("key") == "consumer_status" for c in failed):
            return "消费组仍停在草稿，主题不得先可见，否则下游会启动空消费链路"
        if failed:
            return "；".join(str(c.get("detail")) for c in failed)
        return "检查未通过，就绪包不可消费"

    def _interrupted_overview(self, package: dict[str, Any]) -> str:
        completed = package.get("completed_batches", [])
        remaining = [b for b in range(1, TOTAL_BATCHES + 1) if b not in completed]
        return (
            f"检查在第 {max(completed) if completed else 0} 批后中断；"
            f"未完成批次 {remaining}，上一份完整就绪包保持可用"
        )

    def _message(self, package: dict[str, Any]) -> str:
        status = package.get("status")
        no = package.get("package_no")
        if status == STATUS_INTERRUPTED:
            return f"就绪包 {no} 检查中断，已保留上一份完整就绪包，可续做未完成批次"
        if status == STATUS_CONFLICT:
            return (
                f"就绪包 {no} 命中补报冲突，请确认先后："
                f"主题版本 {package.get('topic_version')} / 接收端版本 {package.get('receiver_version')}"
            )
        if status == STATUS_FAILED:
            return f"就绪包 {no} 未通过：{package.get('overview')}"
        return f"就绪包 {no} 已生成，主题版本 {package.get('topic_version')} / 接收端版本 {package.get('receiver_version')}"

    # ───────────────────────────────
    # 辅助
    # ───────────────────────────────

    def decorate(self, package: dict[str, Any]) -> dict[str, Any]:
        """返回给界面的视图：附带批次进度与逐项版本回查轨迹。"""
        item = {key: value for key, value in package.items() if key != "_snapshots"}
        checks = [dict(c) for c in package.get("checks", [])]
        item["checks"] = checks
        completed = set(int(b) for b in package.get("completed_batches", []))
        item["batches"] = [
            {
                "batch": batch,
                "label": BATCH_LABELS[batch],
                "status": "已完成" if batch in completed else "未完成",
            }
            for batch in range(1, TOTAL_BATCHES + 1)
        ]
        item["trace"] = [
            {
                "item": check.get("label"),
                "topic_version": check.get("topic_version"),
                "receiver_version": check.get("receiver_version"),
                "status": check.get("status"),
                "checked_at": check.get("checked_at"),
            }
            for check in checks
        ]
        item["remaining_batches"] = [
            batch for batch in range(1, TOTAL_BATCHES + 1) if batch not in completed
        ]
        return item

    def _scenarios(self) -> list[dict[str, Any]]:
        counts: dict[str, int] = {}
        for row in store.rows(MODULE):
            name = str(row.get("business_scenario") or "未分类")
            counts[name] = counts.get(name, 0) + 1
        return [{"name": name, "count": counts[name]} for name in sorted(counts)]

    def _next_id(self) -> int:
        rows = store.rows(MODULE)
        return max((int(row.get("id", 0)) for row in rows), default=0) + 1

    def _find_topic(self, topic_code: str) -> dict[str, Any] | None:
        for row in store.rows("topic_registry"):
            if row.get("主题编号") == topic_code:
                return row
        return None

    def _topic_code_for_connector(self, connector: dict[str, Any]) -> str:
        code = str(connector.get("配置编号", ""))
        if code.startswith("KAFK-"):
            return "TOPI-" + code.split("-", 1)[1]
        for row in store.rows("topic_registry"):
            topic_code = str(row.get("主题编号", ""))
            if topic_code and topic_code in str(connector.get("死信队列", "")):
                return topic_code
        return ""

    def _find_interrupted(self, topic_code: str, source: str) -> dict[str, Any] | None:
        candidates = [
            row for row in store.rows(MODULE)
            if row.get("topic_code") == topic_code
            and row.get("source") == source
            and row.get("status") == STATUS_INTERRUPTED
        ]
        return max(candidates, key=lambda r: int(r.get("id", 0))) if candidates else None

    def _latest_consumable(self, topic_code: str, exclude_id: int | None = None) -> dict[str, Any] | None:
        candidates = [
            row for row in store.rows(MODULE)
            if row.get("topic_code") == topic_code
            and row.get("consumable")
            and int(row.get("id", 0)) != int(exclude_id or 0)
        ]
        return max(candidates, key=lambda r: int(r.get("id", 0))) if candidates else None

    def _latest_ready(self, topic_code: str) -> dict[str, Any] | None:
        candidates = [
            row for row in store.rows(MODULE)
            if row.get("topic_code") == topic_code
            and row.get("status") == STATUS_READY
            and not row.get("superseded_by")
        ]
        return max(candidates, key=lambda r: int(r.get("id", 0))) if candidates else None


topic_readiness_service = TopicReadinessService()
