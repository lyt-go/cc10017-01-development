"""主题上线就绪数据包测试：发布边界、中断续做、补报冲突与跨模块交接。"""
from __future__ import annotations

import unittest

from app.seed import SEED_ROWS
from app.services.kafka_connector import KafkaConnectorService
from app.services.topic_readiness import (
    SOURCE_BACKFILL,
    SOURCE_FIRST,
    STATUS_CONFLICT,
    STATUS_FAILED,
    STATUS_INTERRUPTED,
    STATUS_READY,
    TopicReadinessService,
)
from app.services.topic_registry import TopicRegistryService
from app.store import store


def _packages(topic_code: str) -> list[dict]:
    return [row for row in store.rows("topic_readiness") if row.get("topic_code") == topic_code]


class ReadinessTestBase(unittest.TestCase):
    """每个用例都从示例就绪包重新开始，避免用例之间互相消耗版本。"""

    def setUp(self) -> None:
        rows = store.rows("topic_readiness")
        rows.clear()
        rows.extend(dict(row) for row in SEED_ROWS["topic_readiness"])
        store.rows("consumer_readiness_records").clear()


class TestReadinessPackage(ReadinessTestBase):
    """首次上线：主题配置与接收端版本一起进入同一发布边界。"""

    def test_package_keeps_topic_and_receiver_version(self) -> None:
        svc = TopicReadinessService()
        package, message = svc.check("TOPI-0001", source=SOURCE_FIRST)
        self.assertIsNotNone(package)
        self.assertIn("就绪包", message)
        self.assertEqual(package["topic_version"], "v3")
        self.assertEqual(package["receiver_version"], "v2")
        self.assertEqual(package["consumer_group"], "order-consumer")
        self.assertEqual(package["consumer_status"], "已生效")
        # 同一发布边界：主题配置与接收端版本同批写入
        self.assertTrue(package["release_boundary"])
        self.assertTrue(all(c["status"] == "通过" for c in package["checks"]))
        # 逐项回查：每条检查项都带着主题版本与接收端版本
        for item in package["trace"]:
            self.assertEqual(item["topic_version"], "v3")
            self.assertEqual(item["receiver_version"], "v2")

    def test_consumer_draft_blocks_topic_visibility(self) -> None:
        topic_svc = TopicRegistryService()
        topic, _ = topic_svc.create_entry({
            "主题编号": "TOPI-READY1", "主题名称": "草稿消费组主题", "消息格式": "JSON",
        })
        connector, _ = KafkaConnectorService().create_entry({
            "配置编号": "KAFK-READY1", "集群地址": "kafka:9092", "消费组名": "ready-consumer",
            "接收端版本": "v1",
        })
        package, message = TopicReadinessService().check("TOPI-READY1", source=SOURCE_FIRST)
        self.assertIsNotNone(package)
        self.assertEqual(package["status"], STATUS_FAILED)
        self.assertFalse(package["consumable"])
        self.assertIn("空消费链路", message)

        entry, blocked = topic_svc.run_action(topic["id"], "确认上线")
        self.assertIsNone(entry)
        self.assertIn("暂不能上线", blocked)
        self.assertEqual(topic["status"], "草稿")

        # 消息接入把检查结果写回就绪包：消费组就绪后主题才可见。
        KafkaConnectorService().run_action(connector["id"], "标记生效")
        record = store.rows("consumer_readiness_records")[-1]
        self.assertEqual(record["consumer_group"], "ready-consumer")
        self.assertEqual(record["topic_code"], "TOPI-READY1")
        self.assertEqual(record["package_version"], TopicReadinessService().latest_for_topic("TOPI-READY1")["package_version"])

        entry, message = topic_svc.run_action(topic["id"], "确认上线")
        self.assertIsNotNone(entry)
        self.assertEqual(entry["status"], "已上线")
        self.assertTrue(TopicReadinessService().latest_for_topic("TOPI-READY1")["consumable"])

    def test_only_one_consumable_package_per_topic(self) -> None:
        svc = TopicReadinessService()
        first, _ = svc.check("TOPI-0001", source=SOURCE_FIRST)
        second, _ = svc.check("TOPI-0001", source=SOURCE_FIRST)
        consumables = [row for row in _packages("TOPI-0001") if row.get("consumable")]
        self.assertEqual(len(consumables), 1)
        self.assertEqual(consumables[0]["id"], second["id"])
        self.assertEqual(first["id"] and store.find("topic_readiness", first["id"])["superseded_by"], second["package_no"])


class TestInterruptedResume(ReadinessTestBase):
    """发布中断：旧包保留，重试只补未完成批次。"""

    def test_interrupt_keeps_previous_package(self) -> None:
        svc = TopicReadinessService()
        baseline = [row for row in _packages("TOPI-0002") if row.get("consumable")]
        self.assertTrue(baseline)
        previous = baseline[0]

        # 示例里 TOPI-0002 的补报包在第 1 批后中断，重试只续做剩余的 2、3 批。
        package, message = svc.check(
            "TOPI-0002", source=SOURCE_BACKFILL, topic_version="v3", batch_limit=1)
        self.assertEqual(package["status"], STATUS_INTERRUPTED)
        self.assertEqual(package["completed_batches"], [1, 2])
        self.assertEqual(package["remaining_batches"], [3])
        self.assertIn("保留上一份完整就绪包", message)
        # 上一份完整就绪包仍然可消费
        previous = store.find("topic_readiness", previous["id"])
        self.assertTrue(previous["consumable"])

    def test_resume_only_fills_missing_batches(self) -> None:
        svc = TopicReadinessService()
        package, _ = svc.check(
            "TOPI-0002", source=SOURCE_BACKFILL, topic_version="v7", batch_limit=1)
        self.assertEqual(package["remaining_batches"], [3])
        first_batch_at = [
            c["checked_at"] for c in package["checks"] if c["batch"] == 1]
        resumed, _ = svc.resume(package["id"])
        self.assertEqual(resumed["completed_batches"], [1, 2, 3])
        # 已完成批次的检查项没有被重跑
        self.assertEqual(
            [c["checked_at"] for c in resumed["checks"] if c["batch"] == 1],
            first_batch_at,
        )
        self.assertTrue(all(c["status"] != "待检查" for c in resumed["checks"]))

    def test_repeat_check_resumes_instead_of_new_version(self) -> None:
        svc = TopicReadinessService()
        was_consumable = svc.latest_for_topic("TOPI-0002")
        store.find("topic_readiness", was_consumable["id"])["consumable"] = True
        package, _ = svc.check(
            "TOPI-0002", source=SOURCE_BACKFILL, topic_version="v8", batch_limit=1)
        again, message = svc.check(
            "TOPI-0002", source=SOURCE_BACKFILL, topic_version="v8", batch_limit=0)
        self.assertEqual(again["id"], package["id"])
        self.assertIn("已保留上一份完整就绪包", message)


class TestBackfillConflict(ReadinessTestBase):
    """补报版本与已生效接收版本相遇：记录先后，仍只保留一份可消费就绪包。"""

    def test_conflict_blocks_until_resolved(self) -> None:
        svc = TopicReadinessService()
        package, message = svc.check("TOPI-0001", source=SOURCE_BACKFILL, topic_version="v5")
        self.assertEqual(package["status"], STATUS_CONFLICT)
        self.assertFalse(package["consumable"])
        self.assertIn("确认先后", message)
        self.assertEqual(package["topic_version"], "v5")
        self.assertEqual(package["receiver_version"], "v2")
        self.assertIn("v5", package["conflict"]["summary"])
        self.assertIn("v2", package["conflict"]["summary"])

    def test_two_policies_produce_different_version_order(self) -> None:
        svc = TopicReadinessService()
        topic_first, _ = svc.check(
            "TOPI-0001", source=SOURCE_BACKFILL, topic_version="v6", conflict_policy="主题优先")
        receiver_first, _ = svc.check(
            "TOPI-0001", source=SOURCE_BACKFILL, topic_version="v6", conflict_policy="接收端优先")
        self.assertEqual(topic_first["status"], STATUS_READY)
        self.assertEqual(receiver_first["status"], STATUS_READY)
        self.assertNotEqual(topic_first["version_order"], receiver_first["version_order"])
        self.assertIn("主题版本 v6 → 接收端版本 v2", topic_first["version_order"])
        self.assertIn("接收端版本 v2 → 主题版本 v6", receiver_first["version_order"])
        consumables = [row for row in _packages("TOPI-0001") if row.get("consumable")]
        self.assertEqual(len(consumables), 1)
        self.assertEqual(consumables[0]["id"], receiver_first["id"])

    def test_resolve_records_both_versions(self) -> None:
        svc = TopicReadinessService()
        package, _ = svc.check("TOPI-0001", source=SOURCE_BACKFILL, topic_version="v7")
        resolved, message = svc.resolve_conflict(package["id"], "主题优先")
        self.assertEqual(resolved["status"], STATUS_READY)
        self.assertTrue(resolved["consumable"])
        self.assertEqual(resolved["topic_version"], "v7")
        self.assertEqual(resolved["receiver_version"], "v2")
        self.assertIn("主题版本 v7 → 接收端版本 v2", resolved["version_order"])
        self.assertIn("就绪", message)


class TestReadinessQueries(ReadinessTestBase):
    def test_scenarios_and_version_sort(self) -> None:
        svc = TopicReadinessService()
        desc, total, scenarios = svc.list_packages(sort="version_desc")
        asc, _, _ = svc.list_packages(sort="version_asc")
        self.assertEqual(total, len(store.rows("topic_readiness")))
        self.assertTrue(scenarios)
        self.assertGreater(desc[0]["id"], asc[0]["id"])
        names = {item["name"] for item in scenarios}
        self.assertIn("订单实时入湖", names)

    def test_by_connector_returns_same_version(self) -> None:
        svc = TopicReadinessService()
        latest = svc.latest_for_topic("TOPI-0001")
        package = svc.latest_for_connector(1)
        self.assertIsNotNone(package)
        self.assertEqual(package["topic_code"], latest["topic_code"])
        self.assertEqual(package["package_version"], latest["package_version"])


if __name__ == "__main__":
    unittest.main()
