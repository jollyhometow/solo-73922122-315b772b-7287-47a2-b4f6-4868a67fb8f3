"""来源链单元测试：人工维护 / 离线合并 / 删除重建 / 过期拒绝 / 事务回滚。

运行：python3 -m unittest tests.test_provenance
"""

import os
import tempfile
import unittest

_TMP = tempfile.mkdtemp()
os.environ["DB_PATH"] = os.path.join(_TMP, "test.db")

import app.db as db  # noqa: E402
from app.merge import FIELDS, _origin, build_final_actions, compute_plan  # noqa: E402

SEED = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "initial_records.csv")


class ProvenanceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db.init_db(SEED)

    def _events(self, rid):
        groups = db.record_history(rid)
        return [e for g in groups for e in g["events"]]

    def test_trace_start_marks_existing_data(self):
        prov = db.field_provenance("R001")
        for f in FIELDS:
            self.assertEqual(prov[f]["kind"], "trace_start")
            self.assertEqual(prov[f]["value"], {"quadrat": "Q1", "species": "羊草", "count": 12}[f])

    def test_manual_update_records_before_after_and_prev_source(self):
        first = db.field_provenance("R002")["count"]
        status = db.update_record("R002", "Q1", "针茅", 10)
        self.assertEqual(status, "updated")
        prov = db.field_provenance("R002")["count"]
        self.assertEqual(prov["value"], 10)
        self.assertEqual(prov["kind"], "manual")
        self.assertEqual(prov["prev_source"], first["event_id"])
        ev = self._events("R002")[-1]
        self.assertEqual(ev["before_values"], {"count": 8})
        self.assertEqual(ev["after_values"], {"count": 10})

    def test_unchanged_save_does_not_rewrite_source(self):
        before = db.field_provenance("R001")["quadrat"]["event_id"]
        self.assertEqual(db.update_record("R001", "Q1", "羊草", 12), "unchanged")
        self.assertEqual(db.field_provenance("R001")["quadrat"]["event_id"], before)

    def test_merge_keeps_unchanged_field_source_and_same_value_merge(self):
        base = db.get_records()
        cur = db.get_records()
        cur["R003"] = dict(cur["R003"], count=7)  # 当前改株数
        db.update_record("R003", **cur["R003"])
        rev = dict(base)
        rev["R003"] = dict(base["R003"], count=7)  # 修订改成同值
        rev["R004"] = dict(base["R004"], count=11)
        plan = compute_plan(base, rev, cur)
        actions = build_final_actions(plan, {})
        token = "tok-same"
        snap = db.get_records_by_ids(plan["involved"])
        db.save_preview(token, 1, rev, snap, plan)
        self.assertEqual(db.apply_preview(token, 1, snap, plan, actions), "applied")
        # 同值合并不产生新来源事件：仍是人工那次
        self.assertEqual(db.field_provenance("R003")["count"]["kind"], "manual")
        self.assertEqual(db.field_provenance("R003")["count"]["value"], 7)
        prov = db.field_provenance("R004")["count"]
        self.assertEqual(prov["kind"], "merge")
        self.assertEqual(prov["baseline_id"], 1)
        self.assertEqual(prov["preview_token"], token)
        self.assertEqual(prov["revision_value"], 11)

    def test_stale_preview_rejected_without_provenance(self):
        base = db.get_records()
        rev = dict(base)
        rev["R006"] = dict(base["R006"], count=99)
        cur = db.get_records()
        plan = compute_plan(base, rev, cur)
        token = "tok-stale"
        snap = db.get_records_by_ids(plan["involved"])
        db.save_preview(token, 1, rev, snap, plan)
        before_kind = db.field_provenance("R006")["count"]["kind"]
        db.update_record("R006", "Q2", "糙隐子草", 7)  # 预览后改动
        self.assertEqual(db.apply_preview(token, 1, snap, plan, build_final_actions(plan, {})), "stale")
        self.assertEqual(db.get_records()["R006"]["count"], 7)
        self.assertNotEqual(db.field_provenance("R006")["count"]["kind"], before_kind)
        self.assertEqual(db.get_preview(token)["status"], "pending")

    def test_discarded_preview_writes_nothing(self):
        base = db.get_records()
        rev = dict(base)
        rev["R007"] = dict(base["R007"], count=50)
        plan = compute_plan(base, rev, db.get_records())
        token = "tok-discard"
        db.save_preview(token, 1, rev, db.get_records_by_ids(plan["involved"]), plan)
        db.set_preview_status(token, "discarded", "已放弃")
        for rid in plan["involved"]:
            self.assertEqual(db.field_provenance(rid)["count"]["kind"], "trace_start")

    def test_delete_keeps_chain_and_recreate_starts_new_chain(self):
        self.assertTrue(db.delete_record("R009"))
        groups = db.record_history("R009")
        self.assertEqual(len(groups), 1)
        self.assertFalse(groups[0]["live"])
        self.assertEqual(groups[0]["events"][-1]["operation"], "delete")
        self.assertEqual(db.deleted_records()[0]["id"], "R009")
        db.add_record("R009", "Q3", "羊草", 8)
        groups = db.record_history("R009")
        self.assertEqual(len(groups), 2)
        self.assertTrue(groups[1]["live"])
        self.assertFalse(groups[0]["live"])

    def test_merge_update_only_changed_field_keeps_other_sources(self):
        base = db.get_records()
        # 当前人工把 R011 物种改掉
        db.update_record("R011", "Q4", "糙隐子草-改", 4)
        cur = db.get_records()
        rev = dict(base)
        rev["R011"] = dict(base["R011"], count=9)  # 修订只改株数
        plan = compute_plan(base, rev, cur)
        self.assertEqual(len(plan["auto"]), 1)
        token = "tok-field"
        snap = db.get_records_by_ids(plan["involved"])
        db.save_preview(token, 1, rev, snap, plan)
        self.assertEqual(db.apply_preview(token, 1, snap, plan, build_final_actions(plan, {})), "applied")
        prov = db.field_provenance("R011")
        self.assertEqual(prov["count"]["kind"], "merge")
        self.assertEqual(prov["count"]["preview_token"], token)
        self.assertEqual(prov["species"]["kind"], "manual")  # 未变字段沿用原来源
        ev = self._events("R011")[-1]
        self.assertEqual(ev["changed_fields"], ["count"])
        self.assertEqual(ev["before_values"], {"count": 4})
        self.assertEqual(ev["after_values"], {"count": 9})
        self.assertIn("count", ev["prev_sources"])  # 改前株数来源指向基线创建事件
        self.assertNotIn("species", ev["prev_sources"])  # 未变字段不写来源

    def test_revision_restore_after_delete_starts_new_chain(self):
        base = db.get_records()
        db.delete_record("R012")
        cur = db.get_records()
        rev = dict(base)
        rev["R012"] = dict(base["R012"], count=16)  # 修订修改了当前已删记录
        plan = compute_plan(base, rev, cur)
        self.assertEqual(plan["conflicts"][0]["ctype"], "modify_vs_delete")
        token = "tok-restore"
        snap = db.get_records_by_ids(plan["involved"])
        db.save_preview(token, 2, rev, snap, plan)
        actions = build_final_actions(plan, {"R012": {"_": "rev"}})
        self.assertEqual(actions[0]["action"], "insert")
        self.assertTrue(actions[0]["restored"])
        self.assertEqual(db.apply_preview(token, 2, snap, plan, actions), "applied")
        groups = db.record_history("R012")
        self.assertEqual(len(groups), 2)
        self.assertFalse(groups[0]["live"])
        self.assertTrue(groups[1]["live"])
        prov = db.field_provenance("R012")["count"]
        self.assertEqual(prov["kind"], "merge")
        self.assertEqual(prov["baseline_id"], 2)
        self.assertEqual(prov["preview_token"], token)

    def test_atomic_rollback_leaves_no_events(self):
        cur = db.get_records()
        token = "tok-rollback"
        db.save_preview(token, 1, {}, db.get_records_by_ids(["R010"]), {"involved": ["R010"], "auto": [], "conflicts": []})
        events_before = len(self._events("R010"))
        actions = [
            {"action": "update", "id": "R010", "from": cur["R010"],
             "to": dict(cur["R010"], count=77),
             "field_origins": {"count": _origin("revision", 77)}},
            {"action": "insert", "id": "R010", "record": cur["R010"]},  # 重复新增，必失败
        ]
        snap = db.get_records_by_ids(["R010"])
        with self.assertRaises(Exception):
            db.apply_preview(token, 1, snap, {"involved": ["R010"]}, actions)
        self.assertEqual(db.get_records()["R010"]["count"], cur["R010"]["count"])
        self.assertEqual(len(self._events("R010")), events_before)
        self.assertEqual(db.get_preview(token)["status"], "pending")


if __name__ == "__main__":
    unittest.main()
