"""三方向合并核心逻辑：基线(base) / 离线修订(rev) / 当前记录(cur)。

逐字段比较：
- 仅修订侧改动的字段 -> 自动采用修订值
- 仅当前侧改动的字段 -> 自动保留当前值
- 两侧改成相同值      -> 自动保留一份（值未变，不改写该字段来源）
- 两侧改成不同值 / 删除遇修改 / 同ID新增内容不同 -> 冲突，交人工选择

每个自动动作都携带逐字段来源标记（field_origins），确认应用时写入来源链：
origins[f] = {origin: revision|current_kept, revision_value: 修订原值, resolution: rev|cur|null}
"""

FIELDS = ("quadrat", "species", "count")
FIELD_LABELS = {"quadrat": "样方", "species": "物种", "count": "株数"}


def _origin(origin: str, revision_value, resolution=None) -> dict:
    return {"origin": origin, "revision_value": revision_value, "resolution": resolution}


def _changed_field_origins(before: dict | None, after: dict, origins: dict) -> dict:
    """只保留实际变化的字段的来源标记。"""
    if before is None:
        keep = set(after)
    else:
        keep = {f for f in FIELDS if before.get(f) != after.get(f)}
    return {f: origins.get(f, _origin("revision", after.get(f))) for f in FIELDS if f in keep}


def compute_plan(base: dict, rev: dict, cur: dict) -> dict:
    """计算合并计划。base/rev/cur 均为 {id: {quadrat, species, count}}。

    返回 {"auto": [...], "conflicts": [...], "involved": [...]}
    involved 为计划触及的记录 ID（用于确认时的过期检测）。
    """
    auto = []
    conflicts = []
    involved = set()

    for rid in sorted(set(base) | set(rev) | set(cur)):
        b = base.get(rid)
        r = rev.get(rid)
        c = cur.get(rid)

        if b is not None and r is not None and c is not None:
            # 三方都存在：逐字段合并
            if r == b:
                continue  # 修订未改动该记录，保留当前
            merged = {}
            field_conflicts = []
            taken = {}       # 自动决定的字段，供预览展示
            origins = {}     # 逐字段来源标记
            same_value = []  # 双方改成相同值的字段（值等于当前，不改写来源）
            for f in FIELDS:
                bv, rv, cv = b[f], r[f], c[f]
                r_changed = rv != bv
                c_changed = cv != bv
                if r_changed and c_changed and rv != cv:
                    field_conflicts.append({"field": f, "base": bv, "rev": rv, "cur": cv})
                    origins[f] = _origin("revision", rv)
                elif r_changed and c_changed and rv == cv:
                    merged[f] = rv
                    same_value.append(f)
                    # 值没有实际变化：沿用当前字段来源，不产生新来源
                elif r_changed:
                    merged[f] = rv
                    taken[f] = {"src": "rev", "base": bv, "value": rv}
                    origins[f] = _origin("revision", rv)
                else:
                    merged[f] = cv
                    if c_changed:
                        taken[f] = {"src": "cur", "base": bv, "value": cv}
                        origins[f] = _origin("current_kept", rv)
            if field_conflicts:
                conflicts.append(
                    {
                        "id": rid,
                        "ctype": "fields",
                        "base": b,
                        "rev": r,
                        "cur": c,
                        "merged": merged,          # 无冲突字段已合并好的部分
                        "same_value": same_value,
                        "fields": field_conflicts,
                        "field_origins": origins,
                    }
                )
                involved.add(rid)
            elif merged != c:
                auto.append(
                    {
                        "action": "update", "id": rid, "from": c, "to": merged,
                        "taken": taken, "revision_id": rid,
                        "field_origins": _changed_field_origins(c, merged, origins),
                    }
                )
                involved.add(rid)

        elif b is not None and r is None and c is not None:
            # 修订中缺行 = 离线删除
            if c == b:
                # 修订文件里没有该 ID，不填 revision_id
                auto.append(
                    {"action": "delete", "id": rid, "record": b,
                     "field_origins": {f: _origin("revision_deletion", None) for f in FIELDS}}
                )
                involved.add(rid)
            else:
                # 删除 vs 当前已修改 -> 冲突
                conflicts.append(
                    {"id": rid, "ctype": "delete_vs_modify", "base": b, "rev": None, "cur": c}
                )
                involved.add(rid)

        elif b is not None and r is not None and c is None:
            # 当前已删除；修订未改动则删除生效，改动过则冲突
            if r == b:
                continue
            conflicts.append(
                {"id": rid, "ctype": "modify_vs_delete", "base": b, "rev": r, "cur": None}
            )
            involved.add(rid)

        elif b is not None and r is None and c is None:
            continue  # 双方都删除，同一改动只留一份

        elif b is None and r is not None and c is None:
            # 纯离线新增
            auto.append(
                {"action": "insert", "id": rid, "record": r, "revision_id": rid,
                 "field_origins": {f: _origin("revision", r[f]) for f in FIELDS}}
            )
            involved.add(rid)

        elif b is None and r is not None and c is not None:
            # 双方各自新增同一 ID
            if r == c:
                continue  # 内容相同，只留一份（当前值不变，不改写来源）
            conflicts.append(
                {"id": rid, "ctype": "add_add", "base": None, "rev": r, "cur": c}
            )
            involved.add(rid)

        else:
            # 基线与修订都没有，仅当前新增 -> 不动
            continue

    return {"auto": auto, "conflicts": conflicts, "involved": sorted(involved)}


def build_final_actions(plan: dict, resolutions: dict) -> list:
    """把自动合并结果与人工冲突裁决合成为最终动作列表。

    resolutions: {record_id: {"_": "rev"|"cur"} 或 {field: "rev"|"cur", ...}}
    所有冲突必须全部裁决，否则抛 ValueError。
    """
    actions = [dict(a) for a in plan["auto"]]

    for conf in plan["conflicts"]:
        rid = conf["id"]
        ctype = conf["ctype"]
        choice = resolutions.get(rid)
        if not choice:
            raise ValueError(f"记录 {rid} 的冲突尚未裁决")

        if ctype == "fields":
            final = dict(conf["merged"])
            origins = dict(conf.get("field_origins") or {})
            chosen = {}
            for fc in conf["fields"]:
                f = fc["field"]
                sel = choice.get(f)
                if sel not in ("rev", "cur"):
                    raise ValueError(f"记录 {rid} 字段 {f} 的冲突尚未裁决")
                final[f] = fc[sel]
                chosen[f] = sel
                origins[f] = _origin(
                    "revision" if sel == "rev" else "current_kept",
                    fc["rev"],
                    resolution=sel,
                )
            if final != conf["cur"]:
                actions.append(
                    {
                        "action": "update", "id": rid, "from": conf["cur"], "to": final,
                        "revision_id": rid,
                        "field_origins": _changed_field_origins(conf["cur"], final, origins),
                        "resolutions": chosen,
                    }
                )

        elif ctype == "delete_vs_modify":
            # 修订删除 vs 当前修改：rev=删除，cur=保留当前值
            sel = choice.get("_")
            if sel == "rev":
                actions.append(
                    {
                        "action": "delete", "id": rid,
                        "field_origins": {f: _origin("revision_deletion", None, "rev") for f in FIELDS},
                        "resolutions": {"_": "rev"},
                    }
                )
            elif sel != "cur":
                raise ValueError(f"记录 {rid} 的冲突尚未裁决")

        elif ctype == "modify_vs_delete":
            # 修订修改 vs 当前删除：rev=按修订值恢复，cur=保持删除
            sel = choice.get("_")
            if sel == "rev":
                actions.append(
                    {
                        "action": "insert", "id": rid, "record": conf["rev"], "restored": True,
                        "revision_id": rid,
                        "field_origins": {f: _origin("revision", conf["rev"][f], "rev") for f in FIELDS},
                        "resolutions": {"_": "rev"},
                    }
                )
            elif sel != "cur":
                raise ValueError(f"记录 {rid} 的冲突尚未裁决")

        elif ctype == "add_add":
            # 同 ID 新增内容不同：rev=采用修订版本覆盖，cur=保留当前版本
            sel = choice.get("_")
            if sel == "rev":
                actions.append(
                    {
                        "action": "update", "id": rid, "from": conf["cur"], "to": conf["rev"],
                        "revision_id": rid,
                        "field_origins": {
                            f: _origin("revision", conf["rev"][f], "rev") for f in FIELDS
                        },
                        "resolutions": {"_": "rev"},
                    }
                )
            elif sel != "cur":
                raise ValueError(f"记录 {rid} 的冲突尚未裁决")

    return actions
