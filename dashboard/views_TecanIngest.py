from __future__ import annotations

import os, re, uuid, io
from datetime import date,datetime
from typing import Dict, List, Tuple, Set
from collections import defaultdict, deque

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpRequest, HttpResponse, HttpResponseBadRequest, FileResponse
from django.views.decorators.csrf import csrf_protect
from django.utils.html import escape
from django.utils import timezone
from django.shortcuts import render
from .models import *

import os
import json
import pandas as pd
import re
from icecream import ic


pd.set_option('display.max_columns', None)
pd.set_option('display.width', 0)  # 自动适配控制台宽度

# ======== 工具函数 ========

TECAN_FILENAME_RE = re.compile(r"Plate(\d+)_([0-9]+)", re.I)

def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def _save_upload_to_media(subdir: str, f) -> str:
    """
    把用户上传的文件存到 MEDIA_ROOT/subdir/uuid_原名
    返回：保存后的绝对路径
    """
    base_dir = os.path.join(settings.MEDIA_ROOT, subdir)
    _ensure_dir(base_dir)
    filename = f"{uuid.uuid4()}_{f.name}"
    abs_path = os.path.join(base_dir, filename)
    with open(abs_path, "wb") as w:
        for chunk in f.chunks():
            w.write(chunk)
    return abs_path


def _safe_dirname(name: str) -> str:
    """
    将项目名转换为安全的目录名：
    - 允许：中文、英文字母、数字、点(.)、下划线(_)、短横线(-)
    - 空白统一转为下划线
    - 其他字符替换为下划线，首尾的 ._- 去掉
    """
    s = (name or "").strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9._\-\u4e00-\u9fff]+", "_", s)
    s = s.strip("._-")
    return s or "project"

def _extract_station_pairs_from_upload(f) -> list[tuple[str, str]]:
    """
    从岗位清单表中提取 (主条码, 实验号)。
    兼容：
      - xls/xlsx（优先）
      - csv（兜底）
    允许列名为：主条码/实验号；若不存在则尝试 子条码/实验号（兼容旧格式）
    """
    name = (getattr(f, "name", "") or "").lower()

    # 读取成 DataFrame
    try:
        if name.endswith((".xls", ".xlsx")):
            df = pd.read_excel(f, dtype=str)
        elif name.endswith(".csv"):
            df = pd.read_csv(f, dtype=str, encoding="utf-8", engine="python")
        else:
            # 尝试按 excel 读一次
            df = pd.read_excel(f, dtype=str)
    except Exception:
        # 最后兜底：读 bytes 再让 pandas 猜
        bio = io.BytesIO(f.read())
        df = pd.read_excel(bio, dtype=str)

    # 列名清洗
    df.columns = [str(c).strip() for c in df.columns]

    mb_col = "主条码" if "主条码" in df.columns else ("子条码" if "子条码" in df.columns else None)
    sn_col = "实验号" if "实验号" in df.columns else None
    if not mb_col or not sn_col:
        return []

    pairs = []
    for _, r in df.iterrows():
        mb = (str(r.get(mb_col) or "")).strip()
        sn = (str(r.get(sn_col) or "")).strip()
        if not mb or not sn:
            continue
        pairs.append((mb, sn))
    return pairs


def _save_station_store_daily(pairs: list[tuple[str, str]]) -> dict:
    """
    将 pairs 合并保存到：DOWNLOAD_ROOT/岗位清单/YYYY-MM-DD/station_list.json
    冲突不阻断：记录到 warning（json 最前面）
    返回 summary：{saved, added_pairs, conflicts, path}
    """
    day_date = timezone.localdate()
    day_dir = os.path.join(settings.DOWNLOAD_ROOT, "岗位清单", day_date.strftime("%Y-%m-%d"))
    os.makedirs(day_dir, exist_ok=True)
    path = os.path.join(day_dir, "station_list.json")

    def load_store():
        if not os.path.isfile(path):
            return {"warning": [], "主条码->实验号列表": {}, "实验号->主条码": {}}
        try:
            with open(path, "r", encoding="utf-8") as f:
                d = json.load(f)
            if "warning" not in d:
                d = {"warning": [], **d}
            d.setdefault("主条码->实验号列表", {})
            d.setdefault("实验号->主条码", {})
            return d
        except Exception:
            return {"warning": [{"type": "load_error", "msg": f"读取失败，已重置：{os.path.basename(path)}"}],
                    "主条码->实验号列表": {}, "实验号->主条码": {}}

    def dedup_warning(existing, new_items):
        seen = set()
        out = []
        for w in (existing or []) + (new_items or []):
            if not isinstance(w, dict):
                continue
            key = (w.get("type"), w.get("experiment_no"), w.get("old_main_barcode"), w.get("new_main_barcode"), w.get("msg"))
            if key in seen:
                continue
            seen.add(key)
            out.append(w)
        return out

    store = load_store()
    mb2sn = store.get("主条码->实验号列表", {}) or {}
    sn2mb = store.get("实验号->主条码", {}) or {}

    added_pairs = 0
    conflict_cnt = 0
    warnings_new = []

    # 合并
    for mb, sn in pairs:
        mb = (mb or "").strip()
        sn = (sn or "").strip()
        if not mb or not sn:
            continue

        if sn in sn2mb:
            old_mb = str(sn2mb.get(sn) or "").strip()
            if old_mb and old_mb != mb:
                conflict_cnt += 1
                warnings_new.append({
                    "type": "conflict",
                    "experiment_no": sn,
                    "old_main_barcode": old_mb,
                    "new_main_barcode": mb,
                    "msg": f"实验号 {sn} 当天已映射主条码 {old_mb}，本次上传为 {mb}，已忽略本次冲突行"
                })
                continue
        else:
            sn2mb[sn] = mb

        exist_list = mb2sn.get(mb, [])
        if not isinstance(exist_list, list):
            exist_list = [str(exist_list)]
        exist_set = set(str(x).strip() for x in exist_list if str(x).strip())

        if sn not in exist_set:
            exist_list.append(sn)
            added_pairs += 1
        mb2sn[mb] = exist_list

    store["warning"] = dedup_warning(store.get("warning", []), warnings_new)
    store["主条码->实验号列表"] = mb2sn
    store["实验号->主条码"] = sn2mb

    generated_at = timezone.localtime().strftime("%Y-%m-%d %H:%M:%S")
    need_write = (added_pairs > 0) or (conflict_cnt > 0) or (not os.path.isfile(path))
    if need_write:
        ordered_out = {
            "生成时间": generated_at,
            "warning": store.get("warning", []),
            "主条码->实验号列表": store.get("主条码->实验号列表", {}),
            "实验号->主条码": store.get("实验号->主条码", {}),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(ordered_out, f, ensure_ascii=False, indent=2)

    return {"saved": need_write, "added_pairs": added_pairs, "conflicts": conflict_cnt, "path": path}


def _read_raw_csv_lines(path: str) -> tuple[str, list[str], list[list[str]]]:
    """
    返回 (header_line, data_lines, parsed_rows)
    - header_line: 第一行（原样字符串）
    - data_lines: 第二行开始的每一行原文本
    - parsed_rows: data_lines 按 ';' 分割后的二维列表
    """
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = [ln.strip() for ln in f.read().splitlines() if ln.strip() != ""]
    if not lines:
        raise ValueError("CSV为空")
    header_line = lines[0]
    data_lines = lines[1:]
    parsed_rows = [[p.strip() for p in ln.split(";")] for ln in data_lines]
    return header_line, data_lines, parsed_rows


def _parse_tecan_csv_abs(path: str) -> pd.DataFrame:
    """
    解析无表头的 Tecan 扫码 CSV 文件。
    逻辑与 R 版一致：
      - 首行忽略（视为批次/板号信息）
      - 每行以 ';' 分隔
      - 第1列 -> GridPos
      - 第3列 -> TipNumber（若不足3列则 NA）
      - 最后一列 -> SRCTubeID
      - MainBarcode = SRCTubeID.split('-')[0]
    """

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = [ln.strip() for ln in f.read().splitlines() if ln.strip() != ""]

    if not lines:
        raise ValueError("CSV为空")

    # 跳过首行（批次号）
    data_lines = lines[1:]
    if not data_lines:
        raise ValueError("CSV无数据行")

    parsed_rows = []
    for i, line in enumerate(data_lines, start=1):
        parts = [p.strip() for p in line.split(";")]
        if not parts:
            continue

        # 按 R 逻辑取列
        gridpos_raw = parts[0] if len(parts) >= 1 else ""
        tip_raw = parts[2] if len(parts) >= 3 else ""
        src_raw = parts[-1] if len(parts) >= 1 else ""

        # 从文本中提取数字
        def extract_int(s):
            m = re.search(r"(\d+)", str(s))
            return int(m.group(1)) if m else None

        parsed_rows.append({
            "RowID": i,
            "SRCTubeID": str(src_raw),
            "MainBarcode": str(src_raw).split("-", 1)[0],
            "GridPos": extract_int(gridpos_raw),
            "TipNumber": extract_int(tip_raw),
        })

    if not parsed_rows:
        raise ValueError("CSV无有效数据行")

    return pd.DataFrame(parsed_rows)


def _collect_history_mainbarcodes(processed_dir: str) -> Set[str]:
    """
    遍历“当天”的 processed 目录下 *.csv，按 “首行跳过 + ';' 分割 + 最后一列为 SRCTubeID” 解析，
    汇总历史 MainBarcode 集合。
    """
    s: Set[str] = set()
    if not os.path.isdir(processed_dir):
        return s

    for name in os.listdir(processed_dir):
        if not name.lower().endswith(".csv"):
            continue
        p = os.path.join(processed_dir, name)
        try:
            with open(p, "r", encoding="utf-8", errors="ignore") as f:
                lines = [ln.strip() for ln in f.read().splitlines() if ln.strip() != ""]
            if len(lines) <= 1:
                continue
            for line in lines[1:]:  # 跳过首行
                parts = [x.strip() for x in line.split(";")]
                if not parts:
                    continue
                src = parts[-1] if len(parts) >= 1 else ""
                main = str(src).split("-", 1)[0]
                if main:
                    s.add(main)
        except Exception:
            # 忽略坏文件
            continue
    return s


def _detect_conflicts(df: pd.DataFrame, history: Set[str]) -> Dict[str, List[Dict[str, str]]]:
    """
    返回 { intra: [...], cross: [...] }
    - intra：本次文件内重复（逐行列出）
    - cross：跨文件重复（列出重复的 MainBarcode 值）
    """
    res = {"intra": [], "cross": []}

    # === 新：本次内重复（细分规则）
    # A. 主码相同且子码不同：仅标记“后续出现”的重复
    has_multi_subs = df.groupby("MainBarcode")["SRCTubeID"].transform(lambda s: s.nunique() > 1)
    diff_sub_later_mask = has_multi_subs & df["MainBarcode"].duplicated(keep="first")

    # B. 主码和子码均相同：标记所有重复（包括第一次出现）
    exact_dup_all = df["SRCTubeID"].duplicated(keep=False)

    dup_mask = diff_sub_later_mask | exact_dup_all

    if dup_mask.any():
        dup_rows = df[dup_mask].sort_values(["MainBarcode", "RowID"])
        for _, r in dup_rows.iterrows():
            res["intra"].append({
                "RowID": int(r["RowID"]),
                "MainBarcode": str(r["MainBarcode"]),
                "SRCTubeID": str(r["SRCTubeID"]),
            })

    # 跨文件重复
    this_set = set(df["MainBarcode"].dropna().astype(str))
    cross = sorted(this_set & set(history))
    for mb in cross:
        res["cross"].append({"MainBarcode": mb})

    return res


# 将扫码结果表写入processed文件夹（无重复条码时）
def _write_processed_copy_from_original(saved_scan_abs: str, processed_dir: str) -> str:
    """
    无冲突：把原始 CSV 原样复制到当天 processed 目录，文件名保持不变。
    """
    _ensure_dir(processed_dir)
    out_path = os.path.join(processed_dir, os.path.basename(saved_scan_abs))
    with open(saved_scan_abs, "rb") as src, open(out_path, "wb") as dst:
        dst.write(src.read())
    return out_path


def _write_processed_with_row_replacements(
    header_line: str,
    parsed_rows: list[list[str]],
    processed_dir: str,
    original_filename: str,
    fix_map_by_rowid: dict[int, tuple[str, str]],  # {RowID -> (old_srctubeid, new_srctubeid)}
) -> str:
    """
    只修改给定 RowID 的行：若该行最后一列等于 old_srctubeid，则替换为 new_srctubeid；
    其他行保持不变。RowID 与 R 脚本一致，从 1 开始计数（对应 data_lines 的行号）。
    """
    os.makedirs(processed_dir, exist_ok=True)

    # 应用替换（RowID 从 1 开始；parsed_rows 从 0 开始）
    for rowid, (old_v, new_v) in fix_map_by_rowid.items():
        idx = rowid - 1
        if 0 <= idx < len(parsed_rows) and parsed_rows[idx]:
            if parsed_rows[idx][-1] == old_v:
                parsed_rows[idx][-1] = new_v  # 只改“最后一列 SRCTubeID”

    out_name = original_filename[:-4] + "_processed.csv" if original_filename.lower().endswith(".csv") else original_filename + "_processed.csv"
    out_path = os.path.join(processed_dir, out_name)

    with open(out_path, "w", encoding="utf-8", errors="ignore") as f:
        f.write(header_line.strip() + "\n")
        for fields in parsed_rows:
            f.write(";".join(fields) + "\n")

    return out_path


def _validate_new_srctubeids(
    original_dups: list[dict],           # [{'RowID':..., 'SRCTubeID':..., 'GridPos':..., 'TipNumber':...}, ...]
    new_map: dict[str, str],             # {old_srctubeid -> new_srctubeid}
    all_main_barcodes: set[str],         # 当前文件 + 当天processed 的并集
) -> tuple[bool, str]:
    """
    规则：
    - old/new 必填；old 必须等于待修正的 SRCTubeID
    - new 必须包含 '-'，并取 main_part = new.split('-', 1)[0]
    - main_part 不得出现在 all_main_barcodes（历史与本次合并）
    - 多个 new 之间 main_part 也不得互相重复
    """
    seen_new_main: set[str] = set()
    dups_by_old = {d['SRCTubeID']: d for d in original_dups}

    for old, new in new_map.items():
        if not old or not new:
            return False, f"请填写完整：原/新 SRCTubeID 不能为空（问题条目：{old or '（空）'}）"
        if old not in dups_by_old:
            return False, f"原 SRCTubeID 输入不在需修正列表中：{old}"
        if "-" not in new:
            return False, f"新的 SRCTubeID 必须包含 '-'：{new}"
        main_part = new.split("-", 1)[0].strip()
        if not main_part:
            return False, f"新的 SRCTubeID 主码为空：{new}"
        if main_part in all_main_barcodes:
            return False, f"新的主码 {main_part} 已存在于当日历史或本次文件中（对应 {old}）"
        if main_part in seen_new_main:
            return False, f"输入的多条新 SRCTubeID 存在主码重复：{main_part}"
        seen_new_main.add(main_part)

    return True, ""


# ======== 视图函数（Step1：解析 + 重复条码检查） ========

@csrf_protect
def tecaningest(request: HttpRequest) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseBadRequest("仅支持POST")

    # === 新增：当天目录（YYYYMMDD） ===
    today = datetime.now().strftime("%Y%m%d")

    # 从表单获取项目信息（前端 Tecan 页面会提交 project_id / project_name）
    project_id   = request.POST.get("project_id", "").strip()
    project_name = request.POST.get("project_name", "").strip()
    project_dir  = _safe_dirname(project_name or project_id or "project")

    base_dir      = os.path.join(settings.MEDIA_ROOT, "tecan", today, project_dir)
    original_dir  = os.path.join(base_dir, "original")
    processed_dir = os.path.join(base_dir, "processed")
    station_dir   = os.path.join(base_dir, "station")
    for d in (original_dir, processed_dir, station_dir):
        _ensure_dir(d)

    station_file = request.FILES.get("station_list") 
    scan_file    = request.FILES.get("scan_result")

    if not scan_file:
        return HttpResponseBadRequest("请上传‘扫码结果表’(CSV)")

    # === 修改：保存到“当天/original” ===
    saved_scan_abs = _save_upload_to_media(f"tecan/{today}/{project_dir}/original", scan_file)
    station_save_summary = None
    if station_file:
        station_map = _load_station_map_from_upload(station_file)  # ← 新函数
        request.session["tecan_station_map"] = station_map

         # 【新增】保存每日岗位清单映射库（downloads/岗位清单/..）
        try:
            pairs = _extract_station_pairs_from_upload(station_file)
            station_save_summary = _save_station_store_daily(pairs)
        except Exception as e:
            # 不阻断主流程
            station_save_summary = {"saved": False, "added_pairs": 0, "conflicts": 0, "error": str(e)}
    else:
        request.session["tecan_station_map"] = {}

    # 把项目信息写入 session，给 Step2 使用
    request.session["tecan_project_dir"] = project_dir
    request.session["tecan_project_id"]  = project_id
    request.session["tecan_project_name"]    = project_name
    request.session["tecan_instrument_num"]  = request.POST.get("instrument_num", "").strip()
    request.session["tecan_systerm_num"]     = request.POST.get("systerm_num", "").strip()

    request.session["tecan_injection_plate"] = request.POST.get("injection_plate", "").strip()

    # 1) 解析 CSV
    try:
        df = _parse_tecan_csv_abs(saved_scan_abs)
    except Exception as e:
        return HttpResponseBadRequest(f"CSV解析失败：{e}")

    # 冲突检测 + 渲染 
    history = _collect_history_mainbarcodes(processed_dir)

    # === 新：文件内重复的细分规则 ===
    # 1) 主码相同且子码不同：仅标记“后续出现”的重复
    has_multi_subs = df.groupby("MainBarcode")["SRCTubeID"].transform(lambda s: s.nunique() > 1)
    diff_sub_later_mask = has_multi_subs & df["MainBarcode"].duplicated(keep="first")

    # 2) 主码和子码均相同：标记所有重复（包括第一次出现）
    exact_dup_all = df["SRCTubeID"].duplicated(keep=False)

    # 合并两种“文件内重复”的掩码
    mask_infile_new = diff_sub_later_mask | exact_dup_all

    # 跨文件重复（沿用旧逻辑）
    mask_cross = df["MainBarcode"].isin(history)

    # 去除含有 '$' 的主码
    mask_dollar = df["MainBarcode"].astype(str).str.contains(r"\$")

    # 最终需要人工修正的集合 = （新文件内重复 ∪ 跨文件重复） 且 不含 '$'
    intra_or_cross_mask = (mask_infile_new | mask_cross) & (~mask_dollar)

    need_fix_df = (
        df[intra_or_cross_mask]
        # .drop_duplicates(subset=["SRCTubeID"])
        .sort_values(["GridPos", "TipNumber"], na_position="last")
    )

    has_conflict = not need_fix_df.empty

    if has_conflict:
        # 构造模板需要的数据
        dups = [{
            "RowID":       int(r["RowID"]),
            "SRCTubeID":   str(r["SRCTubeID"]),
            "MainBarcode": str(r["MainBarcode"]),
            "GridPos":     int(r["GridPos"]) if pd.notna(r["GridPos"]) else None,
            "TipNumber":   int(r["TipNumber"]) if pd.notna(r["TipNumber"]) else None,
        } for _, r in need_fix_df.iterrows()]

        # 仅用于页面提示：哪些 MainBarcode 是“跨文件重复”
        cross_mains = sorted(set(
            df.loc[mask_cross & (~mask_dollar), "MainBarcode"].astype(str)  # ← 提示也过滤
        ))

        # 把“原始文件相对路径/当天日期”等存 Session（你原来的做法保持不变）
        rel_path = os.path.relpath(saved_scan_abs, settings.MEDIA_ROOT).replace(os.sep, "/")
        request.session["tecan_pending_file"] = rel_path
        request.session["tecan_pending_date"] = today
        request.session.modified = True

        return render(request, "dashboard/sampling/Tecan_duplicates.html", {
            "dups": dups,                # 需要人工填写新 SRCTubeID 的那批行（= R 的 ordered_duplicates）
            "cross_mains": cross_mains,  # 跨文件重复主码，仅提示
            "today": today,
            "project_id": project_id,
            "original_filename": os.path.basename(saved_scan_abs),
            "injection_plate": request.POST.get("injection_plate", "").strip(),
        })

    # === 修改：无冲突 → 原样复制到“当天/processed/”，保持原 CSV 格式 ===  
    out_path = _write_processed_copy_from_original(saved_scan_abs, processed_dir)

    return _render_tecan_process_result(
        request,
        today=today,
        csv_abs_path=out_path,
        project_id=project_id,
        station_save_summary=station_save_summary,   # ✅ 新增
    )


@csrf_protect
def tecan_resolve_duplicates(request: HttpRequest) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseBadRequest("仅支持POST")

    # 先从 POST 取；取不到再从 session 兜底
    project_id = (request.POST.get("project_id")
                  or request.session.get("tecan_project_id"))

    if "injection_plate" in request.POST:
        request.session["tecan_injection_plate"] = request.POST.get("injection_plate", "").strip()
    
    rel_path = request.session.get("tecan_pending_file")
    today    = request.session.get("tecan_pending_date")
    if not rel_path or not today:
        return HttpResponseBadRequest("未找到待处理文件，请返回重新上传。")

    saved_scan_abs = os.path.join(settings.MEDIA_ROOT, rel_path)

    # 读取项目目录（优先 session，回退表单）
    project_dir = (
        request.session.get("tecan_project_dir")
        or _safe_dirname(request.POST.get("project_name", "") or request.POST.get("project_id", ""))
        or "project"
    )

    base_dir      = os.path.join(settings.MEDIA_ROOT, "tecan", today, project_dir)
    processed_dir = os.path.join(base_dir, "processed")
    _ensure_dir(processed_dir)

    # 1) 解析当前文件，准备校验
    try:
        df = _parse_tecan_csv_abs(saved_scan_abs)
        header_line, data_lines, parsed_rows = _read_raw_csv_lines(saved_scan_abs)
    except Exception as e:
        return HttpResponseBadRequest(f"CSV解析失败：{e}")

    # 2) 收集当天历史主码，并构造 all_main_barcodes（当前 + 历史）
    history = _collect_history_mainbarcodes(processed_dir)
    all_main_barcodes = set(df["MainBarcode"].astype(str).tolist()) | set(history)

    # 3) 从表单收集修正映射：字段名 new_<index>，携带 hidden old_<index>
    #    或者你也可以按 SRCTubeID 为键：new_for_<srctubeid>
    #    下面按“new_<i> + old_<i>”示例：
    # === A) 收集映射：{ RowID -> (old_srctubeid, new_srctubeid) } ===
    fix_map_by_rowid: dict[int, tuple[str, str, str]] = {}
    i = 0
    while True:
        rk, ok, cok, nk = f"rowid_{i}", f"old_{i}", f"confirm_old_{i}", f"new_{i}"
        if rk not in request.POST and ok not in request.POST and nk not in request.POST and cok not in request.POST:
            break
        rowid = request.POST.get(rk, "").strip()
        old_v = request.POST.get(ok, "").strip()
        confirm_old_v = request.POST.get(cok, "").strip()
        new_v = request.POST.get(nk, "").strip()
        if rowid:
            fix_map_by_rowid[int(rowid)] = (old_v, confirm_old_v, new_v)
        i += 1
    if not fix_map_by_rowid:
        return HttpResponseBadRequest("没有收到任何修正项")


    # === B) 校验（逐行构造错误并返回原页高亮） ===
    rows_for_fix = df[df["RowID"].isin(fix_map_by_rowid.keys())].copy()
    if rows_for_fix.empty:
        return HttpResponseBadRequest("修正的 RowID 不存在于当前文件")

    history = _collect_history_mainbarcodes(processed_dir)
    all_main = set(df["MainBarcode"].astype(str)) | set(history)

    error_rows = set()      # 需要高亮的行(RowID)
    error_msgs = {}         # RowID -> 错误消息（用于“确认原值”不一致）
    new_bad = set()         # RowID -> 新值问题（例如没'-'、主码冲突）
    new_msgs = {}

    seen_new_main: set[str] = set()
    for rowid, (old_v, confirm_old_v, new_v) in fix_map_by_rowid.items():
        r = rows_for_fix.loc[rows_for_fix["RowID"] == rowid].iloc[0]
        real_old = str(r["SRCTubeID"])
        # 1) 原值一致性：必须“确认输入 == 实际原值”
        if confirm_old_v != real_old:
            error_rows.add(rowid)
            error_msgs[rowid] = f"原 SRCTubeID（确认）与实际原值不一致，应为：{real_old}"
            continue  # 这一行先不过后续校验

        # 2) 旧隐藏值兜底（正常情况下等于 real_old）
        if old_v != real_old:
            error_rows.add(rowid)
            error_msgs[rowid] = f"提交的原 SRCTubeID 与实际原值不一致，应为：{real_old}"
            continue

        # 3) 新值格式/主码校验
        if "-" not in new_v:
            new_bad.add(rowid); new_msgs[rowid] = "新 SRCTubeID 必须包含 '-'"
            continue
        new_main = new_v.split("-", 1)[0].strip()
        if not new_main:
            new_bad.add(rowid); new_msgs[rowid] = "新 SRCTubeID 主码为空"
            continue
        if new_main in all_main:
            new_bad.add(rowid); new_msgs[rowid] = f"新的主码 {new_main} 已存在（当日历史或本次文件）"
            continue
        if new_main in seen_new_main:
            new_bad.add(rowid); new_msgs[rowid] = f"多个输入的新主码重复：{new_main}"
            continue
        seen_new_main.add(new_main)

    # 有任何错误 → 回到重复页并高亮
    if error_rows or new_bad:
        # 重新排序，保持与首次渲染一致
        need_fix_df = rows_for_fix.sort_values(["GridPos", "TipNumber"], na_position="last")

        # 先把用户输入的值收集成“按行序号”的映射
        confirm_vals = {}
        new_vals = {}
        i = 0
        while True:
            rk = f"rowid_{i}"
            if rk not in request.POST:
                break
            confirm_vals[i] = request.POST.get(f"confirm_old_{i}", "")
            new_vals[i]     = request.POST.get(f"new_{i}", "")
            i += 1

        # 构造 dups，并把每行的“回填值”塞进去（索引以 forloop.counter0 的顺序对齐）
        dups = []
        for i, (_, r) in enumerate(need_fix_df.iterrows()):
            dups.append({
                "RowID": int(r["RowID"]),
                "SRCTubeID": str(r["SRCTubeID"]),
                "MainBarcode": str(r["MainBarcode"]),
                "GridPos": int(r["GridPos"]) if pd.notna(r["GridPos"]) else None,
                "TipNumber": int(r["TipNumber"]) if pd.notna(r["TipNumber"]) else None,
                "confirm_val": confirm_vals.get(i, ""),
                "new_val": new_vals.get(i, ""),
            })

        return render(request, "dashboard/sampling/Tecan_duplicates.html", {
            "dups": dups,
            "cross_mains": sorted(set(df.loc[df["MainBarcode"].isin(history), "MainBarcode"].astype(str))),
            "today": today,
            "project_id": project_id,
            "injection_plate": (request.POST.get("injection_plate", "").strip() or (request.session.get("tecan_injection_plate") or "").strip()),
            "original_filename": os.path.basename(saved_scan_abs),
            # —— 改成 4 个简单变量，模板里直接判断，不再出现 `.get` 写法 —— 
            "errors_rowids": error_rows,      # 需高亮行（确认不一致/隐藏原值不一致）
            "errors_msgs": error_msgs,        # 行 -> “确认不一致/原值不一致”提示
            "errors_new_bad": new_bad,        # 新值错误行
            "errors_new_msgs": new_msgs,      # 行 -> 新值错误提示
        })


    # === C) 写 processed：保留首行，仅改这些 RowID 的“最后一列 SRCTubeID” ===
    header_line, data_lines, parsed_rows = _read_raw_csv_lines(saved_scan_abs)

    # 将三元组 (old, confirm_old, new) 瘦身为二元组 (old, new)，以适配写文件函数
    fix_pairs = {rid: (old, new) for rid, (old, _confirm_old, new) in fix_map_by_rowid.items()}

    out_path = _write_processed_with_row_replacements(
        header_line=header_line,
        parsed_rows=parsed_rows,
        processed_dir=processed_dir,
        original_filename=os.path.basename(saved_scan_abs),
        fix_map_by_rowid=fix_pairs,  # ← 只传 (old, new)
    )

    return _render_tecan_process_result(
        request,
        today=today,
        csv_abs_path=out_path,
        project_id=project_id,
        station_save_summary=None, 
    )


@csrf_protect
def tecan_manage_processed_file(request: HttpRequest) -> HttpResponse:
    """
    删除或移动 processed 下的文件
    入参（POST，JSON 或表单）：
      - project_id / project_name
      - today（可选，默认当天）
      - filename（必填）
      - action ∈ {'delete','move'}
    行为：
      - delete: 直接删除 processed/<filename>
      - move:   将 processed/<filename> 移动到 backup/<filename>
    """
    if request.method != "POST":
        return HttpResponseBadRequest("仅支持POST")

    project_name = (request.POST.get("project_name") or "").strip()
    project_id   = (request.POST.get("project_id") or "").strip()
    today        = (request.POST.get("today") or timezone.localtime().strftime("%Y%m%d"))
    filename     = (request.POST.get("filename") or "").strip()
    action       = (request.POST.get("action") or "").strip().lower()

    if not filename or action not in {"delete", "move"}:
        return HttpResponseBadRequest("参数错误")

    project_dir  = _safe_dirname(project_name or project_id or "project")
    base_dir     = os.path.join(settings.MEDIA_ROOT, "tecan",today,project_dir)
    processed_dir= os.path.join(base_dir, "processed")
    backup_dir   = os.path.join(base_dir, "backup")

    # 安全拼接，防止越权
    src_path = os.path.normpath(os.path.join(processed_dir, filename))
    if not src_path.startswith(os.path.abspath(processed_dir) + os.sep):
        return HttpResponseBadRequest("非法文件名")

    if not os.path.exists(src_path) or not os.path.isfile(src_path):
        return HttpResponseBadRequest("文件不存在")

    try:
        if action == "delete":
            os.remove(src_path)
        else:  # move
            os.makedirs(backup_dir, exist_ok=True)
            dst_path = os.path.normpath(os.path.join(backup_dir, filename))
            if not dst_path.startswith(os.path.abspath(backup_dir) + os.sep):
                return HttpResponseBadRequest("非法目标路径")
            os.replace(src_path, dst_path)
    except Exception as e:
        return HttpResponseBadRequest(f"操作失败：{e}")

    return JsonResponse({"ok": True})


def tecan_download_processed_file(request: HttpRequest) -> HttpResponse:
    """
    下载 processed 下的单个文件
    入参（GET）：
      - project_id / project_name
      - today（可选，默认当天）
      - filename（必填）
    """
    if request.method != "GET":
        return HttpResponseBadRequest("仅支持GET")

    project_name = (request.GET.get("project_name") or "").strip()
    project_id   = (request.GET.get("project_id") or "").strip()
    today        = (request.GET.get("today") or timezone.localtime().strftime("%Y%m%d"))
    filename     = (request.GET.get("filename") or "").strip()

    if not filename:
        return HttpResponseBadRequest("参数错误")

    # 目录结构与 tecan_list_processed_files / tecan_manage_processed_file 保持一致 :contentReference[oaicite:2]{index=2}
    project_dir  = _safe_dirname(project_name or project_id or "project")
    base_dir     = os.path.join(settings.MEDIA_ROOT, "tecan", today, project_dir)
    processed_dir= os.path.join(base_dir, "processed")

    # 安全拼接，防止越权
    src_path = os.path.normpath(os.path.join(processed_dir, filename))
    if not src_path.startswith(os.path.abspath(processed_dir) + os.sep):
        return HttpResponseBadRequest("非法文件名")

    if not os.path.exists(src_path) or not os.path.isfile(src_path):
        return HttpResponseBadRequest("文件不存在")

    # 返回文件下载
    return FileResponse(open(src_path, "rb"), as_attachment=True, filename=filename)


@csrf_protect
def tecan_list_processed_files(request: HttpRequest) -> HttpResponse:
    """
    列出 media/tecan/<today>/<project_dir>/processed 下的所有文件
    入参（GET）：
      - project_id（可选）
      - project_name（可选）
      - today（可选；默认当天，格式 YYYYMMDD）
    返回：{ ok: true, files: [{name, size, mtime}] }
    """
    if request.method != "GET":
        return HttpResponseBadRequest("仅支持GET")

    # 1) 解析参数
    project_name = (request.GET.get("project_name") or "").strip()
    project_id   = (request.GET.get("project_id") or "").strip()
    today        = (request.GET.get("today") or timezone.localtime().strftime("%Y%m%d"))

    # 2) 计算项目目录名（与上传逻辑一致）
    try:
        project_dir = _safe_dirname(project_name or project_id or "project")
    except NameError:
        # 若你未按之前建议添加 _safe_dirname，可临时兜底
        def _safe_dirname(s): 
            import re
            s = (s or "").strip()
            s = re.sub(r"\s+", "_", s)
            s = re.sub(r"[^A-Za-z0-9._\-\u4e00-\u9fff]+", "_", s)
            s = s.strip("._-")
            return s or "project"
        project_dir = _safe_dirname(project_name or project_id or "project")

    processed_dir = os.path.join(settings.MEDIA_ROOT, "tecan", today, project_dir, "processed")
    if not os.path.isdir(processed_dir):
        return JsonResponse({"ok": True, "files": []})

    # 3) 列出文件
    files = []
    for name in sorted(os.listdir(processed_dir)):
        fpath = os.path.join(processed_dir, name)
        if not os.path.isfile(fpath):
            continue
        st = os.stat(fpath)
        files.append({
            "name": name,
            "size": st.st_size,
            "mtime": int(st.st_mtime),
        })
    return JsonResponse({"ok": True, "files": files})


# 读取当天 station（岗位清单）映射（子条码→实验号）
def _load_station_map_for_today(today: str, project_dir: str) -> dict[str, str]:
    """
    读取 media/tecan/{today}/station 下“第一份”岗位清单，提取 子条码->实验号 映射。
    要求表头包含：'子条码', '实验号'
    """
    base_dir = os.path.join(settings.MEDIA_ROOT, "tecan", today, project_dir, "station")
    if not os.path.isdir(base_dir):
        return {}
    candidates = [os.path.join(base_dir, n) for n in os.listdir(base_dir)
                  if os.path.isfile(os.path.join(base_dir, n)) and n.lower().endswith((".xlsx", ".xls"))]
    if not candidates:
        return {}
    candidates.sort()
    try:
        df = pd.read_excel(candidates[0])
        cols = {c.strip(): c for c in df.columns if isinstance(c, str)}
        if "子条码" in cols and "实验号" in cols:
            sub = df[[cols["子条码"], cols["实验号"]]].dropna()
            sub.columns = ["barcode", "exp"]
            mp = {}
            for _, r in sub.iterrows():
                b = str(r["barcode"]).strip()
                e = str(r["exp"]).strip()
                if b and e:
                    mp[b] = e
            return mp
    except Exception:
        pass
    return {}


def _load_station_map_from_upload(f) -> dict[str, str]:
    """
    直接从用户本次上传的岗位清单文件对象读取“子条码 -> 实验号”映射。
    要求表头包含：'子条码', '实验号'
    - f: InMemoryUploadedFile / TemporaryUploadedFile
    """
    try:
        import pandas as pd
        df = pd.read_excel(f)
        cols = {c.strip(): c for c in df.columns if isinstance(c, str)}
        if "子条码" not in cols or "实验号" not in cols:
            return {}
        sub = df[[cols["子条码"], cols["实验号"]]].dropna()
        sub.columns = ["barcode", "exp"]
        mp = {}
        for _, r in sub.iterrows():
            b = str(r["barcode"]).strip()
            e = str(r["exp"]).strip()
            if b and e:
                mp[b] = e
        return mp
    except Exception:
        return {}


# 根据文件名解析 plate_number 与 start_offset  
def _parse_plate_meta_by_filename(filename: str) -> tuple[int, int]:
    """
    由文件名的 Plate(\d+)_(\d+) 得到 (plate_number, start_offset)
    """
    m = TECAN_FILENAME_RE.search(filename or "")
    if not m:
        return 1, 1
    plate_number = int(m.group(1))
    start_offset = int(m.group(2))
    return max(1, plate_number), max(1, start_offset)



# === 新增：根据板号/偏移计算定位孔坐标 ===
def _locator_coord_for_plate(plate_number: int, start_offset: int) -> tuple[str, int]:
    """
    返回 (row_letter, col_num)
    - 行：按 plate 轮转 A..H
    - 列：基础列 = 3 + floor((plate-1)/8)，再叠加 start_offset-1
    """
    row_letter = _PLATE_ROWS[(max(1, plate_number)-1) % 8]
    base_col = 3 + ((max(1, plate_number)-1) // 8)
    col_num = base_col + max(1, start_offset) - 1
    return row_letter, col_num


def _apply_locator_shift_for_clinicals(
    clinical_cells: list[tuple[str, int, str, str]],  # ← ★ 添加第4个元素：barcode
    plate_number: int,
    start_offset: int,
) -> list[tuple[str, int, str, str]]:  # ← ★ 返回值也添加 barcode
    """
    按'定位孔随板号'规则对临床样本做位移（沿定位孔所在列'纵向'）。
    - base_col = 3 + floor((plate-1)/8) + (start_offset-1)
    - k = (plate-1) % 8   # 0..7 -> A..H
    处理：
      1) (A, base_col) → vertical_prev(A, base_col)  # A→H 且列-1
      2) (B..rows[k], base_col) 逐个上移一行（B→A，C→B，…）
      3) (rows[k], base_col) 留空（给定位孔）
    """
    base_col = 3 + ((max(1, plate_number) - 1) // 8) + (max(1, start_offset) - 1)
    k = (max(1, plate_number) - 1) % 8  # 0..7

    # ★ 修改：建索引时同时存储 (text, barcode)
    mp: dict[tuple[str, int], tuple[str, str]] = {}
    for r, c, s, bc in clinical_cells:
        mp[(r, c)] = (s, bc)  # (实验号, 条码)

    # 纵向序中的"前一个孔"：A→H 且列-1；其余行向上同列
    def vertical_prev(row_letter: str, col_num: int) -> tuple[str, int]:
        if row_letter != "A":
            prev_row = _PLATE_ROWS[_PLATE_ROWS.index(row_letter) - 1]
            return prev_row, col_num
        return "H", col_num - 1

    # 1) A, base_col → 前一个纵向孔
    src = ("A", base_col)
    if src in mp:
        dst = vertical_prev("A", base_col)
        if dst[1] >= 1:           # 列下限保护
            mp[dst] = mp[src]
        del mp[src]

    # 2) B..rows[k] 逐个上移（B→A、C→B、…）
    for i in range(1, k + 1):
        src = (_PLATE_ROWS[i], base_col)
        dst = (_PLATE_ROWS[i - 1], base_col)
        if src in mp:
            mp[dst] = mp[src]
            del mp[src]

    # 回写为列表
    out = []
    for (r, c), (s, bc) in mp.items():
        out.append((r, c, s, bc))  # ← ★ 返回4元组
    return out



# 获取 QC 名称表（不再读 mapping_file）
def _get_qc_name_table(file_basename: str) -> list[str]:
    """
    返回按“层级顺序”排列的 QC 名称表，例如：
    - CA 默认: ["QC00001663","QC00001664","QC00001665"]
    - ZMNs 默认: ["QC00005101","QC00005102"]  # 示例
    你可以按项目再细分；这里只做最小可用。
    """
    name = file_basename.upper()
    if "ZMN" in name:   # ZMNs
        return ["QC00001663", "QC00001665"]  # 示例，可按你提供的图3替换
    # CA/其他
    return ["QC00001663", "QC00001664", "QC00001665"]


def _get_qc_names_from_mapping(mapping_path: str) -> list[str]:
    """
    从对应关系表（mapping_file）的『工作清单』工作表读取 QC 名称列表：
    - 取 Code 以 'QC' 开头的行
    - 返回 Name 列，保持原表顺序
    """
    try:
        df = pd.read_excel(mapping_path, sheet_name="工作清单")
        df = df.copy()
        df["Code"] = df["Code"].astype(str)
        qc_names = df.loc[df["Code"].str.startswith("QC", na=False), "Name"].astype(str).tolist()
        # 兜底：保证至少返回一个占位
        return qc_names or ["QC"]
    except Exception:
        return ["QC"]


# 生成“STD/QC”列表（纵向从 A1 开始）
def _build_curve_and_qc_cells(curve_points: int, std_names_pool: list[str], qc_groups: int, qc_levels: int, qc_names_pool: list[str]) -> list[dict]:
    """
    生成一个顺序列表，每个元素包含：
    { 'label': 显示文本, 'kind': 'STD'|'QC' }
    - STD: STD0 ~ STD{curve_points}
    - QC:  按 qc_groups * qc_levels 生成，名称来自 qc_names_pool（来自“工作清单”）
    """
    items = []
    STDpool = (std_names_pool or ["STD"])

    # 把‘nan’替换为空字符串
    STDpool = ['' if x == 'nan' else x for x in STDpool]

    # STD（从 0 到 curve_points）
    for i in range(curve_points + 1):
        items.append({"label": STDpool[i], "kind": "STD"})

    # QC（循环使用 qc_names_pool）
    pool = (qc_names_pool or ["QC"])
    for g in range(1, qc_groups + 1):
        for lv in range(1, qc_levels + 1):
            name_idx = (lv - 1) % len(pool)
            items.append({"label": pool[name_idx], "kind": "QC", "group": g, "level": lv})
    return items


def format_vialpos_column(df: pd.DataFrame, colname: str = "VialPos"):
    """
    统一格式化 VialPos 列：
    - 空值 -> ""
    - 1.0 -> "1"
    - 86.0 -> "86"
    - 3 -> "3"
    """
    def fmt(v):
        if pd.isna(v):
            return ""                     # 也可以 return "nan" 看你需求
        try:
            # 能转成数字的全部按整数格式输出
            return str(int(float(v)))
        except Exception:
            # 极少数无法解析成数字的，保持原样
            return str(v)

    df[colname] = df[colname].map(fmt)
    return df


def _apply_mapping_to_table(
    mapping_df: pd.DataFrame,
    worklist_table: pd.DataFrame,
    *,
    name_to_barcodes: dict[str, deque],
    barcode_to_well: dict[str, tuple[str, int]],
    locator_info: dict | None = None,
    project_name: str | None = None,
    injection_plate: str | None = None,
    instrument_name: str | None = None,
    set_name: str | None = None,       
    output_file: str | None = None      
):
    """
    按 '上机列表' 映射模板，把占位符填入 worklist_table（第一列已是完整 SampleName_list）。
    兼容规则：
      - sample_key = 'DB*'/'Test*'/'STD3'/'*' 等
      - 列值 = 常量 / '*'(镜像第一列) / {{Well_Number}} / {{Well_Position}}
      - Tecan 特有：'----------' 代表定位孔（仅第1个生效，其他同名删掉）
    """
    df = worklist_table
    headers = list(df.columns)
    first_col = headers[0]

    ic(barcode_to_well)

    loc_display = (locator_info or {}).get("display_name")  # e.g. "X3"

    # 识别孔号/孔位列（尽量兼容多命名）
    WELLNUM_COLS = {"Well_Number", "Vial position", "VialPos", "样品瓶"}
    WELLPOS_COLS = {"Well_Position", "SourcePositionID", "TargetPositionID", "Vial position"}
    col_wellnum = next((c for c in headers if c in WELLNUM_COLS), None)
    col_wellpos = next((c for c in headers if c in WELLPOS_COLS), None)

    # 记录需要镜像第一列的列（模板值 = '*'）
    mirror_cols = set()

    # 第一次使用定位孔的标记 & 待删除行
    locator_first_used = False
    rows_to_drop = []

    # 按模板逐行套规则（与 NIMBUS 同构）
    for _, map_row in mapping_df.iterrows():
        sample_key = str(map_row.iloc[0]).strip()
        fill_vals  = list(map_row.iloc[1:].values)

        # 针对 sample_key 生成 mask
        col0 = df[first_col].astype(str)

        if sample_key.upper().startswith("DB"):
            mask = col0.str.upper().str.startswith("DB")
        elif sample_key.lower().startswith("test"):
            mask = col0.str.lower().str.startswith("test")
        elif sample_key.upper().startswith("STD"):
            mask = col0 == sample_key  # STD3 等精确匹配
        elif loc_display and sample_key.strip() == loc_display:
            mask = (col0 == loc_display)
        elif sample_key.strip() == "*":
            # 兜底规则：留到最后填尚未填充的行
            mask = df.iloc[:, 1].isna() if df.shape[1] > 1 else pd.Series([True] * len(df), index=df.index)
        else:
            # 临床样本（一般不在模板中定义具体 key），跳过；让 '*' 兜底去管
            continue

        idxs = df.index[mask].tolist()
        if not idxs:
            continue

        # 处理每个目标列
        for col, val in zip(headers[1:], fill_vals):
            sval = str(val).strip()

            # '*'：整列镜像第一列
            if sval == "*":
                mirror_cols.add(col)
                continue

            # 定位孔行：只替第一个命中的行
            if loc_display and sample_key.strip() == loc_display:
                if not locator_info:
                    # 没有定位孔信息 → 保留原样
                    continue
                # 第一个
                first_idx = None
                for i in idxs:
                    if not locator_first_used:
                        first_idx = i
                        locator_first_used = True
                        break
                # 其他同名行删除
                for j in idxs:
                    if j != first_idx:
                        rows_to_drop.append(j)
                if first_idx is None:
                    continue
                # 写显示名 + 坐标（仅当占位符时）
                if col == first_col:
                    df.at[first_idx, col] = loc_display
                if col_wellnum and col == col_wellnum:
                    df.at[first_idx, col_wellnum] = locator_info.get("well_num")
                if col_wellpos and col == col_wellpos:
                    df.at[first_idx, col_wellpos] = locator_info.get("well_pos")
                continue

            # 孔号/孔位占位符：根据“样本名 → 条码 → 孔位”求值  f"{injection_plate}:{well_no}"
            if sval in ("{{Well_Number}}", "{{Well_Position}}"):
                def _resolve(sample_name_value: str):
                    name = str(sample_name_value).strip()

                    # 1) DB*: 固定 A1/1（与 Tecan 现有规则一致）
                    if name.upper().startswith("DB"):
                        return 1 if sval == "{{Well_Number}}" else "A1"
                    
                    # 2) 定位孔：name 等于 X{plate} 时，直接用 locator_info
                    if loc_display and locator_info and name == loc_display:
                        well_pos = locator_info.get("well_pos")
                        well_num = locator_info.get("well_num")
                        if instrument_name == "Thermo" or instrument_name == "Agilent":
                            if sval == "{{Well_Number}}":
                                return f"{injection_plate}:{well_num}" if injection_plate else well_num
                            else:
                                if project_name == "PCA":
                                    return f"{injection_plate}:{well_pos}" if injection_plate else well_pos
                                else:
                                    return f"{injection_plate}-{well_pos}" if injection_plate else well_pos
                        else:
                            if sval == "{{Well_Number}}":
                                return well_num
                            else:
                                return well_pos

                    # 3) QC/STD：Name→Barcode（队列），条码→(well_pos, well_num)
                    if name in name_to_barcodes and name_to_barcodes[name]:
                        barcode = name_to_barcodes[name].popleft()
                        wells = barcode_to_well.get(barcode)
                        if wells:
                            well_pos, well_num = wells.popleft()   # ← 关键：消费本条码的下一个孔位
                            if instrument_name == "Thermo" or instrument_name == "Agilent":
                                if sval == "{{Well_Number}}":
                                    return f"{injection_plate}:{well_num}" if injection_plate else well_num
                                else:
                                    if project_name == "PCA":
                                        return f"{injection_plate}:{well_pos}" if injection_plate else well_pos
                                    else:
                                        return f"{injection_plate}-{well_pos}" if injection_plate else well_pos
                            else:
                                if sval == "{{Well_Number}}":
                                    return well_num
                                else:
                                    return well_pos
                        return None

                    # 3) 临床：第一列值即条码
                    wells = barcode_to_well.get(name)
                    
                    if wells:
                        well_pos, well_num = wells.popleft()       # ← 关键：消费一次
                        if instrument_name == "Thermo" or instrument_name == "Agilent":
                            if sval == "{{Well_Number}}":
                                return f"{injection_plate}:{well_num}" if injection_plate else well_num
                            else:
                                if project_name == "PCA":
                                    return f"{injection_plate}:{well_pos}" if injection_plate else well_pos
                                else:
                                    return f"{injection_plate}-{well_pos}" if injection_plate else well_pos
                        else:
                            if sval == "{{Well_Number}}":
                                return well_num
                            else:
                                return well_pos

                df.loc[mask, col] = df.loc[mask, first_col].apply(_resolve)
            else:
                # 其他列：写常量
                df.loc[mask, col] = val

    # 统一执行“镜像列”
    for col in mirror_cols:
        df[col] = df[first_col]

    # 删除多余的定位孔行
    if rows_to_drop:
        df.drop(rows_to_drop, inplace=True)
        df.reset_index(drop=True, inplace=True)

    if set_name and ("SetName" in df.columns):
        df["SetName"] = set_name

    if output_file and ("OutputFile" in df.columns):
        df["OutputFile"] = output_file
    
    if 'VialPos' in df.columns:
        df = format_vialpos_column(df, "VialPos")
    ic(df)

    return df


# 把“纵向填充”的线性序列 → 96 孔坐标（A1,B1,...H1,A2,...）
_PLATE_ROWS = list("ABCDEFGH")  # 8
_PLATE_COLS = list(range(1, 13))  # 1..12

def _linear_fill_vertical_from_A1(n: int) -> list[tuple[str, int]]:
    """
    纵向：A1->B1->...->H1->A2->...->H2->... 生成前 n 个坐标
    """
    coords = []
    col_idx = 0
    row_idx = 0
    for _ in range(n):
        coords.append((_PLATE_ROWS[row_idx], _PLATE_COLS[col_idx]))
        row_idx += 1
        if row_idx >= len(_PLATE_ROWS):
            row_idx = 0
            col_idx += 1
            if col_idx >= len(_PLATE_COLS):
                break
    return coords


# 根据 CSV（processed 文件）+ station 映射，生成“临床样品”落位
def _build_clinical_cells_from_csv(csv_abs_path: str, start_offset: int, station_map: dict[str, str]) -> list[tuple[str,int,str,str]]:
    """
    返回 [(row_letter, col_num, sample_name, barcode), ...]  ← ★ 修改返回类型
    - area 映射：20->(4,5), 21->(6,7), 22->(7,8), 23->(9,10), 24->(11,12)
    - 列整体偏移：每个列号 += (start_offset-1)
    - pos 1..8 左列，9..16 右列；行号 6..13 （A..H）
    """
    base_area_map = {"20": (3,4), "21": (5,6), "22": (7,8), "23": (9,10), "24": (11,12)}

    rows = []
    with open(csv_abs_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = [ln.strip() for ln in f.read().splitlines() if ln.strip() != ""]
    if len(lines) <= 1:
        return rows
    for ln in lines[1:]:
        parts = [p.strip() for p in ln.split(";")]
        if not parts:
            continue
        area = parts[0] if len(parts) >= 1 else ""
        pos  = int(re.search(r"(\d+)", parts[2]).group(1)) if len(parts) >= 3 and re.search(r"(\d+)", parts[2]) else None
        srctube = parts[-1] if len(parts) >= 1 else ""  # ← 原始条码（如 01000795927-01）
        if not area or pos is None: 
            continue
        if "$" in srctube:  # 含 $ 忽略
            continue
        if area not in base_area_map:
            continue
        col_pair = base_area_map[area]
        left_col, right_col = col_pair[0] + (start_offset-1), col_pair[1] + (start_offset-1)
        use_col = left_col if pos <= 8 else right_col
        row_letter = _PLATE_ROWS[(pos-1) % 8]  # 1->A, 8->H, 9->A...
        
        # 通过条码查找实验号
        sample_name = station_map.get(srctube, "")

        # ★ 修改：未匹配时设置 "No match"（与 NIMBUS 对齐）
        if not sample_name:
            sample_name = "No match"  # ← 改为 "No match"
        
        # ★ 修改：同时保存实验号和原始条码
        rows.append((row_letter, use_col, sample_name, srctube))  # ← 新增第4个元素：条码
    return rows



# 组装“工作清单数据结构”并渲染页面
def _render_tecan_process_result(
    request: HttpRequest, 
    today: str, 
    csv_abs_path: str, 
    project_id: str,
    station_save_summary: dict | None = None,
    ) -> HttpResponse:
    """
    生成页面所需的 96 孔板数据：
    - STD/QC：从 SamplingConfiguration 读取 curve_points / qc_groups / qc_levels
    - 临床样品：读取 csv + station_map，按 R 逻辑定位
    """
    # 1) 读取项目参数（曲线点、QC 组/层）
    # POST 优先；无则回退 session；再无则用传入的函数参数/空串
    project_id  = (request.POST.get("project_id")
                    or project_id
                    or request.session.get("tecan_project_id")
                    or "")

    project_name = (request.POST.get("project_name")
                    or request.session.get("tecan_project_name")
                    or "")

    instrument_num = (request.POST.get("instrument_num")
                    or request.session.get("tecan_instrument_num")
                    or "")

    systerm_num  = (request.POST.get("systerm_num")
                    or request.session.get("tecan_systerm_num")
                    or "")

    injection_plate = ((request.POST.get("injection_plate") or "").strip()
                    or (request.session.get("tecan_injection_plate") or "").strip()
                    or "")
    today_str  = timezone.localtime().strftime("%Y%m%d")
    year       = today_str[:4]
    yearmonth  = today_str[:6]

    # ✅ 新增：确保模板能拿到 station_save_summary
    station_save_summary = station_save_summary

    # 获取后台设置的项目参数，如果没设置报错并提示
    try:
        config = SamplingConfiguration.objects.get(project_name=project_name,default_upload_instrument=instrument_num,systerm_num=systerm_num)
    except SamplingConfiguration.DoesNotExist:
        # 返回友好的错误提示页面
        return render(request, "dashboard/error.html", {
            "message": "未配置项目参数，请前往后台参数配置中完善该项目设置后重试。"
        })

    # 进样体积（非必须设置项）
    try:
        injection_cfg = InjectionVolumeConfiguration.objects.get(project_name=project_name,instrument_num=instrument_num,systerm_num=systerm_num)
        injection_vol  = injection_cfg.injection_volume
    except InjectionVolumeConfiguration.DoesNotExist:
        injection_vol  = ""

    curve_points = qc_groups = qc_levels = 0
    curve_points = config.curve_points
    qc_groups = config.qc_groups
    qc_levels = config.qc_levels
    test_count = config.test_count
    project_name_full   = config.project_name_full

    # 读取上机列表模板
    mapping_file_path = config.mapping_file.path

    # 读取“项目配置”与“上机映射模板”sheet
    df_mapping_wc  = pd.read_excel(mapping_file_path, sheet_name="工作清单")
    df_worklistmap = pd.read_excel(mapping_file_path, sheet_name="上机列表")

    # 解析后台设置的上机模板（txt/csv）→ DataFrame（只需列名 / txt_headers）,获取表头
    try:
        instrument_config = InstrumentConfiguration.objects.get(instrument_num=instrument_num,systerm_num=systerm_num)
    except InstrumentConfiguration.DoesNotExist:
        # 返回友好的错误提示页面
        return render(request, "dashboard/error.html", {
            "message": "未配置仪器厂家参数，请前往后台参数配置中完善设置后重试。"
        })

    instrument_name = instrument_config.instrument_name
    if not instrument_config.upload_file:
        return HttpResponse("未设置该上机仪器对应的上机模板,请设置后再试", status=404)

    # 2) 解析文件名得到 plate_number/offset
    file_basename = os.path.basename(csv_abs_path)
    plate_number, start_offset = _parse_plate_meta_by_filename(file_basename)

    set_name    = f"{instrument_num}_{systerm_num}_{project_name}_{today_str}_X{plate_number}_GZ"
    output_file = f"{year}\\{yearmonth}\\Data{set_name}"

    # 3) 构建 STD/QC 单元（仅实施列偏移，不做“让位”）
    # std_qc_items = _build_curve_and_qc_cells(curve_points, qc_groups, qc_levels, file_basename)

    # 从『工作清单』表取 STD 和QC 名称池 —— 
    std_names_pool = df_mapping_wc.loc[
        df_mapping_wc["Code"].astype(str).str.startswith("STD", na=False), "Name"
    ].astype(str).tolist()

    qc_names_pool = df_mapping_wc.loc[
        df_mapping_wc["Code"].astype(str).str.startswith("QC", na=False), "Name"
    ].astype(str).tolist()

    # —— 传给新版 _build_curve_and_qc_cells —— 
    std_qc_items = _build_curve_and_qc_cells(
        curve_points=curve_points,
        std_names_pool=std_names_pool,
        qc_groups=qc_groups,
        qc_levels=qc_levels,
        qc_names_pool=qc_names_pool
    )

    ic(std_qc_items)

    # 3.1 先按 A1→H1→A2… 的竖向填充得到基础坐标
    std_qc_coords = _linear_fill_vertical_from_A1(len(std_qc_items))

    # 3.2 仅进行横向偏移：所有 STD/QC 列号 += (start_offset - 1)
    off = max(1, start_offset) - 1
    std_qc_coords = [(r, c + off) for (r, c) in std_qc_coords if 1 <= c + off <= 12]

    # 3.3 生成 cell 字典（不做任何纵向位移）
    std_qc_cells = [
        {"row": r, "col": c, "text": it["label"], "kind": it["kind"]}
        for it, (r, c) in zip(std_qc_items, std_qc_coords)
    ]

    ic(std_qc_cells)

    # 4) 临床样品
    project_dir = request.session.get("tecan_project_dir") or _safe_dirname(
        request.POST.get("project_name", "") or request.POST.get("project_id", "")
    )

    station_map = request.session.get("tecan_station_map")
    clinical_cells = []
    try:
        clinical_cells = _build_clinical_cells_from_csv(csv_abs_path, start_offset, station_map)
        # ★ 修改：解包4个元素（row, col, sample_name, barcode）
        clinical_cells = [
            {"row": r, "col": c, "text": s or "", "barcode": bc or ""} 
            for (r, c, s, bc) in clinical_cells
        ]
    except Exception:
        clinical_cells = []

    # === 新增：按板号规则对“临床样本”位移（A 行左移 + 首个挪到前一孔）===
    try:
        # ★ 正确做法：直接传入4元组，位移函数内部同时处理条码
        raw_list = [(d["row"], d["col"], d["text"], d.get("barcode", "")) for d in clinical_cells]
        shifted = _apply_locator_shift_for_clinicals(raw_list, plate_number, start_offset)
        
        # ★ shifted 已经是4元组：(row, col, sample_name, barcode)
        clinical_cells = [
            {"row": r, "col": c, "text": s, "barcode": bc} 
            for (r, c, s, bc) in shifted
        ]
    except Exception as e:
        # 调试用：打印异常信息
        print(f"位移失败: {e}")
        pass


    # 5) 组装 96 孔板矩阵 -> 二级字典 grid[row][col]
    grid: dict[str, dict[int, dict]] = {r: {c: {"std_qc": None, "sample": None} for c in _PLATE_COLS} for r in _PLATE_ROWS}

    for cell in std_qc_cells:
        grid[cell["row"]][cell["col"]]["std_qc"] = cell
    for cell in clinical_cells:
        grid[cell["row"]][cell["col"]]["sample"] = cell

    # === 新增：把“定位孔”写入 grid（用于页面与上机列表替换）===
    loc_row, loc_col = _locator_coord_for_plate(plate_number, start_offset)
    if loc_col >= 1:  # 简单边界保护
        grid.setdefault(loc_row, {}).setdefault(loc_col, {})
        grid[loc_row][loc_col]["is_locator"] = True
        grid[loc_row][loc_col]["locator_warm"] = f"X{plate_number}" 
    

    # 6) 渲染
    # —— 新增：TECAN 适配版 builder，与 views.ProcessResult 的字段命名/语义对齐 ——
    def build_well_dict_tecan(row_letter, col_num, row_idx, col_idx, well_index, cell):
        cell = cell or {}
        std_qc = (cell or {}).get("std_qc") or {}
        sample = (cell or {}).get("sample") or {}

        # 统一字段（与通用版模板/导出对齐）
        d = {
            "letter": row_letter,       # A..H
            "num": col_num,         # 1..12
            "well_str": f"{row_letter}{col_num}",
            "index":  well_index,              # 1..96（固定计算）

            # 兼容通用版的常用键（先留空/占位，后续若接入可补齐）
            "origin_barcode": "",
            "barcode": "",
            "status": "",
            "locator": bool((cell or {}).get("is_locator")),     # ← 从 cell 读取
            "flags": [],                    # 预留：重复/一对多等
            "meta": {},

            # 你可能在通用版用到的若干扩展键，先给默认值，避免模板/导出报错
            "MatchResult": "",
            "match_sample": sample.get("text") or "",
            "DupBarcode": "",
            "DupBarcodeSampleName": "",
            "Warm": "",
        }

        # 兼容“孔内多行”展示
        d["std_qc_text"] = std_qc.get("text") or ""
        d["sample_text"] = sample.get("text") or ""

        # ======== 新增：分割条码为主条码和子条码（与 NIMBUS/Starlet 对齐） ========
        # 若是定位孔，带上显示名（默认 "X{plate}" 已在 grid 写入）
        if d["locator"]:
            d["locator_warm"] = (cell or {}).get("locator_warm") or "Locator"

        else:
            # ★ 修改：优先使用 sample 中的 barcode 字段
            if d["std_qc_text"]:
                d["barcode"] = d["std_qc_text"]  # STD/QC 用名称作为条码
            elif sample.get("barcode"):
                d["barcode"] = sample.get("barcode")  # ← ★ 临床样本用真实条码
            elif d["sample_text"]:
                d["barcode"] = d["sample_text"]  # 兜底：无条码时用实验号

        # ★ 新增：分割 barcode 为 cut_barcode（主码）和 sub_barcode（子码）
        full_barcode = d.get("barcode", "")
        if full_barcode and "-" in str(full_barcode):
            # 格式：01000795927-01 → 主码:01000795927, 子码:01
            parts = str(full_barcode).split("-", 1)
            d["cut_barcode"] = parts[0]  # 主条码
            d["sub_barcode"] = "-" + parts[1] if len(parts) > 1 and parts[1] else ""  # ★ 子条码前加 '-'
        else:
            # 无子码的情况（如 STD/QC 名称）
            d["cut_barcode"] = full_barcode
            d["sub_barcode"] = ""
        # ======== 分割完成 ========

        # match_sample 保持原逻辑
        if d.get("match_sample", "") == "" and d["std_qc_text"]:
            d["match_sample"] = d["std_qc_text"]
            d["flags"].append("std_qc")

        return d

    letters = ["A","B","C","D","E","F","G","H"]
    nums = list(range(1, 13))

    rows_letters = letters    # ['A','B',...,'H']
    cols_numbers = nums    # [1,2,...,12]

    worksheet_grid = [[None for _ in cols_numbers] for __ in rows_letters]

    for col_idx, col_num in enumerate(cols_numbers):
        for row_idx, row_letter in enumerate(rows_letters):
            # 通用版固定 well_index（与布局无关）
            well_index = row_idx * len(cols_numbers) + col_idx + 1

            # 从 TECAN 的 grid[r][c] 取单元
            cell = (grid.get(row_letter, {}) or {}).get(col_num, {}) or {}

            # 用 TECAN 适配 builder 产出与通用版一致的单元字典
            well = build_well_dict_tecan(
                row_letter=row_letter,
                col_num=col_num,
                row_idx=row_idx,
                col_idx=col_idx,
                well_index=well_index,
                cell=cell
            )

            # 关键：按“行(row_idx)、列(col_idx)”写入
            worksheet_grid[row_idx][col_idx] = well

    # 8×12 二维表
    worksheet_table = worksheet_grid

    def _name_to_barcodes_from_grid(grid):
        """
        用 grid/std_qc 解析出来的清单，构建 name->barcodes（队列，保持顺序）
        TECAN：名称即条码
        """
        buckets = defaultdict(list)
        for row_letter, cols in (grid or {}).items():
            for col_num, cell in (cols or {}).items():
                std_qc = (cell or {}).get("std_qc") or {}
                name = (std_qc.get("text") or "").strip()
                if name:
                    buckets[name].append(name)  # 名称即条码
                # # 临床样本
                # sample = (cell or {}).get("sample") or {}
                # sname = (sample.get("text") or "").strip()
                # if sname:
                #     # 临床样本条码也要记录，便于 barcode->well 反查
                #     buckets.setdefault(sname, [])
        return {k: deque(v) for k, v in buckets.items()}

    def _barcode_to_well_from_table(worksheet_table):
        """
        返回： dict[str, deque[(well_pos, well_num)]]
        ★ 修复：临床样本同时用实验号和条码作为 key
        """
        m = defaultdict(deque)
        for row in (worksheet_table or []):
            for cell in (row or []):
                if not cell:
                    continue
                
                # 原逻辑：用完整条码作为 key（用于96孔板显示）
                barcode = str(cell.get("barcode") or "").strip()
                if barcode:
                    m[barcode].append((cell.get("well_str"), cell.get("index")))
                
                # ★ 新增：同时用实验号作为 key（用于上机列表孔位匹配）
                sample_text = str(cell.get("sample_text") or "").strip()
                if sample_text and sample_text != barcode:  # 避免重复注册
                    m[sample_text].append((cell.get("well_str"), cell.get("index")))
        
        return m


    # 1) 基于 grid 的 std_qc，构建 name->barcodes（TECAN：名称即条码）
    name_to_barcodes = _name_to_barcodes_from_grid(grid)

    # 2) 基于 96 孔表（worksheet_table），构建 barcode -> (Well_Position, Well_Number)
    barcode_to_well = _barcode_to_well_from_table(worksheet_table)

    # 3) 准备“定位孔”信息（如果你有定位孔；没有就传 None）
    locator_info = None

    # 比如你在 worksheet_table 某格有 locator=True，可搜出来：
    for row in worksheet_table:
        for cell in row:
            if cell and cell.get("locator"):
                locator_info = {
                    "well_pos": cell.get("well_str"),
                    "well_num": cell.get("index"),
                    "display_name": cell.get("locator_warm") or "Locator",
                }
                break
        if locator_info:
            break

    # 用于构建曲线/QC/Test/DB 序列
    df_std = df_mapping_wc.copy() 
    df_std["Code"] = df_std["Code"].astype(str)
    df_std = df_std[df_std["Code"].str.match(r"^STD\d+$", na=False)].copy()
    df_std["__std_idx"] = df_std["Code"].str.replace("STD", "", regex=False).astype(int)
    df_std = df_std.sort_values("__std_idx")
    std_names = df_std["Name"].head(curve_points + 1).tolist()
    if not std_names or len(std_names) < (curve_points + 1):
        std_names = [f"STD{i}" for i in range(curve_points + 1)]
    
    qc_names = df_mapping_wc.loc[df_mapping_wc["Code"].astype(str).str.startswith("QC"), "Name"].unique().tolist()

    # 临床样本
    # === 用“列优先 A1→H1→A2…”提取临床样本顺序（跳过定位孔）+ 插入定位孔显示名 === 
    def _clinical_barcodes_in_plate_order(worksheet_table):
        out = []
        rows = list("ABCDEFGH")
        for col in range(1, 13):              # 列优先
            for r_idx, row in enumerate(rows):
                cell = worksheet_table[r_idx][col-1]
                if not cell:
                    continue
                if cell.get("locator"):
                    continue
                s = (cell.get("sample_text") or "").strip()
                if s:
                    out.append(s)   
        return out

    ClinicalSample = _clinical_barcodes_in_plate_order(worksheet_table)

    # 定位孔显示名
    locator_display = f"X{plate_number}"
    # N = (plate-1)%8 + 1：位于第 N 个临床样之后
    nth = ((plate_number - 1) % 8) + 1
    insert_idx = min(nth, len(ClinicalSample))
    ClinicalSample_with_locator = ClinicalSample[:insert_idx] + [locator_display] + ClinicalSample[insert_idx:]

    test_list   = ["DB1"] + [f"Test{i}" for i in range(test_count)]
    curve_list  = ["DB2"] + std_names
    qc_list1    = ["DB3"] + qc_names + ["DB4"]
    qc_list2    = qc_names + ["DB5"]

    # ③ 拼出 SampleName_list（与 NIMBUS 同构：DB + STD/QC + 临床 [+ QC 结尾…]）
    SampleName_list = test_list + curve_list + qc_list1 + ClinicalSample_with_locator + qc_list2

    # ④ 以模板列头构造空表，第一列写入 SampleName_list  _write_processed_copy_from_original
    txt_headers = list(df_worklistmap.columns)
    worklist_table = pd.DataFrame(columns=txt_headers)
    worklist_table[txt_headers[0]] = SampleName_list


    # 5) 按 NIMBUS 的四类规则批量替换
    # ⑤ name→barcode 队列、barcode→well 映射、定位孔信息（直接用你现有构造方式）
    # name_to_barcodes: 由 grid 统计 QC/STD 名称映射到条码队列（与原先一致）
    # barcode_to_well : 由可视网格/worksheet_table 反向索引得到（与原先一致）
    # locator_info    : {'well_pos': 'A3', 'well_num': 3, 'display_name': 'Xn'}（与原先一致）

    # ⑥ 应用“上机映射模板”把占位符填入表
    worklist_table = _apply_mapping_to_table(
        df_worklistmap,
        worklist_table,
        name_to_barcodes=name_to_barcodes,
        barcode_to_well=barcode_to_well,
        locator_info=locator_info,
        project_name=project_name,
        injection_plate=injection_plate,
        instrument_name=instrument_name,
        set_name=set_name,                     
        output_file=output_file                  
    )

    # Thermo 专用：若第一列存在完全相同的多行值，则改成 原值-1、原值-2、…（仅对重复值生效）——
    # 判断当前仪器是否 Thermo（InstrumentConfiguration.instrument_name 中包含 "Thermo"）
    vendor_name = str(getattr(instrument_config, "instrument_name", "")).lower()
    if "thermo" in vendor_name:
        # 第一列的列名
        first_col_name = worklist_table.columns[0]
        # 统一按字符串处理
        s = worklist_table[first_col_name].astype(str)

        # 统计每个值出现次数，用于只对“重复值”做处理
        vc = s.map(s.value_counts())
        # 对同值分组做累加计数：0,1,2,...  -> 我们需要 1,2,3,...
        order = s.groupby(s).cumcount() + 1

        # 只改“出现次数 > 1”的那些值；出现 1 次的保持不变
        mask = vc > 1
        worklist_table.loc[mask, first_col_name] = s[mask] + "-" + order[mask].astype(str)

    # 关联后台参数设置中设置的进样体积
    for col in ["SmplInjVol", "Injection volume"]:
        if col in worklist_table.columns and injection_vol != "":
            worklist_table[col] = injection_vol
    
    # 进样盘
    if "PlatePos" in worklist_table.columns:
        worklist_table["PlatePos"] = injection_plate

    # 将第一列内容中含有‘X’关键词的行（即定位孔），整行移到第一列内容为‘DB4’的这一行后
    # X 行
    col = worklist_table.columns[0]
    x_rows = worklist_table[worklist_table[col].astype(str).str.contains('X', na=False)]

    # DB4 行索引
    db4_idx = worklist_table.index[worklist_table[col] == 'DB4'][0]

    # 原表删除 X 行
    df = worklist_table.drop(x_rows.index)

    # 重新拼接（看起来像“原地替换”）
    worklist_table = pd.concat(
        [df.loc[:db4_idx], x_rows, df.loc[db4_idx + 1:]],
        ignore_index=True
    )

    # 6) 导出给模板（动态表头/行）
    txt_headers = list(worklist_table.columns)
    worklist_records = worklist_table.to_dict("records")

    # 7) 写进 session 的 export_payload（便于“预览/导出”链路直接复用）
    error_rows= []

    header_meta = {
        "test_date": timezone.localtime().strftime("%Y-%m-%d"),
        "plate_no": f"X{plate_number}",
        "instrument_num": instrument_num,
        "systerm_num": systerm_num,
        "injection_plate": injection_plate,
        "today_str": today_str,
    }

    # ======== 新增：写入 SampleRecord 历史记录（供每日标本统计功能使用） ========
    # 确定记录日期（当天）
    record_date = date.today()
    
    # 清理同日同项目同板号的旧记录（避免重复提交时累积）
    plate_no_str = f"X{plate_number}"
    SampleRecord.objects.filter(
        project_name=project_name,
        record_date=record_date,
        plate_no=plate_no_str
    ).delete()

    # 遍历 worksheet_table（96孔板数据），逐孔写入
    for row in worksheet_table:
        for cell in row:
            if not cell:
                continue
            
            well_str = cell.get("well_str", "")  # 如 "A1", "H12"
            if not well_str:
                continue
            
            # 提取样本名（优先 match_sample，其次 std_qc_text，再次 sample_text）
            sample_name = (
                cell.get("match_sample") or 
                cell.get("std_qc_text") or 
                cell.get("sample_text") or 
                ""
            ).strip()
            
            # 提取条码（临床样本的条码即为样本名；STD/QC的条码也是其名称）
            barcode = (cell.get("barcode") or "").strip()
            
            # 跳过空孔（既无样本名也无条码）
            if not sample_name and not barcode:
                continue
            
            # 定位孔标记（可选：如果希望在统计中过滤定位孔，可记录特殊标志）
            error_info = ""
            if cell.get("locator"):
                error_info = "locator"  # 标记为定位孔
            
            # 写入数据库
            SampleRecord.objects.update_or_create(
                project_name=project_name,
                record_date=record_date,
                plate_no=plate_no_str,
                well_str=well_str,
                defaults={
                    "sample_name": sample_name,
                    "barcode": barcode,
                    "error_info": error_info,
                }
            )

    # 兼容 NIMBUS 旧逻辑（有些视图会兜底取 export_payload）
    request.session["export_payload"] = {
        "project_name": project_name,
        "project_name_full": project_name_full,
        "instrument_num": instrument_num,
        "systerm_num": systerm_num,
        "platform": 'Tecan',
        "txt_headers": txt_headers,
        "worklist_records": worklist_records,
        "worksheet_table": worksheet_table,
        "error_rows": error_rows,
        "header": header_meta,
    }

    return render(request, "dashboard/ProcessResult_TECAN.html",locals())






