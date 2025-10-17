from __future__ import annotations

import os, re, uuid, io
from datetime import datetime
from typing import Dict, List, Tuple, Set

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse, HttpResponseBadRequest
from django.views.decorators.csrf import csrf_protect
from django.utils.html import escape

import os
import pandas as pd
import re
from icecream import ic

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

    # 本次内重复
    dup_mask = df["MainBarcode"].duplicated(keep="first")  # 只标后续
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
    base_dir     = os.path.join(settings.MEDIA_ROOT, "tecan", today)
    original_dir = os.path.join(base_dir, "original")
    processed_dir= os.path.join(base_dir, "processed")
    station_dir  = os.path.join(base_dir, "station")
    for d in (original_dir, processed_dir, station_dir):
        _ensure_dir(d)

    # 前端保持“基本不变”，这里只取我们需要的字段/文件
    project_id   = request.POST.get("project_id", "").strip()
    station_file = request.FILES.get("station_list")  # 这一步不使用，但可保存做记录
    scan_file    = request.FILES.get("scan_result")

    if not scan_file:
        return HttpResponseBadRequest("请上传‘扫码结果表’(CSV)")

    # === 修改：保存到“当天/original|station” ===
    saved_scan_abs = _save_upload_to_media(f"tecan/{today}/original", scan_file)
    if station_file:
        _save_upload_to_media(f"tecan/{today}/station", station_file)

    # 1) 解析 CSV
    try:
        df = _parse_tecan_csv_abs(saved_scan_abs)
    except Exception as e:
        return HttpResponseBadRequest(f"CSV解析失败：{e}")

    # 冲突检测 + 渲染 
    history = _collect_history_mainbarcodes(processed_dir)

    # === 与 R 一致的重复选择逻辑 ===
    # 文件内重复：只把“后续出现”的重复标出来（首个出现的不算）
    mask_infile = df["MainBarcode"].duplicated(keep="first")

    # 跨文件重复：本次文件任何出现过的 MainBarcode 只要在当天 processed 中出现过就算重复
    mask_cross = df["MainBarcode"].isin(history)

    # 并集 + 对 SRCTubeID 去重 + 按位置信息排序（与 R 的 order(GridPos, TipNumber) 一致）
    need_fix_df = (
        df[mask_infile | mask_cross]
        .drop_duplicates(subset=["SRCTubeID"])
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
        cross_mains = sorted(set(df.loc[mask_cross, "MainBarcode"].astype(str)))

        # 把“原始文件相对路径/当天日期”等存 Session（你原来的做法保持不变）
        rel_path = os.path.relpath(saved_scan_abs, settings.MEDIA_ROOT).replace(os.sep, "/")
        request.session["tecan_pending_file"] = rel_path
        request.session["tecan_pending_date"] = today
        request.session.modified = True

        from django.shortcuts import render
        return render(request, "dashboard/sampling/tecan_duplicates.html", {
            "dups": dups,                # 需要人工填写新 SRCTubeID 的那批行（= R 的 ordered_duplicates）
            "cross_mains": cross_mains,  # 跨文件重复主码，仅提示
            "today": today,
            "original_filename": os.path.basename(saved_scan_abs),
        })

    # === 修改：无冲突 → 原样复制到“当天/processed/”，保持原 CSV 格式 ===
    out_path = _write_processed_copy_from_original(saved_scan_abs, processed_dir)

    rel = os.path.relpath(out_path, settings.MEDIA_ROOT).replace(os.sep, "/")
    url = f"{getattr(settings, 'MEDIA_URL', '/media/')}{rel}"

    return HttpResponse(
        f"<div class='card'><div class='card-body'>"
        f"<h5>Tecan Step1</h5><div class='alert alert-success'>未检测到重复条码，已写入 processed。</div>"
        f"<p>文件：<a href='{url}' target='_blank'>{escape(os.path.basename(out_path))}</a></p>"
        f"</div></div>"
    )


@csrf_protect
def tecan_resolve_duplicates(request: HttpRequest) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseBadRequest("仅支持POST")

    rel_path = request.session.get("tecan_pending_file")
    today    = request.session.get("tecan_pending_date")
    if not rel_path or not today:
        return HttpResponseBadRequest("未找到待处理文件，请返回重新上传。")

    saved_scan_abs = os.path.join(settings.MEDIA_ROOT, rel_path)
    base_dir     = os.path.join(settings.MEDIA_ROOT, "tecan", today)
    processed_dir= os.path.join(base_dir, "processed")
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
    fix_map_by_rowid: dict[int, tuple[str, str]] = {}
    i = 0
    while True:
        rk, ok, nk = f"rowid_{i}", f"old_{i}", f"new_{i}"
        if rk not in request.POST and ok not in request.POST and nk not in request.POST:
            break
        rowid = request.POST.get(rk, "").strip()
        old_v = request.POST.get(ok, "").strip()
        new_v = request.POST.get(nk, "").strip()
        if rowid:
            fix_map_by_rowid[int(rowid)] = (old_v, new_v)
        i += 1
    if not fix_map_by_rowid:
        return HttpResponseBadRequest("没有收到任何修正项")

    # === B) 校验（按行号，和 R 一致） ===
    rows_for_fix = df[df["RowID"].isin(fix_map_by_rowid.keys())].copy()
    if rows_for_fix.empty:
        return HttpResponseBadRequest("修正的 RowID 不存在于当前文件")

    history = _collect_history_mainbarcodes(processed_dir)
    all_main = set(df["MainBarcode"].astype(str)) | set(history)

    seen_new_main: set[str] = set()
    for rowid, (old_v, new_v) in fix_map_by_rowid.items():
        r = rows_for_fix.loc[rows_for_fix["RowID"] == rowid].iloc[0]
        if old_v != str(r["SRCTubeID"]):
            return HttpResponseBadRequest(f"Row {rowid} 原 SRCTubeID 不匹配")
        if "-" not in new_v:
            return HttpResponseBadRequest(f"Row {rowid} 新 SRCTubeID 必须包含 '-'")
        new_main = new_v.split("-", 1)[0].strip()
        if not new_main:
            return HttpResponseBadRequest(f"Row {rowid} 新主码为空")
        if new_main in all_main:
            return HttpResponseBadRequest(f"Row {rowid} 新主码 {new_main} 已存在（当日历史或本次文件）")
        if new_main in seen_new_main:
            return HttpResponseBadRequest(f"多个输入的新主码重复：{new_main}")
        seen_new_main.add(new_main)

    # === C) 写 processed：保留首行，仅改这些 RowID 的“最后一列 SRCTubeID” ===
    header_line, data_lines, parsed_rows = _read_raw_csv_lines(saved_scan_abs)
    out_path = _write_processed_with_row_replacements(
        header_line=header_line,
        parsed_rows=parsed_rows,
        processed_dir=processed_dir,
        original_filename=os.path.basename(saved_scan_abs),
        fix_map_by_rowid=fix_map_by_rowid,
    )

    # 6) 返回成功页
    rel = os.path.relpath(out_path, settings.MEDIA_ROOT).replace(os.sep, "/")
    url = f"{getattr(settings, 'MEDIA_URL', '/media/')}{rel}"
    # 清理 session（可选）
    for k in ("tecan_pending_file", "tecan_pending_date"):
        if k in request.session: del request.session[k]
    request.session.modified = True

    return HttpResponse(
        f"<div class='card'><div class='card-body'>"
        f"<h5>Tecan Step1 - 冲突修正完成</h5>"
        f"<div class='alert alert-success'>已生成处理后的文件（只改最后一列 SRCTubeID）：</div>"
        f"<p>文件：<a href='{url}' target='_blank'>{escape(os.path.basename(out_path))}</a></p>"
        f"</div></div>"
    )
