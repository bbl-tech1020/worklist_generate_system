from __future__ import annotations

import os, re, uuid, io
from datetime import datetime
from typing import Dict, List, Tuple, Set

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse, HttpResponseBadRequest
from django.views.decorators.csrf import csrf_protect
from django.utils.html import escape
from django.shortcuts import render

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

def _save_station_first_only(today: str, f) -> str:
    """
    在 media/tecan/{today}/station 目录下：只保留当天“第一份”岗位清单。
    - 若目录已有任意文件，则直接返回该文件路径，不再保存新的；
    - 若目录为空，保存当前上传的文件；
    - 文件名固定：station_{today}.<ext>（沿用上传扩展名，默认为 .xlsx）
    返回：最终保留的绝对路径
    """
    base_dir = os.path.join(settings.MEDIA_ROOT, "tecan", today, "station")
    _ensure_dir(base_dir)

    # 目录已有文件 -> 保留第一份，直接返回
    existing = [os.path.join(base_dir, name) for name in os.listdir(base_dir)
                if os.path.isfile(os.path.join(base_dir, name))]
    if existing:
        # 返回第一份（按文件名排序保证稳定）
        existing.sort()
        return existing[0]

    # 目录为空 -> 保存第一份
    _, ext = os.path.splitext(getattr(f, "name", "") or "")
    ext = ext if ext else ".xlsx"
    filename = f"station_{today}{ext}"
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
        _save_station_first_only(today, station_file)

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

    # 去除含有'$'的MainBarcode
    mask_dollar = df["MainBarcode"].astype(str).str.contains(r"\$")

    # 并集 + 对 SRCTubeID 去重 + 按位置信息排序（与 R 的 order(GridPos, TipNumber) 一致）
    need_fix_df = (
        df[(mask_infile | mask_cross) & (~mask_dollar)]              # ← 过滤掉含 $
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
        cross_mains = sorted(set(
            df.loc[mask_cross & (~mask_dollar), "MainBarcode"].astype(str)  # ← 提示也过滤
        ))

        # 把“原始文件相对路径/当天日期”等存 Session（你原来的做法保持不变）
        rel_path = os.path.relpath(saved_scan_abs, settings.MEDIA_ROOT).replace(os.sep, "/")
        request.session["tecan_pending_file"] = rel_path
        request.session["tecan_pending_date"] = today
        request.session.modified = True

        return render(request, "dashboard/sampling/tecan_duplicates.html", {
            "dups": dups,                # 需要人工填写新 SRCTubeID 的那批行（= R 的 ordered_duplicates）
            "cross_mains": cross_mains,  # 跨文件重复主码，仅提示
            "today": today,
            "original_filename": os.path.basename(saved_scan_abs),
        })

    # === 修改：无冲突 → 原样复制到“当天/processed/”，保持原 CSV 格式 ===
    out_path = _write_processed_copy_from_original(saved_scan_abs, processed_dir)

    return _render_tecan_process_result(request, today=today, csv_abs_path=out_path, project_id=project_id)
    
    # rel = os.path.relpath(out_path, settings.MEDIA_ROOT).replace(os.sep, "/")
    # url = f"{getattr(settings, 'MEDIA_URL', '/media/')}{rel}"

    # return HttpResponse(
    #     f"<div class='card'><div class='card-body'>"
    #     f"<h5>Tecan Step1</h5><div class='alert alert-success'>未检测到重复条码，已写入 processed。</div>"
    #     f"<p>文件：<a href='{url}' target='_blank'>{escape(os.path.basename(out_path))}</a></p>"
    #     f"</div></div>"
    # )


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

    return _render_tecan_process_result(request, today=today, csv_abs_path=out_path, project_id=project_id)

    # # 6) 返回成功页
    # rel = os.path.relpath(out_path, settings.MEDIA_ROOT).replace(os.sep, "/")
    # url = f"{getattr(settings, 'MEDIA_URL', '/media/')}{rel}"
    # # 清理 session（可选）
    # for k in ("tecan_pending_file", "tecan_pending_date"):
    #     if k in request.session: del request.session[k]
    # request.session.modified = True

    # return HttpResponse(
    #     f"<div class='card'><div class='card-body'>"
    #     f"<h5>Tecan Step1 - 冲突修正完成</h5>"
    #     f"<div class='alert alert-success'>已生成处理后的文件（只改最后一列 SRCTubeID）：</div>"
    #     f"<p>文件：<a href='{url}' target='_blank'>{escape(os.path.basename(out_path))}</a></p>"
    #     f"</div></div>"
    # )


# 读取当天 station（岗位清单）映射（子条码→实验号）
def _load_station_map_for_today(today: str) -> dict[str, str]:
    """
    读取 media/tecan/{today}/station 下“第一份”岗位清单，提取 子条码->实验号 映射。
    要求表头包含：'子条码', '实验号'
    """
    base_dir = os.path.join(settings.MEDIA_ROOT, "tecan", today, "station")
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


# 生成“STD/QC”列表（纵向从 A1 开始）
def _build_curve_and_qc_cells(curve_points: int, qc_groups: int, qc_levels: int, file_basename: str) -> list[dict]:
    """
    生成一个顺序列表，每个元素包含：
    { 'label': 显示文本, 'kind': 'STD'|'QC' }
    - STD: STD0 ~ STD{curve_points-1}
    - QC:  按 qc_groups * qc_levels 生成，名称来自 _get_qc_name_table
           示例：QC1_1 -> 'QC00001663'（显示“名称字段”，不是 code）
    """
    items = []
    # STD
    for i in range(curve_points):
        items.append({"label": f"STD{i}", "kind": "STD"})
    # QC
    qc_names_pool = _get_qc_name_table(file_basename)
    if not qc_names_pool:
        qc_names_pool = ["QC"] * max(1, qc_levels)
    for g in range(1, qc_groups + 1):
        for lv in range(1, qc_levels + 1):
            # 名称取“第 lv 个”QC 名
            name_idx = (lv - 1) % len(qc_names_pool)
            items.append({"label": qc_names_pool[name_idx], "kind": "QC", "group": g, "level": lv})
    return items


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
def _build_clinical_cells_from_csv(csv_abs_path: str, start_offset: int, station_map: dict[str, str]) -> list[tuple[str,int,str]]:
    """
    返回 [(row_letter, col_num, sample_name), ...]
    - area 映射：20->(4,5), 21->(6,7), 22->(8,9), 23->(10,11), 24->(12,13)
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
        srctube = parts[-1] if len(parts) >= 1 else ""
        if not area or pos is None: 
            continue
        if "$" in srctube:  # 含 $ 忽略
            continue
        if area not in base_area_map:
            continue
        col_pair = base_area_map[area]
        left_col, right_col = col_pair[0] + (start_offset-1), col_pair[1] + (start_offset-1)
        use_col = left_col if pos <= 8 else right_col
        # 行 A..H -> 6..13（Excel行号），但我们最后在网页渲染用 A..H + 1..12 逻辑，所以只要 A..H 座标
        row_letter = _PLATE_ROWS[(pos-1) % 8]  # 1->A, 8->H, 9->A...
        sample_name = station_map.get(srctube, "")
        if not sample_name:
            sample_name = ""  # 允许空（按 R）
        rows.append((row_letter, use_col, sample_name))
    return rows


# 组装“工作清单数据结构”并渲染页面
def _render_tecan_process_result(request: HttpRequest, today: str, csv_abs_path: str, project_id: str) -> HttpResponse:
    """
    生成页面所需的 96 孔板数据：
    - STD/QC：从 SamplingConfiguration 读取 curve_points / qc_groups / qc_levels
    - 临床样品：读取 csv + station_map，按 R 逻辑定位
    """
    # 1) 读项目参数（你项目里的 SamplingConfiguration 具体字段名如不同，请在此处改一行）
    curve_points = qc_groups = qc_levels = None
    try:
        # 示例：ProjectConfig.objects.get(pk=project_id).sampling_config   （举例）
        from .models import Project  # 按你仓库的模型改
        proj = Project.objects.filter(pk=project_id).first()
        sc = getattr(proj, "sampling_config", {}) or {}
        curve_points = int(sc.get("curve_points", 8))
        qc_groups    = int(sc.get("qc_groups", 3))
        qc_levels    = int(sc.get("qc_levels", 2))
    except Exception:
        curve_points, qc_groups, qc_levels = 8, 3, 2  # 兜底

    # 2) 解析文件名得到 plate_number/offset
    file_basename = os.path.basename(csv_abs_path)
    plate_number, start_offset = _parse_plate_meta_by_filename(file_basename)

    # 3) 构建 STD/QC 单元
    std_qc_items = _build_curve_and_qc_cells(curve_points, qc_groups, qc_levels, file_basename)
    std_qc_coords = _linear_fill_vertical_from_A1(len(std_qc_items))  # [(row, col),...]
    std_qc_cells = [
        {"row": r, "col": c, "text": it["label"], "kind": it["kind"]}
        for it, (r, c) in zip(std_qc_items, std_qc_coords)
    ]

    # 4) 临床样品（H2 起始在“R 逻辑布局”下自然满足；如需强制从 H2 连续填充，另加规则——等你确认）
    station_map = _load_station_map_for_today(today)
    clinical_cells = []
    try:
        clinical_cells = _build_clinical_cells_from_csv(csv_abs_path, start_offset, station_map)
        # clinical_cells: [(row_letter, col_num, sample_name)]
        clinical_cells = [{"row": r, "col": c, "text": s or ""} for (r, c, s) in clinical_cells]
    except Exception:
        clinical_cells = []

    # 5) 组装 96 孔板矩阵 -> 二级字典 grid[row][col]
    grid: dict[str, dict[int, dict]] = {r: {c: {"std_qc": None, "sample": None} for c in _PLATE_COLS} for r in _PLATE_ROWS}

    for cell in std_qc_cells:
        grid[cell["row"]][cell["col"]]["std_qc"] = cell
    for cell in clinical_cells:
        grid[cell["row"]][cell["col"]]["sample"] = cell

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

            "match_sample": "STD0",

            # 兼容通用版的常用键（先留空/占位，后续若接入可补齐）
            "origin_barcode": "",
            "barcode": "",
            "status": "",
            "is_locator": False,            # TECAN 目前无定位孔概念，先 False
            "flags": [],                    # 预留：重复/一对多等
            "meta": {},

            # 你可能在通用版用到的若干扩展键，先给默认值，避免模板/导出报错
            "MatchResult": "",
            "MatchSampleName": sample.get("text") or "",
            "DupBarcode": "",
            "DupBarcodeSampleName": "",
            "Warm": "",
        }

        # 为了跟通用版“孔内多行”展示兼容，额外带上 TECAN 的两条文本
        d["std_qc_text"]   = std_qc.get("text") or ""
        d["sample_text"]   = sample.get("text") or ""

        return d

    letters = ["A","B","C","D","E","F","G","H"]
    nums = [str(i) for i in range(1, 13)]

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

    ic(worksheet_table)

    return render(request, "dashboard/ProcessResult_TECAN.html",locals())






