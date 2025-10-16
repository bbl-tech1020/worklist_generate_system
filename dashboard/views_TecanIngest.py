from __future__ import annotations

import os, re, uuid, io
from datetime import datetime
from typing import Dict, List, Tuple, Set

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse, HttpResponseBadRequest
from django.views.decorators.csrf import csrf_protect
from django.utils.html import escape

import pandas as pd

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

def _parse_tecan_csv_abs(path: str) -> pd.DataFrame:
    """
    解析 Tecan 扫码 CSV（分号分隔），抽取：
    RowID / SRCTubeID / MainBarcode / GridPos / TipNumber
    """
    df = pd.read_csv(path, sep=";", engine="python", dtype=str)

    # 容错匹配列名（转小写去空格）
    cols = {c.lower().strip(): c for c in df.columns}

    def pick(*cands):
        for k in cands:
            if k in cols:
                return cols[k]
        return None

    col_src  = pick("srctubeid", "src_tube_id", "tubeid", "src")
    col_grid = pick("gridpos", "grid_pos", "grid")
    col_tip  = pick("tipnumber", "tip", "tpositionid", "tip_number")

    if not col_src or not col_grid or not col_tip:
        raise ValueError("CSV缺少必要列：SRCTubeID / GridPos / TipNumber")

    out = pd.DataFrame({
        "RowID": range(1, len(df) + 1),
        "SRCTubeID": df[col_src].astype(str).fillna(""),
        "GridPos":   df[col_grid].astype(str).str.extract(r"(\d+)")[0].astype(int),
        "TipNumber": df[col_tip].astype(str).str.extract(r"(\d+)")[0].astype(int),
    })
    out["MainBarcode"] = out["SRCTubeID"].astype(str).str.split("-", n=1).str[0]
    return out

def _collect_history_mainbarcodes(processed_dir: str) -> Set[str]:
    """
    遍历 processed 目录里的所有 csv，汇总历史 MainBarcode 集合
    只要有列 MainBarcode 就读出来
    """
    s: Set[str] = set()
    if not os.path.isdir(processed_dir):
        return s
    for name in os.listdir(processed_dir):
        if not name.lower().endswith(".csv"):
            continue
        p = os.path.join(processed_dir, name)
        try:
            df = pd.read_csv(p, dtype=str)
            if "MainBarcode" in df.columns:
                s.update(df["MainBarcode"].dropna().astype(str).tolist())
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
    dup_mask = df["MainBarcode"].duplicated(keep=False)
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

def _write_processed_copy(df: pd.DataFrame, original_name: str) -> str:
    """
    无冲突时，把本次数据直接写入 processed 目录的 csv
    返回：绝对路径
    """
    processed_dir = os.path.join(settings.MEDIA_ROOT, "tecan", "processed")
    _ensure_dir(processed_dir)
    # 以时间戳+原名的方式保存，避免重名
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name = f"{ts}__{original_name}.csv"
    out_path = os.path.join(processed_dir, out_name)
    df.to_csv(out_path, index=False)
    return out_path


# ======== 视图函数（Step1：解析 + 去重） ========

@login_required
@csrf_protect
def tecaningest(request: HttpRequest) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseBadRequest("仅支持POST")

    # 前端保持“基本不变”，这里只取我们需要的字段/文件
    project_id   = request.POST.get("project_id", "").strip()
    station_file = request.FILES.get("station_list")  # 这一步不使用，但可保存做记录
    scan_file    = request.FILES.get("scan_result")

    if not scan_file:
        return HttpResponseBadRequest("请上传‘扫码结果表’(CSV)")

    # 保存上传文件，便于复现
    saved_scan_abs = _save_upload_to_media("tecan/original", scan_file)
    if station_file:
        _save_upload_to_media("tecan/station", station_file)  # 先留存，后续步骤会用

    # 1) 解析 CSV
    try:
        df = _parse_tecan_csv_abs(saved_scan_abs)
    except Exception as e:
        return HttpResponseBadRequest(f"CSV解析失败：{e}")

    # 2) 汇总历史 + 去重检测
    processed_dir = os.path.join(settings.MEDIA_ROOT, "tecan", "processed")
    history = _collect_history_mainbarcodes(processed_dir)
    conflicts = _detect_conflicts(df, history)

    # 渲染一个简易结果页（本步骤只需展示结果；无冲突就直接写入 processed）
    has_conflict = bool(conflicts["intra"] or conflicts["cross"])

    html = io.StringIO()
    html.write("<div class='card'><div class='card-body'>")
    html.write("<h5 class='card-title'>Tecan Step1：解析与去重检查</h5>")
    html.write(f"<p>共解析到 <b>{len(df)}</b> 行；抽取字段：RowID / SRCTubeID / MainBarcode / GridPos / TipNumber。</p>")

    if has_conflict:
        # 显示冲突
        html.write("<div class='alert alert-danger'>检测到重复条码，请后续处理冲突再继续。</div>")

        if conflicts["intra"]:
            html.write("<h6>① 本次文件内重复</h6>")
            html.write("<table class='table table-sm table-bordered'><thead>")
            html.write("<tr><th>#</th><th>RowID</th><th>MainBarcode</th><th>SRCTubeID</th></tr></thead><tbody>")
            for i, r in enumerate(conflicts["intra"], 1):
                html.write(
                    f"<tr><td>{i}</td><td>{r['RowID']}</td>"
                    f"<td>{escape(r['MainBarcode'])}</td>"
                    f"<td>{escape(r['SRCTubeID'])}</td></tr>"
                )
            html.write("</tbody></table>")

        if conflicts["cross"]:
            html.write("<h6>② 与历史 processed 冲突（MainBarcode 重复）</h6>")
            html.write("<ul>")
            for r in conflicts["cross"]:
                html.write(f"<li>{escape(r['MainBarcode'])}</li>")
            html.write("</ul>")

        html.write("</div></div>")
        return HttpResponse(html.getvalue())

    # 无冲突：写入 processed
    out_path = _write_processed_copy(df, os.path.basename(saved_scan_abs))
    rel = os.path.relpath(out_path, settings.MEDIA_ROOT).replace(os.sep, "/")
    url = f"{getattr(settings, 'MEDIA_URL', '/media/')}{rel}"

    html.write("<div class='alert alert-success'>未检测到重复条码，已自动写入 processed。</div>")
    html.write(f"<p>processed 文件：<a href='{url}' target='_blank'>{escape(os.path.basename(out_path))}</a></p>")
    html.write("</div></div>")
    return HttpResponse(html.getvalue())