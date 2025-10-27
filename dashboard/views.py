
from django.views.decorators.http import require_POST,require_GET
from django.views.decorators.csrf import csrf_exempt
from django.shortcuts import render, redirect, get_object_or_404
from .models import *
from django.http import JsonResponse,HttpResponseRedirect,HttpResponse, HttpResponseBadRequest,FileResponse, Http404
from django.conf import settings
from django.template.loader import render_to_string,get_template
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile
from django.utils import timezone
from django.utils.encoding import escape_uri_path
from django.urls import reverse
from .forms import *

import xlrd
from datetime import datetime,date
from icecream import ic
from collections import defaultdict, deque
import pandas as pd
pd.set_option('display.max_rows', None)

import os, io, logging
import re
from io import StringIO,BytesIO
import openpyxl
import xlwt
from openpyxl import Workbook
from pathlib import Path
from math import ceil
from weasyprint import HTML, CSS
from weasyprint.text.fonts import FontConfiguration

# Create your views here.
def home(request):
    return render(request, "dashboard/index.html")

def user_manual(request):
    # 这里也可以做权限控制（如仅登录可见）
    return render(request, "dashboard/user_manual.html")

# 1 前端
# 前端功能入口
def frontend_entry(request):
    return render(request, 'dashboard/frontend/index.html')

# 关联后台参数配置中已设置的所有项目
def get_project_list(request):
    configs = SamplingConfiguration.objects.all()
    data = [
        {'id': c.id, 'name': c.project_name}
        for c in configs
    ]
    return JsonResponse({'projects': data})

# 选择具体某个项目时，关联后台参数配置中该项目的配置信息
def get_project_detail(request, pk):
    configs = SamplingConfiguration.objects.get(pk=pk)
    data = {
        'id': configs.id,
        'name': configs.project_name,
        'default_upload_instrument': configs.default_upload_instrument
    }
    return JsonResponse(data)

@require_GET
def get_injection_plates(request):
    """
    根据项目名称(project_name)与仪器编号(instrument_num)返回配置的进样盘号列表。
    兼容 CharField/JSONField 两种历史存储格式。
    """
    project_name = request.GET.get("project_name", "").strip()
    instrument_num = request.GET.get("instrument_num", "").strip()

    plates = []
    if project_name and instrument_num:
        try:
            cfg = InjectionPlateConfiguration.objects.get(
                project_name=project_name,
                instrument_num=instrument_num,
            )
            raw = cfg.injection_plate
            # 兼容：字符串（逗号分隔）或 JSON list
            if isinstance(raw, str):
                plates = [s.strip() for s in raw.split(",") if s.strip()]
            elif isinstance(raw, (list, tuple)):
                plates = list(raw)
            else:
                plates = []
        except InjectionPlateConfiguration.DoesNotExist:
            plates = []

    return JsonResponse({"plates": plates})

# NIMBUS
def NIMBUS_sampling(request):
    return render(request, 'dashboard/sampling/NIMBUS.html')

# Starlet
def Starlet_sampling(request):
    return render(request, 'dashboard/sampling/Starlet.html')

def Starlet_qyzl(request):
    if request.method == 'POST' and request.FILES.get('input_file'):
        input_file = request.FILES['input_file']
        path = default_storage.save('tmp/' + input_file.name, ContentFile(input_file.read()))
        tmp_file = os.path.join(default_storage.location, path)

        wb = openpyxl.load_workbook(tmp_file, data_only=True)
        ws = wb.active

        AreaStartNum = int(ws['I1'].value)
        AreaRowNum = ws['K1'].value
        AreaStartPositionID = "B1"

        center_style = xlwt.XFStyle()
        alignment = xlwt.Alignment()
        alignment.horz = xlwt.Alignment.HORZ_CENTER
        alignment.vert = xlwt.Alignment.VERT_CENTER
        center_style.alignment = alignment

        # 获取条码所在区域
        # Step 1: 自动检测“最大行和最大列”
        min_row, min_col = 3, 2  # B3
        max_row = ws.max_row
        max_col = ws.max_column

        # 1.1 找到从B3开始实际有数据的最大行和最大列
        actual_max_row, actual_max_col = min_row, min_col

        for row in range(min_row, ws.max_row+1):
            for col in range(min_col, ws.max_column+1):
                cell_value = ws.cell(row=row, column=col).value
                if cell_value:  # 有值
                    if row > actual_max_row:
                        actual_max_row = row
                    if col > actual_max_col:
                        actual_max_col = col

        # Step 2: 从B3到实际最大行/列遍历，采集所有有值的条码，仍然按“按列优先”
        barcode_cells = ws.iter_cols(
            min_col=min_col, max_col=actual_max_col,
            min_row=min_row, max_row=actual_max_row
        )
        all_barcodes = [cell.value for col in barcode_cells for cell in col if cell.value]

        barcodes = all_barcodes[12:]
        OutputRowNum = len(barcodes)
        AreaNum = ceil(OutputRowNum / AreaRowNum)

        output = xlwt.Workbook()

        if AreaStartNum<=5:
            sheet = output.add_sheet("Import_1_5_Worklist")
        elif AreaStartNum<=10:
            sheet = output.add_sheet("Import_6_10_Worklist")
        elif AreaStartNum<=15:
            sheet = output.add_sheet("Import_11_15_Worklist")
        else:
            sheet = output.add_sheet("Import_16_20_Worklist")

        # sheet = output.add_sheet("Import_1_5_Worklist")

        sheet.col(1).width = 8 * 256
        sheet.col(2).width = 30 * 256
        sheet.col(3).width = 15 * 256
        sheet.col(4).width = 30 * 256
        sheet.col(5).width = 35 * 256
        sheet.col(6).width = 15 * 256
        sheet.col(7).width = 8 * 256

        headers = ["", "Index", "SourceLabwareID", "SourcePositionID", "SourceBarcode", "TargetLabwareID", "TargetPositionID", "Volume"]
        for col, head in enumerate(headers):
            sheet.write(0, col, head, center_style)

        def next_source_labware(n):
            return f"SMP_CAR_32_12x75_A00_{str(n).zfill(4)}"

        def get_target_labware_id(area_index, AreaStartNum):
            if AreaStartNum % 5 == 0:
                return f"Cos_96_DW_2mL_{str(5 + area_index).zfill(4)}"
            else:
                return f"Cos_96_DW_2mL_{str(AreaStartNum % 5 + area_index).zfill(4)}"

        def generate_target_position_ids(start, count, skip_list=None):
            if skip_list is None:
                skip_list = set()
            col_letter = start[0].upper()
            row_number = int(start[1:])
            result = []
            current_letter = col_letter
            current_number = row_number
            generated = 0
            while generated < count:
                pos = f"{current_letter}{current_number}"
                if pos not in skip_list:
                    result.append(pos)
                    generated += 1
                current_number += 1
                if current_number > 12:
                    current_number = 1
                    current_letter = chr(ord(current_letter) + 1)
            return result

        # -----------修正部分计数器--------------
        row_num = 1
        fixed_barcodes = [ws.cell(row=r, column=2).value for r in range(3, 15) if ws.cell(row=r, column=2).value]

        # 以下变量专用于“实际样本”（不含前12条曲线/质控）
        sample_labware_id_block = 1    # 只针对实际样本自增
        sample_position_id = 13        # 实际样本 SourcePositionID 从13开始

        for area_index in range(AreaNum):
            start_index = area_index * AreaRowNum
            end_index = min(start_index + AreaRowNum, OutputRowNum)
            area_barcodes = barcodes[start_index:end_index]

            total_barcodes = fixed_barcodes + area_barcodes

            start_row = row_num
            end_row = row_num + len(total_barcodes) - 1
            sheet.write_merge(start_row, end_row, 0, 0, f"{area_index + AreaStartNum}", center_style)
            target_labware_id = get_target_labware_id(area_index, AreaStartNum)

            fixed_positions = [f"A{n}" for n in range(1, 13)]
            skip_list = {f"B{AreaStartNum+area_index}"}
            dynamic_positions = generate_target_position_ids(AreaStartPositionID, len(area_barcodes), skip_list)
            total_positions = fixed_positions + dynamic_positions

            for index, (barcode, position) in enumerate(zip(total_barcodes, total_positions)):
                sheet.write(row_num, 1, index + 1, center_style)

                if index < 12:
                    # 前12条曲线/质控
                    sheet.write(row_num, 2, "SMP_CAR_32_12x75_A00_0001", center_style)
                    sheet.write(row_num, 3, index + 1, center_style)
                else:
                    # 实际样本部分
                    sheet.write(row_num, 2, next_source_labware(sample_labware_id_block), center_style)
                    sheet.write(row_num, 3, sample_position_id, center_style)

                    # 只在实际样本递增和判断
                    sample_position_id += 1
                    if sample_position_id > 32:
                        sample_position_id = 1
                        sample_labware_id_block += 1

                sheet.write(row_num, 4, barcode, center_style)
                sheet.write(row_num, 5, target_labware_id, center_style)
                sheet.write(row_num, 6, position, center_style)
                sheet.write(row_num, 7, 100, center_style)

                row_num += 1

        output_stream = BytesIO()
        output.save(output_stream)
        output_stream.seek(0)
        today_str = datetime.today().strftime("%Y-%m-%d")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"qyzl_plate_{AreaStartNum}_{AreaStartNum+AreaNum-1}_{timestamp}.xls"

        # 构造保存路径：Starlet/取样指令/YYYY-MM-DD/
        save_dir = os.path.join(settings.DOWNLOAD_ROOT, "Starlet", "取样指令", today_str)
        os.makedirs(save_dir, exist_ok=True)

        # 写入文件
        save_path = os.path.join(save_dir, file_name)
        with open(save_path, "wb") as f:
            f.write(output_stream.getvalue())

        # 生成“文件下载”页 URL（若你的 urls.py 没给这个路由起名，可直接用硬编码 '/dashboard/file_download/'）
        try:
            download_page_url = reverse("file_download")
        except Exception:
            download_page_url = "/dashboard/file_download/"

        # 直接文件 URL（静态暴露目录 + 平台/类别/日期/文件名）
        download_file_url = f"{settings.DOWNLOAD_URL}Starlet/取样指令/{today_str}/{file_name}"

        # 弹窗要显示的提示语
        popup_message = f"取样指令已生成：{file_name}（已保存至：Starlet/取样指令/{today_str}/）"

        return render(request, "dashboard/sampling/Starlet_qyzl.html", {
            "popup_message": popup_message,
            "download_page_url": download_page_url,
            "download_file_url": download_file_url,
        })

    return render(request, 'dashboard/sampling/Starlet_qyzl.html')


def Starlet_worksheet(request):
    return render(request, 'dashboard/sampling/Starlet_worksheet.html')

# Tecan
def Tecan_sampling(request):
    return render(request, 'dashboard/sampling/Tecan.html')

# 2 标本查找
def sample_search(request):
    return render(request, 'dashboard/sample_search/index.html')


def sample_search_stats_today(request):
    """
    返回：当天（record_date=today）的所有 project_name 的统计结果。
    统计口径和你给的 statistics(process_data) 基本一致：
      - 每个项目：根据 sample_name 自动提取字母前缀，或使用项目定制前缀集
      - 统计：实验号总数、起始/末尾实验号、空号（含区间描述）、共血、多血
    """
    today = datetime.now().date()
    qs = SampleRecord.objects.filter(record_date=today)

    # 若当天无数据，直接返回空结果
    if not qs.exists():
        return JsonResponse({"today_date": today.strftime("%Y-%m-%d"), "projects": []})

    # 可选：按项目设定“允许前缀”集合；项目未配置时自动从数据中抽取
    allowed_prefix_map = {
        "VAE": {"AE", "VF", "V", "ST"},
        "VD":  {"VD", "VF", "V", "YVD", "ST"},
        # 其他项目留空 → 自动提取
    }

    def process_project(records, allowed_prefixes=None):
        """
        records: 当个项目的 Queryset
        allowed_prefixes: set[str] | None
        返回：与示例一致的列表
        """
        # 收集候选 sample_name（兼容 'AE1234-VD5678' 这种 "-" 连接的情况）
        sample_names = []
        for r in records:
            if r.sample_name:
                parts = str(r.sample_name).split('-')
                if parts:
                    sample_names.append(parts[0])
                    if len(parts) > 1:
                        sample_names.append(parts[1])

        # 自动提取所有(字母+数字)的字母前缀，如 "VD123" → "VD"
        detected_prefixes = set()
        for name in sample_names:
            m = re.match(r"^[A-Za-z]+(?=\d)", str(name))
            if m:
                detected_prefixes.add(m.group())

        # 选择要使用的前缀集合
        prefixes = allowed_prefixes if allowed_prefixes else detected_prefixes

        # ★ 新增：统计时剔除 QC / STD 前缀
        prefixes = {p for p in prefixes if p not in {"QC", "STD"}}

        # 构造“条码→样本名集合”、“样本名→出现次数”，用于“共血/多血”
        barcode_to_samples = defaultdict(set)
        sample_count = defaultdict(int)
        for r in records:
            name_head = str(r.sample_name).split('-')[0] if r.sample_name else ""
            barcode_to_samples[r.barcode].add(name_head)
            sample_count[name_head] += 1

        # 生成统计行
        result_rows = []
        for prefix in sorted(prefixes):
            # 仅统计纯 '前缀+数字' 的样本名
            matching = [n for n in sample_names if re.fullmatch(fr"{prefix}\d+", str(n))]

            nums = sorted(
                int(re.sub(r"^[A-Za-z]+", "", n))
                for n in matching
                if re.sub(r"^[A-Za-z]+", "", n).isdigit()
            )

            total = len(matching)
            start_number = f"{prefix}{nums[0]}" if nums else None
            end_number   = f"{prefix}{nums[-1]}" if nums else None

            # 空号（找缺口）
            missing_ranges = []
            empty_count = 0
            for i in range(len(nums) - 1):
                gap = nums[i+1] - nums[i] - 1
                if gap > 0:
                    empty_count += gap
                    if gap == 1:
                        missing_ranges.append(f"{prefix}{nums[i] + 1}")
                    else:
                        missing_ranges.append(f"{prefix}{nums[i]+1}-{prefix}{nums[i+1]-1}")
            empty_info = ", ".join(missing_ranges)

            # 共血：同一个样本名映射多个条码（以样本名匹配、条码计数>1）
            shared_blood_samples = [
                name for name, barcodes in barcode_to_samples.items()
                if len(barcodes) > 1 and re.fullmatch(fr"{prefix}\d+", str(name))
            ]
            shared_blood_info  = ", ".join(sorted(set(shared_blood_samples)))
            shared_blood_count = len(set(shared_blood_samples))

            # 多血：同一个样本名在当天记录出现多次（>1）
            multi_blood_samples = [
                name for name, cnt in sample_count.items()
                if cnt > 1 and re.fullmatch(fr"{prefix}\d+", str(name))
            ]
            multi_blood_info  = ", ".join(sorted(set(multi_blood_samples)))
            multi_blood_count = len(set(multi_blood_samples))

            result_rows.append({
                "prefix":      prefix,
                "total":       total,
                "start":       start_number,
                "end":         end_number,
                "empty":       f"{empty_count}（{empty_info}）" if empty_count else "0",
                "sharedBlood": f"{shared_blood_count}（{shared_blood_info}）" if shared_blood_count else "0",
                "multiBlood":  f"{multi_blood_count}（{multi_blood_info}）" if multi_blood_count else "0",
            })
        return result_rows

    # 分项目统计
    projects_payload = []
    for proj in qs.values_list("project_name", flat=True).distinct().order_by("project_name"):
        proj_qs = qs.filter(project_name=proj)
        prefixes = allowed_prefix_map.get(proj)  # 未配置则为 None → 自动检测
        stats = process_project(proj_qs, prefixes)
        projects_payload.append({
            "project_name": proj,
            "statistics": stats
        })

    return JsonResponse({
        "today_date": today.strftime("%Y-%m-%d"),
        "projects": projects_payload
    })

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def download_export(request, platform, date_name, project, filename):
    """
    统一的下载入口：无论 .pdf/.xlsx/.txt，都以 attachment 方式下载
    路径受限在 settings.DOWNLOAD_ROOT 下，防止越权访问
    """
    root = settings.DOWNLOAD_ROOT
    # 组装并规范化路径
    fpath = os.path.normpath(os.path.join(root, platform, date_name, project, filename))

    # 安全校验：必须在 DOWNLOAD_ROOT 内
    if not fpath.startswith(os.path.abspath(root) + os.sep):
        raise Http404("Invalid path")

    if not os.path.exists(fpath) or not os.path.isfile(fpath):
        raise Http404("File not found")

    # 统一用 FileResponse + as_attachment=True 强制下载
    # content_type 用二进制更稳妥，浏览器不会尝试内联展示
    resp = FileResponse(open(fpath, "rb"), as_attachment=True, filename=filename, content_type="application/octet-stream")

    # 兼容一些旧浏览器/中文文件名
    resp["Content-Disposition"] = f"attachment; filename*=UTF-8''{escape_uri_path(filename)}"
    return resp


def file_download(request):
    """
    展示 downloads 目录结构。
    - 通用：平台 / 日期 / 项目 / 文件
    - Starlet：平台 / {工作清单和上机列表 | 取样指令} / 日期 / (项目?) / 文件
    """
    root = settings.DOWNLOAD_ROOT
    os.makedirs(root, exist_ok=True)

    groups = []  # 输出给模板

    for platform in sorted(os.listdir(root)):  # 平台层
        p_path = os.path.join(root, platform)
        if not os.path.isdir(p_path):
            continue

        # —— Starlet：多一层“类别”（工作清单和上机列表 / 取样指令）——
        if platform == "Starlet":
            category_names = ["工作清单和上机列表", "取样指令"]
            categories = []

            for cat in category_names:
                c_path = os.path.join(p_path, cat)
                if not os.path.isdir(c_path):
                    continue

                days = []
                # 日期倒序
                for date_name in sorted(os.listdir(c_path), reverse=True):
                    d_path = os.path.join(c_path, date_name)
                    if not (DATE_RE.match(date_name) and os.path.isdir(d_path)):
                        continue

                    # 判断“日期目录”下面是否还有项目层
                    proj_dirs = sorted([
                        s for s in os.listdir(d_path)
                        if os.path.isdir(os.path.join(d_path, s))
                    ])

                    if proj_dirs:
                        # 有项目层：平台 / 类别 / 日期 / 项目 / 文件
                        projects = []
                        for proj in proj_dirs:
                            proj_path = os.path.join(d_path, proj)
                            files = []
                            for fname in sorted(os.listdir(proj_path)):
                                fpath = os.path.join(proj_path, fname)
                                if not os.path.isfile(fpath):
                                    continue
                                files.append({
                                    "name": fname,
                                    "url": f"{settings.DOWNLOAD_URL}{platform}/{cat}/{date_name}/{proj}/{fname}",
                                    "force_download": ext.endswith((".pdf", ".txt", ".xlsx", ".xls", ".csv")),
                                })
                            projects.append({"name": proj, "files": files})

                        days.append({"date": date_name, "projects": projects})

                    else:
                        # 无项目层：平台 / 类别 / 日期 / 文件（用于“取样指令”）
                        files = []
                        for fname in sorted(os.listdir(d_path)):
                            fpath = os.path.join(d_path, fname)
                            if not os.path.isfile(fpath):
                                continue
                            files.append({
                                "name": fname,
                                "url": f"{settings.DOWNLOAD_URL}{platform}/{cat}/{date_name}/{fname}",
                                "force_download": ext.endswith((".pdf", ".txt", ".xlsx", ".xls", ".csv")),
                            })
                        days.append({"date": date_name, "files": files})

                categories.append({"category": cat, "days": days})

            groups.append({"group": platform, "categories": categories})
            continue  # Starlet 处理完，跳到下一个平台

        # —— 其他平台（沿用旧三层）：平台 / 日期 / 项目 / 文件 ——
        days = []
        for date_name in sorted(os.listdir(p_path), reverse=True):
            d_path = os.path.join(p_path, date_name)
            if not (DATE_RE.match(date_name) and os.path.isdir(d_path)):
                continue

            projects = []
            for proj in sorted(os.listdir(d_path)):
                proj_path = os.path.join(d_path, proj)
                if not os.path.isdir(proj_path):
                    continue

                files = []
                for fname in sorted(os.listdir(proj_path)):
                    fpath = os.path.join(proj_path, fname)
                    ext = fname.lower()
                    if not os.path.isfile(fpath):
                        continue
                    files.append({
                        "name": fname,
                        "url": f"{settings.DOWNLOAD_URL}{platform}/{date_name}/{proj}/{fname}",
                        "force_download": ext.endswith((".pdf", ".txt", ".xlsx", ".xls", ".csv")),
                    })

                projects.append({"name": proj, "files": files})

            days.append({"date": date_name, "projects": projects})

        groups.append({"group": platform, "days": days})

    return render(request, "dashboard/file_download.html", {"groups": groups})


# 3 后台参数配置
def project_config(request):
    project_configs = SamplingConfiguration.objects.all().order_by('-created_at')
    return render(request, 'dashboard/config/project_config.html', {
        'project_configs': project_configs
    })

def project_config_create(request):
    if request.method == 'POST':
        # 1. 判断是哪种取样方式
        sampling_method = request.POST.get('sampling_method')
        instance = SamplingConfiguration(
            project_name=request.POST.get('project_name'),
            project_name_full=request.POST.get('project_name_full'),
            sampling_method=sampling_method,
            curve_points=request.POST.get('curve_points'),
            qc_groups=request.POST.get('qc_groups'),
            qc_levels=request.POST.get('qc_levels'),
            qc_insert=request.POST.get('qc_insert'),
            test_count=request.POST.get('test_count'),
            layout=request.POST.get('layout'),
            default_upload_instrument=request.POST.get('default_upload_instrument'),
            mapping_file=request.FILES.get('mapping_file'),
        )

        instance.save()
        return redirect('project_config')
    else:
        curve_range = list(range(6, 11))  # [6, 7, 8, 9, 10]
    return render(request, 'dashboard/config/project_config_create.html', {
        'curve_range': curve_range
    })

def project_config_view(request, pk):
    config = get_object_or_404(SamplingConfiguration, pk=pk)
    return render(request, 'dashboard/config/project_config_view.html', {
        'config': config
    })

def project_config_edit(request, pk):
    config = get_object_or_404(SamplingConfiguration, pk=pk)
    if request.method == 'POST':
        # 保存旧文件引用
        old_mapping_file = config.mapping_file.path if config.mapping_file else None

        form = SamplingConfigurationForm(request.POST, request.FILES, instance=config)
        if form.is_valid():
            # 检查是否上传新文件，如果上传了，删除原文件
            if 'mapping_file' in request.FILES and old_mapping_file and os.path.exists(old_mapping_file):
                os.remove(old_mapping_file)

            form.save()
            return redirect('project_config')
    else:
        curve_range = list(range(6, 11)) 
        form = SamplingConfigurationForm(instance=config)
    return render(request, 'dashboard/config/project_config_edit.html', {
        'form': form,
        'config': config,
    })


def project_config_delete(request, pk):
    if request.method == "POST":
        config = get_object_or_404(SamplingConfiguration, pk=pk)
        config.delete()
        if request.headers.get('x-requested-with') == 'XMLHttpRequest':
            return JsonResponse({"success": True})
        return redirect('config_preview')
    return JsonResponse({"success": False, "error": "Only POST allowed"})

def vendor_config(request):
    instrument_configs = InstrumentConfiguration.objects.all().order_by('-created_at')
    return render(request, 'dashboard/config/vendor_config.html', {
        'instrument_configs': instrument_configs
    })

def vendor_config_create(request):
    if request.method == 'POST':
        instance = InstrumentConfiguration(
            instrument_name=request.POST.get('instrument_name'),
            instrument_num=request.POST.get('instrument_num'),
            systerm_num=request.POST.get('systerm_num'),
            upload_file=request.FILES.get('upload_file'),
        )

        instance.save()
        return redirect('vendor_config')
    else:
        return render(request, 'dashboard/config/vendor_config_create.html')
    
def vendor_config_delete(request, pk):
    if request.method == "POST":
        config = get_object_or_404(InstrumentConfiguration, pk=pk)
        config.delete()
        if request.headers.get('x-requested-with') == 'XMLHttpRequest':
            return JsonResponse({"success": True})
        return redirect('vendor_config')
    return JsonResponse({"success": False, "error": "Only POST allowed"})

# 进样体积设置   
def injection_volume_config(request):
    injection_volume_configs = InjectionVolumeConfiguration.objects.all().order_by('-created_at')
    return render(request, 'dashboard/config/injection_volume_config.html', {
        'injection_volume_configs': injection_volume_configs
    })

def injection_volume_config_create(request):
    if request.method == 'POST':
        instance = InjectionVolumeConfiguration(
            project_name=request.POST.get('project_name'),
            instrument_num=request.POST.get('instrument_num'),
            injection_volume=request.POST.get('injection_volume'),
        )

        instance.save()
        return redirect('injection_volume_config')
    else:
        return render(request, 'dashboard/config/injection_volume_config_create.html')

def injection_volume_config_delete(request, pk):
    if request.method == "POST":
        config = get_object_or_404(InjectionVolumeConfiguration, pk=pk)
        config.delete()
        if request.headers.get('x-requested-with') == 'XMLHttpRequest':
            return JsonResponse({"success": True})
        return redirect('config_preview')
    return JsonResponse({"success": False, "error": "Only POST allowed"})

# 进样盘号设置   
def injection_plate_config(request):
    configs = InjectionPlateConfiguration.objects.all().order_by('-created_at')
    return render(request, 'dashboard/config/injection_plate_config.html', {
        'configs': configs
    })

# 进样盘号配置 —— 新建
def injection_plate_config_create(request):
    if request.method == 'POST':
        # 前端以 JSON 字符串传入 injection_plate_json，例如 ["Plate1","Plate2"]
        import json
        raw = request.POST.get('injection_plate_json', '[]')
        try:
            plate_list = json.loads(raw)
            # 简单清洗：去空、去重、全部转为字符串
            plate_list = list(dict.fromkeys([str(x).strip() for x in plate_list if str(x).strip()]))
        except Exception:
            plate_list = []

        instance = InjectionPlateConfiguration(
            project_name=request.POST.get('project_name', '').strip(),
            instrument_num=request.POST.get('instrument_num', '').strip(),
            injection_plate=plate_list,
        )
        instance.save()
        return redirect('injection_plate_config')
    else:
        return render(request, 'dashboard/config/injection_plate_config_create.html')


# 进样盘号配置 —— 删除
def injection_plate_config_delete(request, pk):
    if request.method == "POST":
        config = get_object_or_404(InjectionPlateConfiguration, pk=pk)
        config.delete()
        if request.headers.get('x-requested-with') == 'XMLHttpRequest':
            return JsonResponse({"success": True})
        return redirect('injection_plate_config')
    return JsonResponse({"success": False, "error": "Only POST allowed"})


###################### Starlet专用函数 ###################### 
def _tail_number(s: str):
    """提取字符串末尾的连续数字；失败则返回 None"""
    import re
    if not s:
        return None
    m = re.search(r'(\d+)$', str(s).strip())
    return int(m.group(1)) if m else None

def _starlet_split_plates(scan_sheet, index_map):
    """
    将 Starlet 的扫码 sheet,按 TLabwareId 的末尾数字 分为多块板。
    返回 plate_groups = [(plate_no_int, row_indexes), ...]
    - plate_no_int: 1,2,3...
    - row_indexes: 属于该板的行下标列表（从 1 开始，不含表头）
    """
    labware_idx = index_map.get("TLabwareId")
    pos_idx     = index_map.get("TPositionId", 0)

    # 兜底：若缺 TLabwareId，退化为通过孔位序列回退分板（A1..H12 每 96 个一板）
    if labware_idx is None:
        nrows = scan_sheet.nrows
        all_rows = list(range(1, nrows))
        # 简单分块：每 96 行一板
        groups = [all_rows[i:i+96] for i in range(0, len(all_rows), 96)]
        return [(i+1, g) for i, g in enumerate(groups)]

    # 主规则：取 TLabwareId 的末尾数字做板号
    bucket = {}
    nrows  = scan_sheet.nrows
    for i in range(1, nrows):
        lab_str = str(scan_sheet.row_values(i)[labware_idx]).strip()
        k = _tail_number(lab_str)
        # 若无法提取，使用回退：按孔位序号（A1..H12）分组
        if k is None:
            # 根据 TPositionId 转序号（A1=1, A2=2, ... H12=96），超过 96 继续分块
            pos = str(scan_sheet.row_values(i)[pos_idx]).strip()
            if len(pos) >= 2 and pos[0].isalpha():
                row = "ABCDEFGH".find(pos[0].upper())
                try:
                    col = int(pos[1:])
                except:
                    col = 1
                if 0 <= row <= 7 and 1 <= col <= 12:
                    seq = row * 12 + col         # 1..96
                    k = ((seq-1) // 96) + 1      # 退化逻辑：一起视为第1板
                else:
                    k = 1
            else:
                k = 1
        bucket.setdefault(k, []).append(i)

    plate_groups = sorted(bucket.items(), key=lambda x: x[0])  # [(n, [rows...]), ...]
    return plate_groups

# 把原先 Starlet 单板的 96 孔对齐 与 定位孔补齐 逻辑封装进来
def _process_one_starlet_plate(scan_sheet, index_map, row_indexes, plate_no_int):
    """
    传入某一板对应的行下标 row_indexes，完成：
      1) 读出 Status/Barcode
      2) 96 孔对齐（A1..A12, B1..B12, ... H12）
      3) 定位孔补齐：第 1~12 板用 B1..B12；第 13~24 用 C1..C12；以此类推
    返回字典：
      {
        "Position": [...], "Status": [...], "OriginBarcode": [...],
        "CutBarcode": [...], "SubBarcode": [...], "Warm": [...],
        "plate_no_str": "X{n}"      # 导出/展示用
      }
    """
    # 列索引
    p_idx = index_map.get("TPositionId", 0)
    s_idx = index_map.get("TSumStateDescription", 0)
    b_idx = index_map.get("SPositionBC", 0)

    # 先按 position -> row 信息字典
    row_by_pos = {}
    for i in row_indexes:
        pos = str(scan_sheet.row_values(i)[p_idx]).strip()
        row_by_pos[pos] = {
            "Status":        scan_sheet.row_values(i)[s_idx],
            "OriginBarcode": scan_sheet.row_values(i)[b_idx],
            "Warm":          ""   # Starlet 无 Warm
        }

    # 预期 96 孔顺序
    letters_fix = list("ABCDEFGH")
    nums_fix    = [str(i) for i in range(1, 13)]
    expected_positions = [f"{r}{c}" for r in letters_fix for c in nums_fix]

    # 对齐并在缺行时补齐
    Position, Status, OriginBarcode = [], [], []
    CutBarcode, SubBarcode, Warm    = [], [], []

    # 定位孔规则：第 1~12 板 -> B1..B12；13~24 -> C1..C12；……
    locator_rows = ["B","C","D","E","F","G","H"]  # 最多 7*12 = 84 块板可直接映射
    row_index = (plate_no_int - 1) // 12         # 第几行
    col_num   = ((plate_no_int - 1) % 12) + 1    # 第几列
    locator_target = None
    if row_index < len(locator_rows):
        locator_target = f"{locator_rows[row_index]}{col_num}"  # 如 B1, B2, ... C1, ...

    for pos in expected_positions:
        if pos in row_by_pos:
            status = row_by_pos[pos]["Status"]
            bc     = row_by_pos[pos]["OriginBarcode"]
            warm   = row_by_pos[pos]["Warm"]
        else:
            status, bc, warm = "Not used", "NOTUBE", ""
            # 缺行但命中定位孔位置时，用 X{n}
            if locator_target and pos == locator_target:
                bc = f"X{plate_no_int}"

        Position.append(pos)
        Status.append(status)
        Warm.append(warm)

        bc_str = str(bc)
        parts  = bc_str.split("-", 1)
        OriginBarcode.append(bc_str)
        CutBarcode.append(parts[0])
        SubBarcode.append("-" + parts[1] if len(parts) == 2 else "")

    return {
        "Position": Position,
        "Status": Status,
        "OriginBarcode": OriginBarcode,
        "CutBarcode": CutBarcode,
        "SubBarcode": SubBarcode,
        "Warm": Warm,
        "plate_no_str": f"X{plate_no_int}",
    }


# 结果处理，用户在前端功能入口处选择项目，上传文件并点击提交按钮后的处理逻辑
def ProcessResult(request):
    """
    同时支持：
      - NIMBUS：单板
      - Starlet：单板/多板（按 TLabwareId 末尾数字分组）
    """
    from collections import defaultdict, Counter, deque
    import os, re
    import xlrd
    import pandas as pd
    from io import StringIO
    from django.utils import timezone
    from django.http import HttpResponse, HttpResponseBadRequest
    from django.shortcuts import get_object_or_404, render
    from datetime import date

    # ========== 1. 入参与上传文件 ==========
    project_id      = request.POST.get("project_id")
    platform        = request.POST.get("platform")                # 'NIMBUS' | 'Starlet'
    injection_plate = request.POST.get("injection_plate") if 'injection_plate' in request.POST else None
    instrument_num  = request.POST.get("instrument_num")

    Stationlist = request.FILES.get('station_list')               # 每日操作清单
    Scanresult  = request.FILES.get('scan_result')                # 扫码结果
    if not (Stationlist and Scanresult and project_id and platform and instrument_num):
        return HttpResponseBadRequest("缺少必要参数或文件。")

    Stationtable = xlrd.open_workbook(filename=None, file_contents=Stationlist.read())
    Scantable    = xlrd.open_workbook(filename=None, file_contents=Scanresult.read())
    scan_sheet   = Scantable.sheets()[0]

    # 扫码表头索引
    nrows = scan_sheet.nrows
    ncols = scan_sheet.ncols
    scan_header = [str(scan_sheet.row_values(0)[i]).strip() for i in range(ncols)]
    scan_index  = {col: idx for idx, col in enumerate(scan_header)}

    # 关键列索引（容错：缺失时给默认 0）
    POS_IDX   = scan_index.get("TPositionId", 0)
    STAT_IDX  = scan_index.get("TSumStateDescription", 0)
    BC_IDX    = scan_index.get("SPositionBC", 0)
    WARM_IDX  = scan_index.get("Warm")               # NIMBUS有；Starlet通常无
    LABW_IDX  = scan_index.get("TLabwareId")        # Starlet有

    # ========== 2. 基础配置与映射（整次仅读一次） ==========
    config         = SamplingConfiguration.objects.get(id=project_id)
    project_name   = config.project_name
    mapping_path   = config.mapping_file.path
    df_mapping_wc  = pd.read_excel(mapping_path, sheet_name="工作清单")   # for worksheet
    df_worklistmap = pd.read_excel(mapping_path, sheet_name="上机列表")    # worklist mapping 模板

    # 解析后台设置的上机模板（txt/csv）→ DataFrame（只需列名 / txt_headers）,获取表头
    instrument_config = get_object_or_404(InstrumentConfiguration, instrument_num=instrument_num)
    instrument_name = instrument_config.instrument_name
    ic(instrument_name)
    if not instrument_config.upload_file:
        return HttpResponse("未设置该上机仪器对应的上机模板,请设置后再试", status=404)

    ext = os.path.splitext(instrument_config.upload_file.name)[1].lower()
    if ext not in (".txt", ".csv"):
        return HttpResponse(f"仅支持 .txt 或 .csv 模板，当前为：{ext}", status=400)

    with instrument_config.upload_file.open("rb") as f:
        raw = f.read()
    text, last_err = None, None
    for enc in ("utf-8", "utf-8-sig", "gb18030"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError as e:
            last_err = e
    if text is None:
        raise last_err or UnicodeDecodeError("unknown", b"", 0, 1, "unable to decode upload_file")
    df_template = pd.read_csv(StringIO(text), sep=None, engine="python", dtype=str)
    txt_headers = df_template.columns.tolist()

    # 岗位清单主条码 ↔ 实验号（获取一一对应关系）
    st_sheet   = Stationtable.sheets()[0]
    st_nrows   = st_sheet.nrows
    st_ncols   = st_sheet.ncols
    st_header  = [str(st_sheet.row_values(0)[i]).strip() for i in range(st_ncols)]
    st_index   = {col: idx for idx, col in enumerate(st_header)}
    MB_IDX     = st_index.get("主条码", 0)
    SN_IDX     = st_index.get("实验号", 0)
    MainBarcode = [str(st_sheet.row_values(i)[MB_IDX]) for i in range(1, st_nrows)]
    SampleName  = [str(st_sheet.row_values(i)[SN_IDX]) for i in range(1, st_nrows)]
    barcode_to_names = defaultdict(list)
    for bc, sn in zip(MainBarcode, SampleName):
        barcode_to_names[str(bc)].append(str(sn))

    # 曲线/质控映射（获取一一对应关系,供后续识别非临床样本）
    barcode_to_name = dict(zip(df_mapping_wc["Barcode"].astype(str), df_mapping_wc["Name"]))

    # 用于构建曲线/QC/Test/DB 序列
    test_count    = config.test_count
    curve_points  = config.curve_points
    df_std = df_mapping_wc.copy()
    df_std["Code"] = df_std["Code"].astype(str)
    df_std = df_std[df_std["Code"].str.match(r"^STD\d+$", na=False)].copy()
    df_std["__std_idx"] = df_std["Code"].str.replace("STD", "", regex=False).astype(int)
    df_std = df_std.sort_values("__std_idx")
    std_names = df_std["Name"].head(curve_points + 1).tolist()
    if not std_names or len(std_names) < (curve_points + 1):
        std_names = [f"STD{i}" for i in range(curve_points + 1)]

    qc_names = df_mapping_wc.loc[df_mapping_wc["Code"].astype(str).str.startswith("QC"), "Name"].unique().tolist()

    # 进样体积
    try:
        injection_cfg  = get_object_or_404(InjectionVolumeConfiguration, instrument_num=instrument_num, project_name=project_name)
        injection_vol  = injection_cfg.injection_volume
    except InstrumentConfiguration.DoesNotExist:
        injection_vol  = ""

    # ====== 工具：把“对齐后的单板数据” → 构建（工作清单 + 上机列表）并返回一个 plate payload ======
    # 适用于只有一块板的情况，多块板需要循环反复调用此函数
    def build_one_plate_payload(aligned: dict, layout: str, plate_no_str: str):
        """
        aligned: 由 _process_one_starlet_plate()（Starlet）或 NIMBUS 构造出的：
          Position/Status/OriginBarcode/CutBarcode/SubBarcode/Warm
        layout: 'horizontal' | 'vertical'
        plate_no_str: "X{n}"
        """
        Position       = aligned["Position"]
        Status         = aligned["Status"]
        OriginBarcode  = aligned["OriginBarcode"]
        CutBarcode     = aligned["CutBarcode"]
        SubBarcode     = aligned["SubBarcode"]
        Warm           = aligned["Warm"]

        # 96 补齐（Starlet单块板不到96个孔位，需补齐以保证数据格式与NIMBUS统一）
        rows, cols = 8, 12
        TOTAL = rows * cols
        def pad_to(seq, n, fill=""):
            if len(seq) < n: seq.extend([fill] * (n - len(seq)))
        pad_to(Position, TOTAL, "")
        pad_to(OriginBarcode, TOTAL, "NOTUBE")
        pad_to(CutBarcode, TOTAL, "")
        pad_to(SubBarcode, TOTAL, "")
        pad_to(Warm, TOTAL, "")
        pad_to(Status, TOTAL, "Not used")

        # —— 与岗位清单匹配（按原来的规则）——
        cut_counter = Counter(CutBarcode)
        MatchSampleName, MatchResult = [], []
        DupBarcode, DupBarcodeSampleName = [], []
        for cb in CutBarcode:
            cb_str = str(cb)
            matched_names = barcode_to_names.get(cb_str, [])
            cb_count = cut_counter[cb_str]
            if len(matched_names) == 1:
                MatchResult.append("TRUE")
                MatchSampleName.append(matched_names[0]); DupBarcode.append(""); DupBarcodeSampleName.append("")
            elif len(matched_names) == 0:
                MatchResult.append("FALSE")
                if cb_str != "":
                    MatchSampleName.append(cb_str); DupBarcode.append(""); DupBarcodeSampleName.append("")
                else:
                    MatchSampleName.append("");    DupBarcode.append(""); DupBarcodeSampleName.append("")
            elif len(matched_names) == 2:
                MatchResult.append("TRUE")
                if matched_names[0] == matched_names[1]:
                    DupBarcode.append("Likely"); MatchSampleName.append(matched_names[0]); DupBarcodeSampleName.append("")
                else:
                    DupBarcode.append("TRUE");   MatchSampleName.append(matched_names[0] + "-" + matched_names[1])
                    DupBarcodeSampleName.append("TRUE" if cb_count >= 2 else "")
            else:
                MatchResult.append("TRUE")
                unique_Middlelist = list(dict.fromkeys(matched_names))
                order = {'VF': 1, 'AE': 2, 'VD': 3, 'V': 4, 'VK': 5, 'WV': 6}
                def sort_key(x):
                    for prefix_length in (2, 1):
                        prefix = x[:prefix_length]
                        if prefix in order: return order[prefix]
                    return len(order) + 1
                sorted_lis = sorted(unique_Middlelist, key=sort_key)
                if len(unique_Middlelist) >= 2:
                    MatchSampleName.append('-'.join(sorted_lis)); DupBarcode.append("TRUE")
                    DupBarcodeSampleName.append("TRUE" if cb_count >= 2 else "")
                else:
                    MatchSampleName.append(matched_names[0]); DupBarcode.append(""); DupBarcodeSampleName.append("")

        # 二 构建 96 孔工作清单网格（保持原渲染结构）——
        letters = list("ABCDEFGH"); nums = [str(i) for i in range(1, 13)]
        # 定位孔集合（NIMBUS: Warm 含 X；Starlet: _process_one_starlet_plate 已把 Xn 放在 OriginBarcode 且 Warm 为空）
        locator_positions = set()
        if platform == "NIMBUS":
            for idx, w in enumerate(Warm):
                if isinstance(w, str) and "X" in w:
                    locator_positions.add(Position[idx])

        barcode_to_well = {}   # OriginBarcode -> (well_str, well_number)
        worksheet_grid  = [[None for _ in nums] for _ in letters]
        error_rows      = []

        def _well_number_rowwise(row_idx: int, col: int) -> int:
            return row_idx * 12 + col

        def build_well(row_letter, col_num, row_idx, col_idx, data_idx, well_index):
            well_pos_str = f"{row_letter}{col_num}"
            origin_barcode = str(OriginBarcode[data_idx])

            if origin_barcode not in ("", "nan"):
                barcode_to_well[origin_barcode] = (well_pos_str, well_index)

            value = str(MatchSampleName[data_idx])
            match_sample = value if MatchResult[data_idx] == "TRUE" else ("" if value == "" else barcode_to_name.get(value, "No match"))

            is_locator    = (well_pos_str in locator_positions) or (str(OriginBarcode[data_idx]).upper().startswith("X"))
            locator_label = Warm[data_idx] if is_locator else ""
            if is_locator and (not locator_label):
                locator_label = plate_no_str

            well = {
                "letter": row_letter, "num": col_num, "well_str": well_pos_str, "index": well_index,
                "locator": is_locator, "locator_warm": locator_label,
                "match_sample": match_sample,
                "cut_barcode": CutBarcode[data_idx], "sub_barcode": SubBarcode[data_idx],
                "origin_barcode": OriginBarcode[data_idx],
                "warm": Warm[data_idx], "status": Status[data_idx],
                "dup_barcode": DupBarcode[data_idx], "dup_barcode_sample": DupBarcodeSampleName[data_idx],
            }

            status_text = str(Status[data_idx]).strip().lower()
            is_pipetting_error = ("pipetting error" in status_text)
            if (str(Warm[data_idx]) in ["1", "4", "16384"]) or is_pipetting_error or (match_sample == "No match"):
                error_rows.append({
                    "sample_name": match_sample,
                    "origin_barcode": OriginBarcode[data_idx],
                    "plate_no": plate_no_str,
                    "well_str": well_pos_str,
                    "warn_level": Warm[data_idx] if Warm[data_idx] != "" else ("PIPETTING_ERROR" if is_pipetting_error else ""),
                    "warn_info": Status[data_idx],
                })

            # 落库（以“同日-同项目-同板号-孔位”为粒度）
            SampleRecord.objects.update_or_create(
                project_name=project_name,
                record_date=date.today(),
                plate_no=plate_no_str,
                well_str=well_pos_str,
                defaults={
                    "sample_name": match_sample,
                    "barcode": origin_barcode,
                }
            )
            return well

        # 清理同日同项目同板号旧记录（避免重复）
        SampleRecord.objects.filter(project_name=project_name, record_date=date.today(), plate_no=plate_no_str).delete()

        if layout == 'vertical':   # NIMBUS（列优先）
            for col_idx, col_num in enumerate(nums):
                for row_idx, row_letter in enumerate(letters):
                    data_idx   = col_idx * 8 + row_idx
                    well_index = _well_number_rowwise(row_idx, int(col_num))
                    worksheet_grid[row_idx][col_idx] = build_well(row_letter, col_num, row_idx, col_idx, data_idx, well_index)

        else:                      # Starlet（行优先）
            for row_idx, row_letter in enumerate(letters):
                for col_idx, col_num in enumerate(nums):
                    data_idx   = row_idx * 12 + col_idx
                    well_index = _well_number_rowwise(row_idx, int(col_num))
                    worksheet_grid[row_idx][col_idx] = build_well(row_letter, col_num, row_idx, col_idx, data_idx, well_index)

        worksheet_table = [[worksheet_grid[r][c] for c in range(12)] for r in range(8)]

        # —— 生成上机列表 —— #
        # ClinicalSample：OriginBarcode 中不在映射表“Barcode”里的条码（若 Warm 含 X 则以 Xn 记名）
        mapping_barcodes = set(str(x) for x in df_mapping_wc["Barcode"].tolist())
        ClinicalSample = []
        for i, ob in enumerate(OriginBarcode):
            ob_str   = str(ob)
            warm_val = str(Warm[i]).strip().upper()
            if ob_str not in mapping_barcodes:
                if 'X' in warm_val: ClinicalSample.append(warm_val)  # 定位孔
                else:               ClinicalSample.append(ob_str)

        test_list   = ["DB1"] + [f"Test{i}" for i in range(test_count)]
        curve_list  = ["DB2"] + std_names
        qc_list1    = ["DB3"] + qc_names + ["DB4"]
        qc_list2    = qc_names + ["DB5"]
        SampleName_list = test_list + curve_list + qc_list1 + ClinicalSample + qc_list2
        SampleName_list = [name for name in SampleName_list if isinstance(name, str) and name.count('-') <= 3]

        # worklist 空表
        worklist_table = pd.DataFrame(columns=df_template.columns)
        worklist_table[worklist_table.columns[0]] = SampleName_list

        # value 队列：Name -> [barcodes...]
        name_to_barcodes = defaultdict(deque)
        for barcode, name in barcode_to_name.items():
            name_to_barcodes[name].append(barcode)

        first_col = worklist_table.columns[0]

        # 记录哪些列需要镜像第一列（由映射表的 * 标注）
        mirror_cols = set()

        for _, row in df_worklistmap.iterrows():
            sample_key = row.iloc[0]
            fill_vals  = row.iloc[1:]

            # ===== 新增：标记本行是否为“默认 * 行” =====
            is_default_star_row = (str(sample_key).strip() == "*")

            def fill_cols(mask):
                for col, val in zip(worklist_table.columns[1:], fill_vals.values):
                    # ------- 新增：当“当前行是 * 行 且 该列映射值也为 *” → 镜像第一列 -------
                    # ★ 标注“需要镜像”的列（映射表该列写了 "*"）
                    if str(val).strip() == "*":
                        mirror_cols.add(col)
                        continue
                    # -------------------------------------------------------------------

                    if col in ("SmplInjVol", "Injection volume"):
                        continue
                    if col in ("VialPos", "Vial position", "样品瓶"):
                        ROWS = list("ABCDEFGH")
                        def _resolve_vialpos(sample_name_value):
                            s = str(sample_name_value).strip().upper()
                            m = re.fullmatch(r"X(\d+)", s)
                            if m:
                                k0 = int(m.group(1)) - 1
                                if platform == "NIMBUS":
                                    coln     = 3 + (k0 // 8)
                                    row_idx  = k0 % 8
                                else:  # Starlet
                                    row_idx  = 1 + (k0 // 12)
                                    coln     = 1 + (k0 % 12)
                                if not (0 <= row_idx < 8 and 1 <= coln <= 12):
                                    return None
                                well_pos = f"{ROWS[row_idx]}{coln}"
                                well_no  = _well_number_rowwise(row_idx, coln)
                                if instrument_name == "Thermo" or instrument_name == "Agilent":
                                    if val == "{{Well_Number}}":
                                        return f"{injection_plate}:{well_no}" if injection_plate else well_no
                                    else:
                                        return f"{injection_plate}-{well_pos}" if injection_plate else well_pos
                                else:
                                    if val == "{{Well_Number}}":
                                        return well_no
                                    else:
                                        return ell_pos

                            if val in ["{{Well_Number}}", "{{Well_Position}}"]:
                                # 1) QC/STD：通过 name->barcode 队列取条码，再查位置信息
                                if sample_name_value in name_to_barcodes and name_to_barcodes[sample_name_value]:
                                    barcode = name_to_barcodes[sample_name_value].popleft()
                                    if barcode in barcode_to_well:
                                        pos, no = barcode_to_well[barcode]
                                        if instrument_name == "Thermo" or instrument_name == "Agilent":
                                            if val == "{{Well_Number}}":
                                                return f"{injection_plate}:{well_no}" if injection_plate else well_no
                                            else:
                                                return f"{injection_plate}-{well_pos}" if injection_plate else well_pos
                                        else:
                                            if val == "{{Well_Number}}":
                                                return f"{well_no}"
                                            else:
                                                return f"{well_pos}"

                                # 2) 临床样本：第一列就是条码
                                elif sample_name_value in barcode_to_well:
                                    pos, no = barcode_to_well[sample_name_value]
                                    if instrument_name == "Thermo" or instrument_name == "Agilent":
                                        if val == "{{Well_Number}}":
                                            return f"{injection_plate}:{well_no}" if injection_plate else well_no
                                        else:
                                            return f"{injection_plate}-{well_pos}" if injection_plate else well_pos
                                    else:
                                        if val == "{{Well_Number}}":
                                            return well_no
                                        else:
                                            return ell_pos
                                return None
                            return val

                        worklist_table.loc[mask, col] = worklist_table.loc[mask, first_col].apply(_resolve_vialpos)
                        # # 数字列尽量转 Int64（仅当全部为数字时）
                        # non_null = worklist_table[col].dropna()
                        # if len(non_null) and non_null.astype(str).str.strip().str.fullmatch(r"\d+").all():
                        #     worklist_table[col] = pd.to_numeric(worklist_table[col], errors="coerce").astype("Int64")
                    else:
                        worklist_table.loc[mask, col] = val

            if str(sample_key).startswith("DB"):
                mask = worklist_table.iloc[:, 0].str.startswith("DB")
                fill_cols(mask)
            elif str(sample_key).startswith("Test"):
                mask = worklist_table.iloc[:, 0].str.startswith("Test")
                fill_cols(mask)
            elif "STD" in str(sample_key):
                mask = worklist_table.iloc[:, 0] == str(sample_key)
                fill_cols(mask)
            elif str(sample_key) == "*":
                mask = worklist_table.iloc[:, 1].isna()
                fill_cols(mask)

        today_str  = timezone.localtime().strftime("%Y%m%d")
        year       = today_str[:4]
        yearmonth  = today_str[:6]
        setname    = f"{instrument_num}_{project_name}_{today_str}_{plate_no_str}_GZ"
        output_val = f"{year}\\{yearmonth}\\Data{setname}"
        if "SetName" in worklist_table.columns:  worklist_table["SetName"]  = setname
        if "OutputFile" in worklist_table.columns: worklist_table["OutputFile"] = output_val

        # Thermo 专用：若第一列存在完全相同的多行值，则改成 原值_1、原值_2、…（仅对重复值生效）——
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
            worklist_table.loc[mask, first_col_name] = s[mask] + "_" + order[mask].astype(str)

        # ★ 统一“镜像列”赋值（整列镜像，不再区分哪些行）
        if mirror_cols:
            first_col_name = worklist_table.columns[0]
            for col in mirror_cols:
                worklist_table[col] = worklist_table[first_col_name]

        # 进样体积
        if "SmplInjVol" in worklist_table.columns:
            worklist_table["SmplInjVol"] = injection_vol
        
        # 进样盘
        if "PlatePos" in worklist_table.columns:
            worklist_table["PlatePos"] = injection_plate
        
        ic(worklist_table)

        worklist_records = worklist_table.to_dict(orient="records")

        header_meta = {
            "test_date": timezone.localtime().strftime("%Y-%m-%d"),
            "plate_no": plate_no_str,
            "instrument_num": instrument_num,
            "injection_plate": injection_plate,
            "today_str": today_str,
        }

        return {
            "plate_no": plate_no_str,
            "worksheet_table": worksheet_table,
            "error_rows": error_rows,
            "txt_headers": txt_headers,
            "worklist_records": worklist_records,
            "header": header_meta,
        }

    # ========== 3. 分板 & 逐板处理 ==========
    plates_payload = []

    if platform == "NIMBUS":
        # —— NIMBUS：仍按“单板”处理 —— #
        # 直接从整表读取出 arrays（行顺序不调，后续在 build_one_plate_payload 里补齐&渲染）
        Position, Status, OriginBarcode, CutBarcode, SubBarcode, Warm = [], [], [], [], [], []
        for i in range(1, nrows):
            pos  = scan_sheet.row_values(i)[POS_IDX]
            stat = scan_sheet.row_values(i)[STAT_IDX]
            bc   = scan_sheet.row_values(i)[BC_IDX]
            w    = scan_sheet.row_values(i)[WARM_IDX] if WARM_IDX is not None else ""
            Position.append(pos); Status.append(stat); OriginBarcode.append(bc); Warm.append(w)
            parts = str(bc).split("-", 1)
            CutBarcode.append(parts[0]); SubBarcode.append("-" + parts[1] if len(parts) == 2 else "")

        # 从 Warm 取 Xn
        plate_no_int = 1
        for w in Warm:
            s = str(w)
            if s.upper().startswith("X"):
                m = re.search(r"X(\d+)", s.upper())
                if m:
                    plate_no_int = int(m.group(1))
                    break
        plate_no_str = f"X{plate_no_int}"

        aligned = {
            "Position": Position, "Status": Status, "OriginBarcode": OriginBarcode,
            "CutBarcode": CutBarcode, "SubBarcode": SubBarcode, "Warm": Warm
        }
        plates_payload.append(build_one_plate_payload(aligned, layout='vertical', plate_no_str=plate_no_str))

    else:
        # —— Starlet：支持单板/多板 —— #
        plate_groups = _starlet_split_plates(scan_sheet, scan_index)  # [(n, [rows...]), ...]
        for plate_no_int, row_indexes in plate_groups:
            aligned = _process_one_starlet_plate(scan_sheet, scan_index, row_indexes, plate_no_int)
            plates_payload.append(build_one_plate_payload(aligned, layout='horizontal', plate_no_str=aligned["plate_no_str"]))

    # ========== 4. 保存 Session 与渲染 ==========
    request.session["export_payload"] = {
        "project_name": project_name,
        "platform": platform,
        "injection_plate": injection_plate,
        "plates": plates_payload,                 # ⭐ 多板/单板统一
    }
    request.session.modified = True

    return render(request, "dashboard/ProcessResult.html", {
        "project_name": project_name,
        "platform": platform,
        "plates": plates_payload,                 # 模板循环多张卡片
    })


def preview_export(request):
    all_payload = request.session.get("export_payload")
    if not all_payload:
        return HttpResponseBadRequest("没有可预览的数据，请先生成结果页面。")

    # 兼容：多板/单板
    if "plates" in all_payload:
        try:
            idx = int(request.GET.get("plate", "0"))
        except ValueError:
            idx = 0
        plates = all_payload["plates"]
        if idx < 0 or idx >= len(plates):
            return HttpResponseBadRequest("板索引无效。")
        payload = plates[idx]   # ⭐ 这里面才有 worksheet_table / error_rows / txt_headers / worklist_records
    else:
        payload = all_payload   # 旧结构，仍包含 worksheet_table 等顶层字段

    nums = [str(i) for i in range(1, 13)]
    project = str(all_payload.get("project_name", "PROJECT"))
    platform = str(all_payload.get("platform", "NewPlatform"))
    return render(
        request,
        "dashboard/export_pdf.html",
        {
            "preview": True,
            "nums": nums,
            "project": project,
            "platform": platform,
            **payload,   # 必须把 worksheet_table / error_rows / txt_headers / worklist_records / header 带进去
        },
    )


# 导出pdf和excel
def export_files(request):
    """使用WeasyPrint生成PDF（兼容单板/多板）"""
    all_payload = request.session.get("export_payload")
    if not all_payload:
        return HttpResponseBadRequest("没有可导出的数据，请先生成结果页面。")

    # === 兼容两种会话结构：单板 vs 多板 ===
    # 单板（旧结构）：直接就是一份 payload，包含 worksheet_table 等键
    # 多板（新结构）：export_payload 里有 plates: [ {worksheet_table,..., header{plate_no}}, ... ]
    if "plates" in all_payload and isinstance(all_payload["plates"], list):
        # 读取前端传入的板索引（默认 0）
        try:
            plate_idx = int(request.GET.get("plate", "0"))
        except ValueError:
            plate_idx = 0
        if plate_idx < 0 or plate_idx >= len(all_payload["plates"]):
            return HttpResponseBadRequest("板索引无效。")
        payload = all_payload["plates"][plate_idx]
        # 这些顶层字段依然从总的 all_payload 里取（沿用旧有逻辑）
        payload["project_name"] = all_payload.get("project_name")
        payload["platform"] = all_payload.get("platform")
    else:
        payload = all_payload  # 旧结构：单板

    # 1) 目录设置
    today_str = datetime.today().strftime("%Y-%m-%d")
    project = str(payload.get("project_name", "PROJECT"))
    platform = str(payload.get("platform", "NewPlatform"))
    base_dir = settings.DOWNLOAD_ROOT

    if platform == 'NIMBUS':
        target_dir = os.path.join(base_dir, platform, today_str, project)
    else:
        target_dir = os.path.join(base_dir, platform,'工作清单和上机列表',today_str, project)

    os.makedirs(target_dir, exist_ok=True)

    # 2) 字体路径设置
    font_path = os.path.join(settings.BASE_DIR, 'dashboard', 'static', 'css', 'fonts', 'NotoSansSC-Regular.ttf')

    # 验证字体文件存在
    if not os.path.exists(font_path):
        return JsonResponse({"ok": False, "message": f"字体文件不存在: {font_path}"})

    # 3) 准备模板数据
    nums = [str(i) for i in range(1, 13)]

    # 4) 渲染HTML
    pdf_html = render_to_string(
        "dashboard/export_pdf.html",
        {
            "worksheet_table": payload["worksheet_table"],
            "error_rows": payload["error_rows"],
            "project": payload["project_name"],
            "nums": nums,
            "preview": False,  # PDF模式，不使用浏览器字体加载
            "header": payload.get("header", {}), 
        },
    )

    # 5) 创建字体配置 - WeasyPrint需要
    font_config = FontConfiguration()

    # 6) 定义PDF专用CSS，包含字体配置
    pdf_css = CSS(string=f"""
        @font-face {{
            font-family: "NotoSans";
            src: url("{font_path}") format("truetype");
            font-weight: normal;
            font-style: normal;
        }}
        
        @page {{
            size: A4 landscape;
            margin: 6mm 6mm 8mm 6mm;
        }}
        
        body, table, td, th, div, span {{
            font-family: "NotoSans", "DejaVu Sans", sans-serif;
            font-size: 9pt;
            line-height: 1.25;
        }}
    """, font_config=font_config)

    # 7) 生成PDF
    header = payload.get("header") or {}
    plate_no = header.get("plate_no", "") 
    plate_suffix = f"_{plate_no}" if str(plate_no) else ""

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pdf_fname = f"WorkSheet_{timestamp}{plate_suffix}.pdf"
    pdf_path = os.path.join(target_dir, pdf_fname)
    
    try:
        # 使用WeasyPrint生成PDF
        HTML(string=pdf_html).write_pdf(
            pdf_path,
            stylesheets=[pdf_css],
            font_config=font_config
        )
    except Exception as e:
        return JsonResponse({"ok": False, "message": f"PDF生成失败: {str(e)}"})

    # 8) 生成Excel
    xlsx_path = os.path.join(target_dir, f"上机列表_{timestamp}.xlsx")
    
    # 组装 DataFrame
    headers = payload["txt_headers"]
    records = payload["worklist_records"]  # list[dict]
    df = pd.DataFrame(records, columns=headers)

    # 从会话 payload 里取仪器编号，再查仪器厂家
    instrument_num = (payload.get("header") or {}).get("instrument_num")  # 你在 payload['header'] 里放过它
    instrument_name = ""
    if instrument_num:
        cfg = InstrumentConfiguration.objects.filter(instrument_num=instrument_num).first()
        if cfg:
            instrument_name = (cfg.instrument_name or "").strip()

    worklist_url_key = None  # 返回 JSON 用
    worklist_url_val = None

    if instrument_name.lower() == "sciex":
        # Sciex：导出制表符分隔的 .txt
        txt_fname = f"OnboardingList_{timestamp}{plate_suffix}.txt"
        txt_path = os.path.join(target_dir, txt_fname)
        df.to_csv(txt_path, sep="\t", index=False, encoding="utf-8")
        worklist_url_key = "txt_url"
        worklist_url_val = f"{settings.DOWNLOAD_URL}{today_str}/{project}/{txt_fname}"

    else:
        # 其它厂家：维持原有 .xlsx
        xlsx_fname = f"OnboardingList_{timestamp}{plate_suffix}.xlsx"
        xlsx_path = os.path.join(target_dir, xlsx_fname)
        with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
            df.to_excel(writer, sheet_name="Worklist", index=False)
        worklist_url_key = "xlsx_url"
        worklist_url_val = f"{settings.DOWNLOAD_URL}{today_str}/{project}/{xlsx_fname}"

    # 返回结果：保留 pdf_url，并根据厂家返回 txt_url 或 xlsx_url
    resp = {
        "ok": True,
        "message": "导出完成",
        "pdf_url": f"{settings.DOWNLOAD_URL}{today_str}/{project}/工作清单_{timestamp}{plate_suffix}.pdf",
    }
    resp[worklist_url_key] = worklist_url_val
    return JsonResponse(resp)



# 历史标本查找
def sample_search_api(request):
    query = request.GET.get("q", "").strip()
    if not query:
        return JsonResponse({"results": []})

    records = SampleRecord.objects.filter(
        models.Q(sample_name__icontains=query) | models.Q(barcode__icontains=query)
    ).order_by("-record_date")

    results = [
        {
            "project_name": r.project_name,
            "date": r.record_date.strftime("%Y-%m-%d"),
            "plate_no": r.plate_no,
            "well_str": r.well_str,
        }
        for r in records
    ]

    return JsonResponse({"results": results})


