
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
        today_str = datetime.now().strftime("%Y%m%d")

        file_name = f"qyzl-{today_str}-plate_{AreaStartNum}_{AreaStartNum+AreaNum-1}.xls"
        response = HttpResponse(output_stream.getvalue(), content_type='application/vnd.ms-excel')
        response['Content-Disposition'] = f'attachment; filename={file_name}'
        return response

    return render(request, 'dashboard/sampling/Starlet_qyzl.html')


def Starlet_worksheet(request):
    return render(request, 'dashboard/sampling/Starlet_worksheet.html')

# Tecan
def Tecan_sampling(request):
    return render(request, 'dashboard/sampling/Tecan.html')

# 2 标本查找
def sample_search(request):
    return render(request, 'dashboard/sample_search/index.html')

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
    展示 downloads 目录下的真实目录结构：
    downloads/
    └── 平台名/
      └── YYYY-MM-DD/
          └── 项目名/
              ├── 工作清单.pdf
              └── 上机列表.xlsx
    """
    root = settings.DOWNLOAD_ROOT
    ic(root)

    groups = []   # [{group:"NIMBUS", days:[{date:"2025-09-20", projects:[{name, files:[{name,url,is_pdf}]}]}]}]

    if not os.path.exists(root):
        os.makedirs(root)

    for platform in sorted(os.listdir(root)):     # 平台层
        p_path = os.path.join(root, platform)
        if not os.path.isdir(p_path):
            continue

        days = []
        for date_name in sorted(os.listdir(p_path), reverse=True):  # 日期倒序
            if not (DATE_RE.match(date_name) and os.path.isdir(os.path.join(p_path, date_name))):
                continue
            d_path = os.path.join(p_path, date_name)

            projects = []
            for proj in sorted(os.listdir(d_path)):        # 项目层
                proj_path = os.path.join(d_path, proj)
                if not os.path.isdir(proj_path):
                    continue

                files = []
                for fname in sorted(os.listdir(proj_path)):
                    fpath = os.path.join(proj_path, fname)
                    if not os.path.isfile(fpath):
                        continue
                    files.append({
                        "name": fname,
                        "url": reverse("download_export", args=[platform, date_name, proj, fname]),
                        "is_pdf": fname.lower().endswith(".pdf"),
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
        # 前端以 JSON 字符串传入 injection_plate_json，例如 ["X1","X2"]
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


# 结果处理，用户在前端功能入口处选择项目，上传文件并点击提交按钮后的处理逻辑
def ProcessResult(request):
    # 获取项目类型和取样平台 layout
    project_id = request.POST.get("project_id")
    platform = request.POST.get('platform')
    
    # 获取上传的文件对象
    Stationlist = request.FILES.get('station_list')  # 每日操作清单
    Scanresult = request.FILES.get('scan_result')  # 扫码结果

    # 文件读取
    Stationtable = xlrd.open_workbook(filename=None, file_contents=Stationlist.read())  # 每日工作清单
    Scantable = xlrd.open_workbook(filename=None, file_contents=Scanresult.read())  # 扫码结果

    ###################### 处理扫码结果，处理索引，提取关键几个字段信息 ###################### 

    # 抓取Scantable中孔位、标本状态、条码和Warm四列
    scan_data = Scantable.sheets()[0]
    nrows = scan_data.nrows
    ncols = scan_data.ncols

    # 读取首行作为表头，一次性查找列索引
    header = [str(scan_data.row_values(0)[i]).strip() for i in range(ncols)]
    index_map = {col: idx for idx, col in enumerate(header)}

    Positionindex = index_map.get("TPositionId", 0)
    Statusindex = index_map.get("TSumStateDescription", 0)
    Barcodeindex = index_map.get("SPositionBC", 0)

    # ✅ 兼容：Starlet 的扫码结果没有 Warm 列，新增 TLabwareId 取板号
    Warmindex    = index_map.get("Warm")             # None 表示没有 Warm 列（Starlet）
    Labwareindex = index_map.get("TLabwareId")       # Starlet 会有

    # 批量读取数据
    Position = []
    Status = []
    OriginBarcode = []  # 原始条码
    CutBarcode = []     # 以“-”切割后的主条码
    SubBarcode = []     # 以“-”切割后的子条码
    Warm = []
    labware_ids   = []   # Starlet: 记录 TLabwareId

    # 两种来源的定位孔集合（NIMBUS: Warm；Starlet: TLabwareId 映射）
    locator_positions = set()

    for i in range(1, nrows):
        pos = scan_data.row_values(i)[Positionindex]
        status = scan_data.row_values(i)[Statusindex]
        barcode = scan_data.row_values(i)[Barcodeindex]

        # Warm 兼容：Starlet 场景下为空串
        warm = ""
        if Warmindex is not None and Warmindex < ncols:
            warm = scan_data.row_values(i)[Warmindex] if Warmindex is not None else ""

        # Starlet 用于定位和板号
        labware_id = ""
        if Labwareindex is not None and Labwareindex < ncols:
            labware_id = str(scan_data.row_values(i)[Labwareindex]).strip()
        labware_ids.append(labware_id)

        Position.append(pos)
        Status.append(status)
        OriginBarcode.append(barcode)

        # 切割主/子条码，保证即使barcode为非字符串也能处理
        barcode_str = str(barcode)
        parts = barcode_str.split("-", 1)
        CutBarcode.append(parts[0])
        if len(parts) == 2:
            SubBarcode.append("-" + parts[1])
        else:
            SubBarcode.append("")

        Warm.append(warm)

        # 添加定位孔标识
        # ======= NIMBUS：Warm 含 'X' 的孔即定位孔（保持原逻辑） =======
        if Warmindex is not None:
            if isinstance(warm, str) and ("X" in warm):
                locator_positions.add(pos)


    if platform == 'Starlet':
        # 预期 96 孔的顺序（Starlet 固定横排：A1..A12, B1..B12, ...）
        letters_fix = ["A","B","C","D","E","F","G","H"]
        nums_fix    = [str(i) for i in range(1, 13)]
        expected_positions = [f"{r}{c}" for r in letters_fix for c in nums_fix]

        # 先把『读到的』行按 TPositionId 做成字典，便于精准落位
        row_by_pos = {}
        for i in range(1, nrows):
            pos = str(scan_data.row_values(i)[Positionindex]).strip()
            row_by_pos[pos] = {
                "Status":        scan_data.row_values(i)[Statusindex],
                "OriginBarcode": scan_data.row_values(i)[Barcodeindex],
                "Warm":          ""  # Starlet 无 Warm
            }

        # 把上面你已经 append 完成的数组，替换成『按 expected_positions 对齐后的新数组』
        Position_aligned, Status_aligned, OriginBarcode_aligned = [], [], []
        CutBarcode_aligned, SubBarcode_aligned, Warm_aligned     = [], [], []

        for pos in expected_positions:
            if pos in row_by_pos:
                status  = row_by_pos[pos]["Status"]
                bc      = row_by_pos[pos]["OriginBarcode"]
                warm    = row_by_pos[pos]["Warm"]
            else:
                # 关键：缺行用占位，不前移
                status, bc, warm = "Not used", "NOTUBE", ""

            Position_aligned.append(pos)
            Status_aligned.append(status)
            Warm_aligned.append(warm)

            bc_str = str(bc)
            parts  = bc_str.split("-", 1)
            OriginBarcode_aligned.append(bc_str)
            CutBarcode_aligned.append(parts[0])
            SubBarcode_aligned.append("-" + parts[1] if len(parts) == 2 else "")

        # 用对齐后的数组覆盖原数组
        Position       = Position_aligned
        Status         = Status_aligned
        OriginBarcode  = OriginBarcode_aligned
        CutBarcode     = CutBarcode_aligned
        SubBarcode     = SubBarcode_aligned
        Warm           = Warm_aligned

    # ======= 板号：NIMBUS 优先 Warm；否则 Starlet 用 TLabwareId 末尾数字 =======
    import re

    plate_no = None
    if Warmindex is not None:
        for w in Warm:
            w_str = str(w)
            if w_str.startswith("X"):
                m = re.search(r"X(\d+)", w_str)
                plate_no = int(m.group(1)) if m else 1
                break

    if plate_no is None:
        # Starlet：从 TLabwareId 末尾数字得 n → plate_no = "X{n}"
        labware_ids = []
        if Labwareindex is not None:
            for i in range(1, nrows):
                labware_ids.append(str(scan_data.row_values(i)[Labwareindex]).strip())
        first_non_empty = next((s for s in labware_ids if s), "")
        m = re.search(r"(\d+)$", first_non_empty)
        plate_no = f"X{int(m.group(1))}" if m else "X1"

    # ======= Starlet 的定位孔：按 TLabwareId 末尾数字 n -> B/C/.../H + 列号 =======
    if Warmindex is None:
        # 取用于定位映射的 n（TLabwareId 末尾数字）
        first_non_empty = next((s for s in labware_ids if s), "")
        m = re.search(r"(\d+)$", first_non_empty)
        if m:
            n = int(m.group(1))  # 1-based
            # 将 n 映射到 96 孔板从 B 行开始的 12 列栅格（B..H 共 7 行可覆盖到 84）
            # 若 n > 84，则按 84 回绕（可根据业务需要改为报错或其他规则）
            base = max(1, n)
            i0 = (base - 1) % 84           # 0..83
            row_block = i0 // 12           # 0..6 -> B..H
            col = (i0 % 12) + 1            # 1..12
            row_letters = ["B", "C", "D", "E", "F", "G", "H"]
            row = row_letters[row_block]
            locator_positions = {f"{row}{col}"}  # 覆盖为 Starlet 的单一定位孔
        else:
            # 无法从 TLabwareId 提取数字时，不设定位孔（维持空集合）
            locator_positions = set()

    ###################### 抓取每日工作清单的主条码与实验号，并与扫码结果匹配 ###################### 

    # 抓取Stationtable中的主条码、实验号
    station_data = Stationtable.sheets()[0]
    nrows = station_data.nrows
    ncols = station_data.ncols

    # 一次性获取表头索引，容错顺序
    header = [str(station_data.row_values(0)[i]).strip() for i in range(ncols)]
    index_map = {col: idx for idx, col in enumerate(header)}
    MainBarcodeindex = index_map.get("主条码", 0)
    SampleNameindex = index_map.get("实验号", 0)

    # 主条码与实验号列表批量读取
    MainBarcode = [station_data.row_values(i)[MainBarcodeindex] for i in range(1, nrows)]
    SampleName = [station_data.row_values(i)[SampleNameindex] for i in range(1, nrows)]

    # 用字典加速查找，支持一对多
    from collections import defaultdict, Counter

    barcode_to_names = defaultdict(list)
    for bc, sn in zip(MainBarcode, SampleName):
        barcode_to_names[str(bc)].append(str(sn))

    cutbarcode_counter = Counter(CutBarcode)  # 统计每个cutbarcode出现次数（用于后续标记）

    MatchSampleName = [] # 匹配到的样本名称
    MatchResult = [] # 匹配结果，用于判断是否要匹配曲线质控

    DupBarcode = [] # 条码在岗位清单中出现多次，且实验号可能不同
    DupBarcodeSampleName = [] # 条码在移液/岗位清单出现多次，且实验号也不同

    for cb in CutBarcode:
        cb_str = str(cb)
        matched_names = barcode_to_names.get(cb_str, [])
        cb_count = cutbarcode_counter[cb_str]

        if len(matched_names) == 1:
            MatchResult.append("TRUE")
            MatchSampleName.append(matched_names[0])
            DupBarcode.append("")
            DupBarcodeSampleName.append("")
        elif len(matched_names) == 0:
            MatchResult.append("FALSE")
            if cb_str != "":
                MatchSampleName.append(cb_str)
                DupBarcode.append("")
                DupBarcodeSampleName.append("")
            else:
                MatchSampleName.append("")
                DupBarcode.append("")
                DupBarcodeSampleName.append("")
        elif len(matched_names) == 2:
            MatchResult.append("TRUE")
            if matched_names[0] == matched_names[1]:
                DupBarcode.append("Likely")
                MatchSampleName.append(matched_names[0])
                DupBarcodeSampleName.append("")
            else:
                DupBarcode.append("TRUE")
                MatchSampleName.append(matched_names[0] + "-" + matched_names[1])
                DupBarcodeSampleName.append("TRUE" if cb_count >= 2 else "")      
        else:
            MatchResult.append("TRUE")
            # 三个及以上
            # 只保留唯一实验号并排序
            unique_Middlelist = list(dict.fromkeys(matched_names))
            order = {'VF': 1, 'AE': 2, 'VD': 3, 'V': 4, 'VK': 5, 'WV': 6}
            def sort_key(x):
                for prefix_length in (2, 1):
                    prefix = x[:prefix_length]
                    if prefix in order:
                        return order[prefix]
                return len(order) + 1

            sorted_lis = sorted(unique_Middlelist, key=sort_key)
            if len(unique_Middlelist) >= 2:
                MatchSampleName.append('-'.join(sorted_lis))
                DupBarcode.append("TRUE")
                DupBarcodeSampleName.append("TRUE" if cb_count >= 2 else "")
            else:
                MatchSampleName.append(matched_names[0])
                DupBarcode.append("")
                DupBarcodeSampleName.append("")


    ###################### 1 生成工作清单和报错表格 ######################
    
    rows, cols = 8, 12
    TOTAL = rows * cols  # 96

    def pad_to(seq, n, fill=""):
        if len(seq) < n:
            seq.extend([fill] * (n - len(seq)))

    # 这些数组都补到 96for i in range(1, nrows):
    pad_to(Position,    TOTAL, "")
    pad_to(OriginBarcode, TOTAL, "NOTUBE")
    pad_to(CutBarcode,    TOTAL, "")
    pad_to(SubBarcode,    TOTAL, "")
    pad_to(Warm,          TOTAL, "")
    pad_to(Status,        TOTAL, "Not used")
    pad_to(MatchSampleName, TOTAL, "")
    pad_to(MatchResult,     TOTAL, "")
    pad_to(DupBarcode,       TOTAL, "")
    pad_to(DupBarcodeSampleName, TOTAL, "")

    letters = ["A","B","C","D","E","F","G","H"]
    nums = [str(i) for i in range(1, 13)]
    
    # 抓取96孔板的排列顺序（纵向/横向）
    config = SamplingConfiguration.objects.get(id=project_id)
    project_name = config.project_name
    layout = config.layout

    # 这里强制覆盖 layout，不再使用后台表配置
    if platform == 'Starlet':
        layout = 'horizontal'   # Starlet 固定横向
    else:
        layout = 'vertical'     # NIMBUS 固定纵向

    mapping_file_path = config.mapping_file.path

    # 匹配曲线质控
    df_mapping = pd.read_excel(mapping_file_path, sheet_name="工作清单")

    # 建立字典方便查找
    barcode_to_name = dict(zip(df_mapping["Barcode"].astype(str), df_mapping["Name"]))

    # 组装渲染数据
    well_list = []

    error_rows = []   # 存放错误信息行

    # 建立映射字典： OriginBarcode -> (Well_Position, Well_Number)，方便后续生成上机列表
    barcode_to_well = {}
    index_counter = 1  # 从1开始编号

    # 清理旧数据：限定 project_name + record_date + plate_no
    SampleRecord.objects.filter(
        project_name=project_name,
        record_date=date.today(),
        plate_no=plate_no
    ).delete()

    # 显示网格：8 行 (A~H) × 12 列 (1~12)，按坐标写入，避免被遍历顺序影响
    worksheet_grid = [[None for _ in nums] for _ in letters]

    def build_well_dict(row_letter, col_num, row_idx, col_idx, data_idx, well_index):
        well_pos_str = f"{row_letter}{col_num}"
        origin_barcode = str(OriginBarcode[data_idx])

        # 建立条码 -> (孔位, 序号) 的映射（供后续 VialPos / Well_Number 使用）
        if origin_barcode not in ("", "nan"):
            barcode_to_well[origin_barcode] = (well_pos_str, well_index)

        # 样本名匹配
        value = str(MatchSampleName[data_idx])
        if MatchResult[data_idx] == "TRUE":
            match_sample = value
        else:
            match_sample = "" if value == "" else barcode_to_name.get(value, "No match")

        is_locator = well_pos_str in locator_positions

        locator_label = Warm[data_idx] if is_locator else ""

        if is_locator and (not locator_label):   # Starlet 没 Warm
            locator_label = plate_no             # 例如 "X7"

        well = {
            "letter": row_letter,
            "num": col_num,
            "well_str": well_pos_str,
            "index": well_index,                      # 显示用序号（随布局：列优先/行优先）

            "locator": is_locator,
            "locator_warm": locator_label, 

            "match_sample": match_sample,
            "cut_barcode": CutBarcode[data_idx],
            "sub_barcode": SubBarcode[data_idx],
            "origin_barcode": OriginBarcode[data_idx],
            
            "warm": Warm[data_idx],
            "status": Status[data_idx],
            "dup_barcode": DupBarcode[data_idx],
            "dup_barcode_sample": DupBarcodeSampleName[data_idx],
        }

        # 报错信息表

        # Starlet的添加逻辑
        status_text = str(Status[data_idx]).strip().lower()
        is_pipetting_error = ("pipetting error" in status_text)

        if (str(Warm[data_idx]) in ["1", "4", "16384"]) \
            or is_pipetting_error \
            or (match_sample == "No match"):
                error_rows.append({
                    "sample_name": match_sample,
                    "origin_barcode": OriginBarcode[data_idx],
                    "plate_no": plate_no,
                    "well_str": well_pos_str,
                    "warn_level": Warm[data_idx] if Warm[data_idx] != "" else ("PIPETTING_ERROR" if is_pipetting_error else ""),
                    "warn_info": Status[data_idx],
                })

        # 写数据库（与遍历顺序无关）
        SampleRecord.objects.create(
            project_name=project_name,
            plate_no=plate_no,
            well_str=well_pos_str,
            sample_name=match_sample,
            barcode=origin_barcode,
        )

        return well


    if layout == 'vertical':
        # 列优先：A1, B1, ... H1 → A2, B2, ... → ... → H12
        for col_idx, col_num in enumerate(nums):
            for row_idx, row_letter in enumerate(letters):
                data_idx   = col_idx * rows + row_idx           # 列优先取数索引（已有逻辑）
                well_index = row_idx * cols + col_idx + 1       # well_index不随LAYOUT_CHOICES的选择而变化
                well = build_well_dict(row_letter, col_num, row_idx, col_idx, data_idx, well_index)
                # 关键：按“行(row_idx)、列(col_idx)”坐标放进网格
                worksheet_grid[row_idx][col_idx] = well

    else:
        # 行优先：A1~A12 → B1~B12 → ... → H1~H12（与原来一致）
        for row_idx, row_letter in enumerate(letters):
            for col_idx, col_num in enumerate(nums):
                data_idx   = row_idx * cols + col_idx           # 行优先取数索引（已有逻辑）
                well_index = row_idx * cols + col_idx + 1       # well_index不随LAYOUT_CHOICES的选择而变化
                well = build_well_dict(row_letter, col_num, row_idx, col_idx, data_idx, well_index)
                worksheet_grid[row_idx][col_idx] = well

    # 最终用于模板的二维表：严格按 A~H 为行，1~12 为列的顺序输出
    worksheet_table = [[worksheet_grid[r][c] for c in range(cols)] for r in range(rows)]
    ic(worksheet_table)

    ###################### 2 生成上机列表 ######################

    # 获取仪器编号对应的上机模板

    # 获取上机仪器
    instrument_num = request.POST.get("instrument_num")

    # 获取上机仪器对应的上机模板,并转化为pandas数据框
    instrument_config = get_object_or_404(InstrumentConfiguration, instrument_num=instrument_num)

    if not instrument_config.upload_file:
        return HttpResponse("未设置该上机仪器对应的上机模板,请设置后再试", status=404)
    
    # 解析并读取上机模板
    # 只支持 .txt / .csv
    ext = os.path.splitext(instrument_config.upload_file.name)[1].lower()
    if ext not in (".txt", ".csv"):
        return HttpResponse(f"仅支持 .txt 或 .csv 模板，当前为：{ext}", status=400)

    # 读取二进制，再做“编码回退”解码
    with instrument_config.upload_file.open("rb") as f:
        raw = f.read()

    text = None
    last_err = None
    for enc in ("utf-8", "utf-8-sig", "gb18030"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError as e:
            last_err = e

    if text is None:
        # 所有候选编码都失败
        raise last_err or UnicodeDecodeError("unknown", b"", 0, 1, "unable to decode upload_file")
    
    # 用 pandas 自动嗅探分隔符（支持逗号/制表符等）
    # 注意：必须指定 engine="python" 才能启用 Sniffer；dtype=str 保持后续逻辑不变
    df = pd.read_csv(StringIO(text), sep=None, engine="python", dtype=str)

    # 提取表头
    txt_headers = df.columns.tolist()

    # 获取SampleName列的内容
    # 第一步：获取OriginBarcode中的内容，并在此基础上去除df_mapping["Barcode"]中的内容(为曲线和质控)
    barcode_list = [str(x) for x in df_mapping["Barcode"].tolist()]

    ClinicalSample = []

    for i, ob in enumerate(OriginBarcode):
        if ob not in barcode_list:  # 只处理未匹配条码
            warm_val = str(Warm[i]).strip()  # 转成字符串，去掉空格
            # 如果 Warm 值含 'X'（定位孔标记），则替换
            if 'X' in warm_val.upper():
                ClinicalSample.append(warm_val)
            else:
                ClinicalSample.append(ob)


    # 第二步：添加DB1和Test(获取用户后台设置的Test个数)
    test_count = config.test_count
    test_list = ["DB1"] + [f"Test{i}" for i in range(test_count)]

    # 第三步：添加曲线(获取用户后台设置的STD个数)
    curve_points = config.curve_points

    # 从“工作清单”表中提取 Code=STD* 对应的 Name，按 STD 序号排序后取前 curve_points+1 个
    df_std = df_mapping.copy()
    df_std["Code"] = df_std["Code"].astype(str)

    # 仅保留形如 STD0/STD1/... 的行，并提取数字序号用于排序
    df_std = df_std[df_std["Code"].str.match(r"^STD\d+$", na=False)].copy()
    df_std["__std_idx"] = df_std["Code"].str.replace("STD", "", regex=False).astype(int)
    df_std = df_std.sort_values("__std_idx")

    # 取 Name 列作为曲线名称列表
    std_names = df_std["Name"].head(curve_points + 1).tolist()

    # 兜底：如果映射里缺失，退回旧逻辑，避免列表为空导致后续失败
    if not std_names or len(std_names) < (curve_points + 1):
        std_names = [f"STD{i}" for i in range(curve_points + 1)]

    curve_list = ["DB2"] + std_names

    # 第四步：添加QC(获取对应关系表模板中设置的QC名称)
    qc_list1 = ["DB3"] + df_mapping.loc[df_mapping["Code"].str.startswith("QC"), "Name"].unique().tolist() + ["DB4"]

    # 添加封尾质控和DB
    qc_list2 = df_mapping.loc[df_mapping["Code"].str.startswith("QC"), "Name"].unique().tolist() + ["DB5"]

    # 拼接上述列表
    SampleName_list = test_list + curve_list + qc_list1 + ClinicalSample + qc_list2

    ic(SampleName_list)

    worklist_mapping = pd.read_excel(mapping_file_path, sheet_name="上机列表")
    
    # 生成上机列表
    # 获取后台设置的进样体积
    try:
        injectionvolume_config = get_object_or_404(InjectionVolumeConfiguration, instrument_num=instrument_num, project_name=project_name)
        injection_volume = injectionvolume_config.injection_volume
    except InstrumentConfiguration.DoesNotExist:
        injection_volume = ""

    # 1. 构建 value -> queue of barcodes 的映射
    name_to_barcodes = defaultdict(deque)
    for barcode, name in barcode_to_name.items():
        name_to_barcodes[name].append(barcode)

    # 1. 创建一个空表，列名与 df 相同
    worklist_table = pd.DataFrame(columns=df.columns)

    # 2. 填充第一列
    worklist_table[worklist_mapping.columns[0]] = SampleName_list

    first_col = worklist_table.columns[0]  # 动态获取第一列列名

    # 3. 遍历 worklist_mapping 按规则填充

    # 填充规则：
    # 1 若worklist_mapping第一列的元素为DB1，则将worklist_table中对应列元素为DB1的行的第2至最后一列的内容按照worklist_mapping中内容进行填充
    # 2 若worklist_mapping第一列的元素为Test，则将worklist_table中对应列元素为Test开头（注意这里）的行的第2至最后一列的内容按照worklist_mapping中内容进行填充
    # 3 若worklist_mapping第一列的元素为*，则将worklist_table中除上述已填充的行以外（注意这里）的行的第2至最后一列的内容按照worklist_mapping中内容进行填充

    # 20251011新增规则：
    # 1 无论worklist_mapping中第一列的元素的什么，若worklist_table中某一列的列名为‘SetName’，则该列的内容按照下述形式填充：'仪器编号-项目名称-日期'，即{instrument_num}-{project_name}-日期，其中日期精确到天（如20251011）。例如：FXS-YZ38-25OHD-20251011
    # 2 无论worklist_mapping中第一列的元素的什么，若worklist_table中某一列的列名为‘OutputFile’，则该列的内容按照下述形式填充：'年\年月\Data{instrument_num}-{project_name}-日期'，其中日期精确到天（如20251011）。例如：2025\202510\DataFXS-YZ38-25OHD-20251011

    # 20251019新增规则：
    # 1 若worklist_mapping第一列的元素中包含‘STD’，则将worklist_table中对应列元素与worklist_mapping第一列的元素内容完全相同的行的第2至最后一列的内容按照worklist_mapping中内容进行填充

    for _, row in worklist_mapping.iterrows():
        sample_name = row.iloc[0]   # 用 iloc 显式取第一列
        fill_values = row.iloc[1:]  # 用 iloc 显式取剩余列

        # --- 规则 1: 进样体积单独处理 ---
        if "SmplInjVol" in worklist_table.columns:
            worklist_table["SmplInjVol"] = injection_volume

        # --- 规则 2: 按 sample_name 分类处理 --- 
        if str(sample_name).startswith("DB"):  # 规则1
            mask = worklist_table.iloc[:, 0].str.startswith("DB")
            for col, val in zip(worklist_table.columns[1:], fill_values.values):
                if col == "SmplInjVol":  
                    continue  # 已经在上面统一处理
                worklist_table.loc[mask, col] = val

        elif str(sample_name).startswith("Test"): # 规则2
            mask = worklist_table.iloc[:, 0].str.startswith("Test")
            for col, val in zip(worklist_table.columns[1:], fill_values.values):
                if col == "SmplInjVol":  
                    continue
                worklist_table.loc[mask, col] = val

        # —— 新增规则：STD 精确匹配 —— 
        elif "STD" in str(sample_name):
            # 只填充与 worklist_mapping 第一列中该 STD 名称“完全相同”的行
            mask = worklist_table.iloc[:, 0] == str(sample_name)
            for col, val in zip(worklist_table.columns[1:], fill_values.values):
                # 与其它规则保持一致：进样体积列在上面已统一处理
                if col == "SmplInjVol" or col == "Injection volume":
                    continue
                worklist_table.loc[mask, col] = val

        elif sample_name == "*":  # 规则3
            # 找到还没填充的行（即第2列为空的行）
            mask = worklist_table.iloc[:, 1].isna()
            for col, val in zip(worklist_table.columns[1:], fill_values.values):
                if col == "SmplInjVol" or col == "Injection volume":  
                    continue
                if col in ("VialPos", "Vial position", "样品瓶"):  
                    # 特殊处理 VialPos
                    ROWS = ["A", "B", "C", "D", "E", "F", "G", "H"]

                    def _well_number_rowwise(row_idx: int, col: int) -> int:
                        """
                        计算孔号：按行(A..H)横向编号（A1=1, A2=2, ..., A12=12, B1=13, ...）
                        row_idx: 0..7 -> A..H
                        col: 1..12
                        """
                        return row_idx * 12 + col

                    def resolve_vialpos(sample_name_value):
                           
                        # Case 2: 定位孔 "----------"
                        s = str(sample_name_value).strip().upper()

                        m = re.fullmatch(r"X(\d+)", s)
                        if m:
                            k = int(m.group(1))           # 第 k 个定位点（从 1 开始）
                            k0 = k - 1

                            if platform == "NIMBUS":
                                # 列从 3 开始；每列 8 个（A..H），纵向走完 8 个后换到下一列
                                col = 3 + (k0 // 8)               # 3,4,5,...
                                row_idx = k0 % 8                  # 0..7 -> A..H
                                row_letter = ROWS[row_idx]
                                well_pos = f"{row_letter}{col}"   # 例如 A3, B3, ...
                                well_num = _well_number_rowwise(row_idx, col)  # 例如 A3=3, B3=15, ...
                                if val == "{{Well_Number}}":
                                    return int(well_num)
                                elif val == "{{Well_Position}}":
                                    return well_pos
                                else:
                                    return val

                            elif platform == "Starlet":
                                # 行从 B(=index 1) 开始；每行 12 个（1..12），横向走完 12 个后换到下一行
                                row_idx = 1 + (k0 // 12)          # 1(B),2(C),...,最多到 7(H)
                                col = 1 + (k0 % 12)               # 1..12
                                if not (0 <= row_idx < len(ROWS)) or not (1 <= col <= 12):
                                    return None  # 超出 96 孔范围则不返回（可按需处理）
                                row_letter = ROWS[row_idx]
                                well_pos = f"{row_letter}{col}"   # 例如 B1,B2,...,B12, C1, ...
                                well_num = _well_number_rowwise(row_idx, col)  # 例如 B1=13, B2=14, ...
                                if val == "{{Well_Number}}":
                                    return int(well_num)
                                elif val == "{{Well_Position}}":
                                    return well_pos
                                else:
                                    return val

                        # case 3: QC/STD/曲线（worklist_table里存 Name，需要映射到 barcode）
                        if val in ["{{Well_Number}}", "{{Well_Position}}"]:
                            if sample_name_value in name_to_barcodes and name_to_barcodes[sample_name_value]:
                                # 取出当前 sample_name 对应的第一个条码，并从队列中移除，保证顺序消耗
                                barcode = name_to_barcodes[sample_name_value].popleft()

                                # 再用 barcode 去查 barcode_to_well
                                if barcode in barcode_to_well:
                                    well_pos, well_num = barcode_to_well[barcode]
                                    return int(well_num) if val == "{{Well_Number}}" else well_pos
                                else:
                                    return None
                                
                            # case 4: 临床样品（worklist_table里就是条码本身）
                            elif sample_name_value in barcode_to_well:
                                well_pos, well_num = barcode_to_well[sample_name_value]
                                return int(well_num) if val == "{{Well_Number}}" else well_pos 

                            else:
                                return None
                        else:
                            return val

                    worklist_table.loc[mask, col] = worklist_table.loc[mask, first_col].apply(resolve_vialpos)

                    # 仅当该列（去除空值后）全部为纯数字时，才转换为可空整型 Int64
                    col_vals = worklist_table[col]

                    # 去掉空值后，检查是否全部匹配纯数字（允许前后空格）
                    non_null = col_vals.dropna()
                    all_numeric = non_null.astype(str).str.strip().str.fullmatch(r"\d+").all()

                    if all_numeric and len(non_null) > 0:
                        # 确保可安全转型
                        worklist_table[col] = pd.to_numeric(worklist_table[col], errors="coerce").astype("Int64")

                else:
                    # 普通列，直接填充
                    worklist_table.loc[mask, col] = val

    # 日期精确到天，如 20251011
    today_str = timezone.localtime().strftime("%Y%m%d")
    year = today_str[:4]       # 2025
    yearmonth = today_str[:6]  # 202510
    
    # e.g. FXS-YZ38-25OHD-20251011
    setname_value = f"{instrument_num}-{project_name}-{today_str}-{plate_no}"

    # e.g. 2025\202510\DataFXS-YZ38-25OHD-20251011
    output_value = f"{year}\\{yearmonth}\\Data{setname_value}"
    
    if "SetName" in worklist_table.columns:
        worklist_table["SetName"] = setname_value

    if "OutputFile" in worklist_table.columns:
        worklist_table["OutputFile"] = output_value

    # 转成 records 形式（每行是一个字典）
    worklist_records = worklist_table.to_dict(orient="records")

    request.session["export_payload"] = {
        "project_name": project_name,           # 如 VD / CSA
        "platform": platform, 
        "worksheet_table": worksheet_table,     # 你生成的 96 孔板展示数据（列表套字典）
        "error_rows": error_rows,               # 报错信息
        "txt_headers": txt_headers,             # 上机列表表头
        "worklist_records": worklist_records,   # 上机列表数据（DataFrame -> to_dict('records') 的结果）

        # ✅ 新增：导出 PDF 页眉需要的元信息（按你 ProcessResult.html 的第二行设计）
        "header": {
            "test_date": timezone.localtime().strftime("%Y-%m-%d"),  # 检测日期
            "plate_no": plate_no,                # 例如 "X2"（你已从 Scanresult Warm 提取）
            "instrument_num": instrument_num,    # 例如 FXS-YZ___（你已有 POST 获取）
            "tray_no": request.POST.get("tray_no", ""),  # 上机盘号（如果表单没这个字段就给空）
        },
    }
    request.session.modified = True

    return render(request, 'dashboard/ProcessResult.html',locals())


def preview_export(request):
    """
    用于浏览器预览 export_pdf.html（不生成 PDF）
    """
    payload = request.session.get("export_payload")
    if not payload:
        return HttpResponseBadRequest("没有可预览的数据，请先生成结果页面。")

    nums = [str(i) for i in range(1, 13)]
    # 传 preview=True，让模板走浏览器用的字体加载方式
    return render(request, "dashboard/export_pdf.html", {
        "worksheet_table": payload["worksheet_table"],
        "error_rows": payload["error_rows"],
        "nums": nums,
        "project": payload["project_name"],
        "preview": True,
    })

# 导出pdf和excel
def export_files(request):
    """使用WeasyPrint生成PDF"""
    payload = request.session.get("export_payload")
    if not payload:
        return HttpResponseBadRequest("没有可导出的数据，请先生成结果页面。")

    # 1) 目录设置
    today_str = datetime.today().strftime("%Y-%m-%d")
    project = str(payload.get("project_name", "PROJECT"))
    platform = str(payload.get("platform", "NewPlatform"))
    base_dir = settings.DOWNLOAD_ROOT
    target_dir = os.path.join(base_dir, platform, today_str, project)
    os.makedirs(target_dir, exist_ok=True)

    # 2) 字体路径设置
    font_path = os.path.join(settings.BASE_DIR, 'dashboard', 'static', 'css', 'fonts', 'NotoSansSC-Regular.ttf')
    ic(font_path)

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
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pdf_path = os.path.join(target_dir, f"WorkSheet_{timestamp}.pdf")
    
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
        txt_fname = f"OnboardingList_{timestamp}.txt"
        txt_path = os.path.join(target_dir, txt_fname)
        df.to_csv(txt_path, sep="\t", index=False, encoding="utf-8")
        worklist_url_key = "txt_url"
        worklist_url_val = f"{settings.DOWNLOAD_URL}{today_str}/{project}/{txt_fname}"

    else:
        # 其它厂家：维持原有 .xlsx
        xlsx_fname = f"OnboardingList_{timestamp}.xlsx"
        xlsx_path = os.path.join(target_dir, xlsx_fname)
        with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
            df.to_excel(writer, sheet_name="Worklist", index=False)
        worklist_url_key = "xlsx_url"
        worklist_url_val = f"{settings.DOWNLOAD_URL}{today_str}/{project}/{xlsx_fname}"

    # 返回结果：保留 pdf_url，并根据厂家返回 txt_url 或 xlsx_url
    resp = {
        "ok": True,
        "message": "导出完成",
        "pdf_url": f"{settings.DOWNLOAD_URL}{today_str}/{project}/工作清单_{timestamp}.pdf",
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


