from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponse, HttpResponseBadRequest
from django.shortcuts import render, redirect
from django.conf import settings
from django.utils import timezone
from django.template.loader import render_to_string
from django.views.decorators.http import require_POST
from collections import defaultdict, Counter
from datetime import date, timedelta
from weasyprint import HTML, CSS
from weasyprint.text.fonts import FontConfiguration
from icecream import ic
import pandas as pd
import xlrd
import xlwt
import os
import re
import json

from .models import SamplingConfiguration, InstrumentConfiguration, SampleRecord


# ========== ★ 新增：报错关键词列表 ==========
ERROR_KEYWORDS = [
    "无料",
    "有料",
    "取料NG",
    "扫码NG",
    "开盖NG",
    "吸液NG",
    "分液NG",
    "吸液曲线NG",
    "分液曲线NG",
    "吸/分液曲线NG",
    "待回收",
]

# ========== 第一步：处理上传数据，返回中间展示页面 ==========
def WholeBloodWorkstationResult(request):
    """
    全血工作站取样结果处理函数（第一步）
    从取样总表中读取 ScannerCode、Row、Column，生成96孔板工作清单预览
    """
    if request.method != 'POST':
        return HttpResponseBadRequest("仅支持 POST 请求")
    
    # ========== 1. 获取表单参数 ==========
    project_id = request.POST.get("project_id")
    project_name = request.POST.get("project_name")
    platform = "WholeBloodWorkstation"
    instrument_num = request.POST.get("instrument_num")
    systerm_num = request.POST.get("systerm_num")
    testing_day = request.POST.get("testing_day", "today")
    
    # 获取上传的两个文件
    station_list = request.FILES.get('station_list')      # 岗位清单表
    sampling_summary = request.FILES.get('sampling_summary')  # 取样总表
    
    if not (station_list and sampling_summary and project_id and instrument_num):
        return HttpResponseBadRequest("缺少必要参数或文件")
    
    # 获取项目配置信息
    try:
        config = SamplingConfiguration.objects.get(pk=project_id)
        project_name_full = config.project_name_full
    except:
        project_name_full = project_name
    
    # 确定日期
    if testing_day == "today":
        today_str = timezone.localtime().strftime("%Y%m%d")
        record_date = date.today()
    else:
        today_str = (timezone.localtime() + timedelta(days=1)).strftime("%Y%m%d")
        record_date = date.today() + timedelta(days=1)
    
    # ========== 2. 读取岗位清单表（获取条码→实验号映射）==========
    station_wb = xlrd.open_workbook(filename=None, file_contents=station_list.read())
    station_sheet = station_wb.sheets()[0]
    station_nrows = station_sheet.nrows
    station_ncols = station_sheet.ncols
    
    # 解析表头
    station_header = [str(station_sheet.row_values(0)[i]).strip() for i in range(station_ncols)]
    station_index = {col: idx for idx, col in enumerate(station_header)}
    
    # 获取主条码和实验号列索引
    mb_idx = station_index.get("主条码", 0)
    sn_idx = station_index.get("实验号", 0)
    
    # ========== 构建主条码→实验号列表的映射（拆分条码） ==========
    barcode_to_names = defaultdict(list)
    for i in range(1, station_nrows):
        barcode = str(station_sheet.row_values(i)[mb_idx]).strip()
        sample_name = str(station_sheet.row_values(i)[sn_idx]).strip()
        if barcode and sample_name:
            # ★ 新增：拆分条码，只取主条码部分作为键
            parts = barcode.split("-", 1)
            main_barcode = parts[0]  # 主条码部分（如 "2437871821"）
            barcode_to_names[main_barcode].append(sample_name)
    
    
    # ========== 3. 读取取样总表（从"产品信息"工作表）==========
    summary_wb = xlrd.open_workbook(filename=None, file_contents=sampling_summary.read())
    
    # 查找"产品信息"工作表
    product_sheet = None
    for sheet in summary_wb.sheets():
        if "产品信息" in sheet.name:
            product_sheet = sheet
            break
    
    if product_sheet is None:
        return render(request, "dashboard/error.html", {
            "message": "取样总表中未找到'产品信息'工作表,请检查文件格式"
        })
    
    # 解析产品信息表头
    product_nrows = product_sheet.nrows
    product_ncols = product_sheet.ncols
    product_header = [str(product_sheet.row_values(0)[i]).strip() for i in range(product_ncols)]
    product_index = {col: idx for idx, col in enumerate(product_header)}
    
    # 获取关键列索引
    scanner_code_idx = product_index.get("ScannerCode")
    row_idx = product_index.get("Row")
    column_idx = product_index.get("Column")

    process_no_str_idx = product_index.get("ProcessNoStr")
    
    if scanner_code_idx is None or row_idx is None or column_idx is None:
        return render(request, "dashboard/error.html", {
            "message": f"取样总表'产品信息'工作表中缺少必要列(ScannerCode/Row/Column)\n当前表头: {' | '.join(product_header)}"
        })
    
    # ========== 4. 构建96孔板数据结构 ==========
    letters = list("ABCDEFGH")
    nums = [str(i) for i in range(1, 13)]

    # 用字典暂存：(row, col) -> barcode
    well_data = {}

    # 报错信息
    well_errors = {}
    
    # 读取产品信息工作表数据
    for i in range(1, product_nrows):
        row_val = product_sheet.row_values(i)[row_idx]
        col_val = product_sheet.row_values(i)[column_idx]
        scanner_code = product_sheet.row_values(i)[scanner_code_idx]

        process_no_str = ""
        if process_no_str_idx is not None:
            process_no_str = str(product_sheet.row_values(i)[process_no_str_idx]).strip()
        
        try:
            row_num = int(float(row_val))
            col_num = int(float(col_val))
        except:
            continue
        
        # 验证范围
        if not (1 <= row_num <= 8 and 1 <= col_num <= 12):
            continue
        
        well_data[(row_num, col_num)] = str(scanner_code).strip()

        # ========== ★ 新增：检测 ProcessNoStr 是否包含报错关键词 ==========
        if process_no_str:
            for keyword in ERROR_KEYWORDS:
                if keyword in process_no_str:
                    well_errors[(row_num, col_num)] = process_no_str
                    break  # 匹配到一个关键词即可，无需继续检测
    
    # ========== 5. 按96孔板顺序填充数据 ==========
    worksheet_grid = [[None for _ in nums] for _ in letters]
    error_rows = []
    
    for row_num in range(1, 9):  # 1-8
        for col_num in range(1, 13):  # 1-12
            row_letter = letters[row_num - 1]
            col_str = nums[col_num - 1]
            well_pos = f"{row_letter}{col_str}"
            well_index = (row_num - 1) * 12 + col_num  # 1-96
            
            # 获取该孔位的条码
            barcode = well_data.get((row_num, col_num), "NOTUBE")
            
            # 先拆分条码，再用主条码匹配实验号
            if barcode and barcode != "NOTUBE":
                parts = barcode.split("-", 1)
                cut_barcode = parts[0]
                sub_barcode = "-" + parts[1] if len(parts) == 2 else ""
            else:
                cut_barcode = "NOTUBE" if barcode == "NOTUBE" else ""
                sub_barcode = ""
            
            # 从岗位清单匹配实验号
            if cut_barcode and cut_barcode != "NOTUBE":
                if cut_barcode in barcode_to_names:
                    matched_names = barcode_to_names[cut_barcode]
                    if len(matched_names) == 1:
                        match_sample = matched_names[0]
                    elif len(matched_names) > 1:
                        # ========== ★ 修改：去重后再拼接 ==========
                        # 使用 list(dict.fromkeys(...)) 保留顺序的同时去重
                        unique_names = list(dict.fromkeys(matched_names))
                        if len(unique_names) == 1:
                            # 去重后只剩一个实验号，直接使用
                            match_sample = unique_names[0]
                    else:
                        match_sample = "No match"
                else:
                    match_sample = "No match"

            # 构建单元格数据
            cell = {
                "letter": row_letter,
                "num": col_str,
                "well_str": well_pos,
                "index": well_index,
                "locator": False,
                "locator_warm": "",
                "match_sample": match_sample,
                "cut_barcode": cut_barcode,
                "sub_barcode": sub_barcode,
                "origin_barcode": barcode,
                "warm": "",
                "status": "Used",
                "dup_barcode": "",
                "dup_barcode_sample": "",
                "flag_suck": "",
                "flag_dispense": "",
            }
            
            worksheet_grid[row_num - 1][col_num - 1] = cell

            # 收集报错信息
            error_message = well_errors.get((row_num, col_num))  # 从取样总表获取的报错信息
            if error_message:
                # ProcessNoStr 包含报错关键词
                error_rows.append({
                    "sample_name": match_sample if match_sample else barcode,
                    "origin_barcode": barcode,
                    "plate_no": "X1",
                    "well_str": well_pos,
                    "warn_info": error_message,  # 使用 ProcessNoStr 的内容
                })
            elif match_sample == "No match" and barcode != "NOTUBE":
                # 实验号匹配失败
                error_rows.append({
                    "sample_name": match_sample,
                    "origin_barcode": barcode,
                    "plate_no": "X1",
                    "well_str": well_pos,
                    "warn_info": "No match",
                })

    
    # ========== 6. 构建 plates 数据结构（用于模板渲染）==========
    worksheet_table = [[worksheet_grid[r][c] for c in range(12)] for r in range(8)]
    
    plates = [{
        "header": {
            "plate_no": "X1",
            "instrument_num": instrument_num,
            "systerm_num": systerm_num,
            "injection_plate": "",  # 全血工作站无上机盘号
        },
        "worksheet_table": worksheet_table,
        "error_rows": error_rows,
        "txt_headers": [],  # 全血工作站无上机列表
        "worklist_records": [],
    }]
    
    # ========== 7. 将数据保存到 session（供导出函数使用）==========
    payload = {
        "project_name": project_name,
        "project_name_full": project_name_full,
        "instrument_num": instrument_num,
        "systerm_num": systerm_num,
        "platform": platform,
        "today_str": today_str,
        "plates": [{
            "plate_no": "X1",
            "worksheet_table": worksheet_table,
            "error_rows": error_rows,
        }],
    }
    
    request.session['wholeblood_payload'] = payload
    request.session.modified = True
    
    # ========== 8. 返回中间展示页面 ==========
    return render(request, "dashboard/ProcessResult_WholeBloodWorkstation.html", {
        "project_name": project_name,
        "project_name_full": project_name_full,
        "instrument_num": instrument_num,
        "systerm_num": systerm_num,
        "today_str": today_str[:4] + "-" + today_str[4:6] + "-" + today_str[6:8],
        "platform": platform,
        "plates": plates,
    })


# ========== 第二步：用户点击"导出本板"时，生成文件并保存到服务器 ==========
@require_POST
def export_wholeblood_files(request):
    """
    导出全血工作站工作清单和报错信息表（第二步）
    """
    # 从 session 中读取之前保存的数据
    payload = request.session.get('wholeblood_payload')
    if not payload:
        return JsonResponse({"ok": False, "message": "数据已过期，请重新上传文件"}, status=400)
    
    project_name = payload['project_name']
    project_name_full = payload['project_name_full']
    instrument_num = payload['instrument_num']
    systerm_num = payload['systerm_num']
    platform = payload['platform']
    today_str = payload['today_str']
    plates = payload['plates']
    
    # 获取要导出的板号（支持多板，但全血工作站目前只有一块板）
    plate_index = int(request.GET.get('plate', 0))
    if plate_index >= len(plates):
        return JsonResponse({"ok": False, "message": "板号不存在"}, status=400)
    
    plate_data = plates[plate_index]
    plate_no = plate_data['plate_no']
    worksheet_table = plate_data['worksheet_table']
    error_rows = plate_data['error_rows']
    
    # ========== 1. 生成时间戳和文件名 ==========
    timestamp = timezone.localtime().strftime("%Y%m%d_%H%M%S")
    worksheet_filename_stem = f"{plate_no}_WorkSheet_{instrument_num}_{systerm_num}_{project_name}_{timestamp}_{plate_no}_GZ"
    
    # ========== 2. 创建保存目录 ==========
    date_folder = today_str[:4] + "-" + today_str[4:6] + "-" + today_str[6:8]
    save_dir = os.path.join(
        settings.DOWNLOAD_ROOT,
        platform,
        date_folder,
        project_name
    )
    os.makedirs(save_dir, exist_ok=True)
    
    # ========== 3. 生成 PDF 工作清单 ==========
    pdf_payload = {
        "project_name": project_name,
        "project_name_full": project_name_full,
        "instrument_num": instrument_num,
        "systerm_num": systerm_num,
        "plate_no": plate_no,
        "worksheet_table": worksheet_table,
        "error_rows": error_rows,
        "platform": platform,
    }
    
    # 渲染 HTML 模板
    worksheet_html = render_to_string("dashboard/export_pdf.html", pdf_payload)
    
    # 配置字体和样式
    font_config = FontConfiguration()
    pdf_css = CSS(string="""
        @page { size: A4; margin: 10mm; }
        body { font-family: "Noto Sans CJK SC", "SimSun", sans-serif; font-size: 10px; }
        .highlight { background-color: #ffcccc; }
    """, font_config=font_config)
    
    # 生成 PDF 文件
    worksheet_pdf_filename = f"{worksheet_filename_stem}.pdf"
    worksheet_pdf_path = os.path.join(save_dir, worksheet_pdf_filename)
    
    HTML(string=worksheet_html).write_pdf(
        worksheet_pdf_path,
        stylesheets=[pdf_css],
        font_config=font_config
    )
    
    # ========== 4. 保存 payload.json（用于后续重新生成）==========
    payload_filename = f"{worksheet_filename_stem}.payload.json"
    payload_path = os.path.join(save_dir, payload_filename)
    
    with open(payload_path, "w", encoding="utf-8") as f:
        json.dump(pdf_payload, f, ensure_ascii=False, indent=2)
    
    # ========== 5. 生成 Excel 报错信息表（如果有报错）==========
    if error_rows:
        error_wb = xlwt.Workbook()
        error_sheet = error_wb.add_sheet("报错信息")
        
        # 表头
        headers = ["实验号", "条码", "板号", "孔位", "警告信息"]
        for col, header in enumerate(headers):
            error_sheet.write(0, col, header)
        
        # 数据行
        for row_idx, err in enumerate(error_rows, start=1):
            error_sheet.write(row_idx, 0, err["sample_name"])
            error_sheet.write(row_idx, 1, err["origin_barcode"])
            error_sheet.write(row_idx, 2, err["plate_no"])
            error_sheet.write(row_idx, 3, err["well_str"])
            error_sheet.write(row_idx, 4, err["warn_level"])
            error_sheet.write(row_idx, 5, err["warn_info"])
        
        error_filename = f"{plate_no}_ErrorList_{instrument_num}_{systerm_num}_{project_name}_{timestamp}_{plate_no}_GZ.xls"
        error_path = os.path.join(save_dir, error_filename)
        error_wb.save(error_path)
    
    # ========== 6. 返回成功响应 ==========
    return JsonResponse({"ok": True, "message": "导出成功"})
