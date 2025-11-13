from django.db import models
import os
from django.utils.text import slugify
from datetime import date

# Create your models here.
def upload_to_project_folder(instance, filename):
    # 对 project_name 进行 slugify 处理，避免中文或特殊字符问题
    project_folder = slugify(instance.project_name)
    # filename 就是上传时用户原始文件名
    return os.path.join('dashboard/project_config_files', project_folder, filename)

def upload_to_instrument_folder(instance, filename):
    instrument_folder = slugify(instance.instrument_name)
    # filename 就是上传时用户原始文件名
    return os.path.join('dashboard/instrument_config_files', instrument_folder, filename)

class SamplingConfiguration(models.Model):
    SAMPLING_CHOICES = [
        ('manual', '手工取样'),
        ('auto', '自动化取样'),
    ]
    LAYOUT_CHOICES = [
        ('horizontal', '横向'),
        ('vertical', '纵向'),
    ]
    BOOLEAN_CHOICES = [
        ('yes', '是'),
        ('no', '否'),
    ]
    SYSTERM_NUM_CHOICES = [
        ('S0', 'S0'),
        ('S1', 'S1'),
        ('S2', 'S2'),
        ('S3', 'S3'),
        ('S4', 'S4'),
    ]

    project_name = models.CharField(max_length=200)
    project_name_full = models.CharField(max_length=255, default='', blank=True)  # ★ 新增字段:项目全称
    sampling_method = models.CharField(max_length=10, choices=SAMPLING_CHOICES)

    # 公共字段
    curve_points = models.IntegerField()
    qc_groups = models.IntegerField()
    qc_levels = models.IntegerField()
    qc_insert = models.CharField(max_length=5, choices=BOOLEAN_CHOICES)
    test_count = models.IntegerField()

    layout = models.CharField(max_length=10, choices=LAYOUT_CHOICES, blank=True, null=True)

    default_upload_instrument = models.CharField()
    systerm_num = models.CharField(max_length=10, choices=SYSTERM_NUM_CHOICES, default='', blank=True)  # ★ 新增字段:系统号

    mapping_file = models.FileField(upload_to=upload_to_project_folder, blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.project_name} ({self.sampling_method})"
    
class InstrumentConfiguration(models.Model):
    SYSTERM_NUM_CHOICES = [
        ('S0', 'S0'),
        ('S1', 'S1'),
        ('S2', 'S2'),
        ('S3', 'S3'),
        ('S4', 'S4'),
    ]

    instrument_name = models.CharField(max_length=200)
    instrument_num = models.CharField(max_length=200)
    systerm_num = models.CharField(max_length=10, choices=SYSTERM_NUM_CHOICES, default='', blank=True)  # ★ 新增字段:系统号
    upload_file = models.FileField(upload_to=upload_to_instrument_folder, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

class InjectionVolumeConfiguration(models.Model):
    SYSTERM_NUM_CHOICES = [
        ('S0', 'S0'),
        ('S1', 'S1'),
        ('S2', 'S2'),
        ('S3', 'S3'),
        ('S4', 'S4'),
    ]
    project_name = models.CharField(max_length=200)
    instrument_num = models.CharField(max_length=200)
    systerm_num = models.CharField(max_length=10, choices=SYSTERM_NUM_CHOICES, default='', blank=True)  # ★ 新增字段:系统号
    injection_volume = models.IntegerField()
    created_at = models.DateTimeField(auto_now_add=True)

class InjectionPlateConfiguration(models.Model):
    SYSTERM_NUM_CHOICES = [
        ('S0', 'S0'),
        ('S1', 'S1'),
        ('S2', 'S2'),
        ('S3', 'S3'),
        ('S4', 'S4'),
    ]
    project_name = models.CharField(max_length=200)
    instrument_num = models.CharField(max_length=200)
    systerm_num = models.CharField(max_length=10, choices=SYSTERM_NUM_CHOICES, default='', blank=True)  # ★ 新增字段:系统号
    injection_plate = models.JSONField(default=list, help_text="保存为字符串列表，例如 ['Plate1','Plate2','Plate3'] ")
    created_at = models.DateTimeField(auto_now_add=True)

# 记录每一板样本信息，用于标本查找
class SampleRecord(models.Model):
    project_name = models.CharField(max_length=100)   # 项目名称
    plate_no = models.CharField(max_length=20)        # 板号
    well_str = models.CharField(max_length=10)        # 孔位 (如 A1)
    sample_name = models.CharField(max_length=100, blank=True, null=True)  # 实验号
    barcode = models.CharField(max_length=100, blank=True, null=True)      # 条码
    error_info = models.CharField(max_length=100, default='', blank=True)  # ★ 新增字段:报错信息（1，4，16384）
    record_date = models.DateField(default=date.today)  # 生成日期（自动）

    def __str__(self):
        return f"{self.project_name} | {self.sample_name or self.barcode} | {self.plate_no}-{self.well_str}"
