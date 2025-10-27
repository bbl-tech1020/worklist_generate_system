from django import forms
from .models import SamplingConfiguration

class UploadFileForm(forms.Form):
    CSV = forms.FileField()  # 与 HTML 的 name="CSV" 保持一致
    TissueType = forms.CharField(max_length=100)  # 为 TissueType 字段添加表单验证

class SamplingConfigurationForm(forms.ModelForm):
    class Meta:
        model = SamplingConfiguration
        fields = '__all__'
        widgets = {
            'project_name': forms.TextInput(attrs={'class': 'form-control'}),
            'project_name_full': forms.TextInput(attrs={'class': 'form-control'}),
            'sampling_method': forms.Select(attrs={'class': 'form-select'}),
            'curve_points': forms.Select(choices=[(i, i) for i in range(6, 11)], attrs={'class': 'form-select'}),
            'qc_groups': forms.Select(choices=[(2, 2), (3, 3)], attrs={'class': 'form-select'}),
            'qc_levels': forms.Select(choices=[(2, 2), (3, 3)], attrs={'class': 'form-select'}),
            'qc_insert': forms.Select(choices=[('yes', '是'), ('no', '否')], attrs={'class': 'form-select'}),
            'test_count': forms.Select(choices=[(2, 2), (3, 3), (4, 4)], attrs={'class': 'form-select'}),
            'layout': forms.Select(choices=[('horizontal', '横向'), ('vertical', '纵向')], attrs={'class': 'form-select'}),
            'default_upload_instrument': forms.TextInput(attrs={'class': 'form-control'}),
        }