from django.urls import path
from . import views
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path("", views.home, name="dashboard_home"),

    # 1 前端
    path('dashboard/frontend_entry/', views.frontend_entry, name='frontend_entry'),  # 前端入口

    path('dashboard/frontend_entry/NIMBUS_sampling/', views.NIMBUS_sampling, name='NIMBUS_sampling'), # NIMBUS取样

    path('dashboard/frontend_entry/Starlet_sampling/', views.Starlet_sampling, name='Starlet_sampling'), # Starlet取样
    path('dashboard/frontend_entry/Starlet_qyzl/', views.Starlet_qyzl, name='Starlet_qyzl'),
    path('dashboard/frontend_entry/Starlet_worksheet/', views.Starlet_worksheet, name='Starlet_worksheet'),

    # 2 标本查找
    path('dashboard/sample_search/', views.sample_search, name='sample_search'),  # 标本查找
    path("dashboard/sample_search_api/", views.sample_search_api, name="sample_search_api"),


    path('dashboard/file_download/', views.file_download, name='file_download'),  # 文件下载
    path("dashboard/export_files/", views.export_files, name="export_files"),

    path("preview_export/", views.preview_export, name="preview_export"),


    # 3 后台参数配置
    path('dashboard/project_config/', views.project_config, name='project_config'),  # 项目参数配置

    path('dashboard/project_config_create/', views.project_config_create, name='project_config_create'),  # 项目参数配置——新建
    path('dashboard/project_config_view/<int:pk>/', views.project_config_view, name='project_config_view'),  # 项目参数配置——预览
    path('dashboard/project_config_edit/<int:pk>/', views.project_config_edit, name='project_config_edit'),  # 项目参数配置——编辑
    path('dashboard/project_config_delete/<int:pk>/', views.project_config_delete, name='project_config_delete'),  # 项目参数配置——删除


    path('dashboard/vendor_config/', views.vendor_config, name='vendor_config'),  # 仪器厂家参数配置
    path('dashboard/vendor_config_create/', views.vendor_config_create, name='vendor_config_create'),  # 仪器厂家参数配置——新建
    path('dashboard/vendor_config_delete/<int:pk>/', views.vendor_config_delete, name='vendor_config_delete'),  # 仪器厂家参数配置——删除


    path('dashboard/injection_volume_config/', views.injection_volume_config, name='injection_volume_config'),  # 进样体积配置
    path('dashboard/injection_volume_config_create/', views.injection_volume_config_create, name='injection_volume_config_create'),  # 进样体积配置——新建
    path('dashboard/injection_volume_config_delete/<int:pk>/', views.injection_volume_config_delete, name='injection_volume_config_delete'),  # 仪器厂家参数配置——删除

    # 获取后台配置的项目信息
    path('dashboard/get_project_list/', views.get_project_list, name='get_project_list'),
    path('dashboard/frontend_entry/NIMBUS_sampling/get_project_detail/<int:pk>/', views.get_project_detail, name='get_project_detail'),

    # 结果处理，用户在前端功能入口处选择项目，上传文件并点击提交按钮后的处理逻辑
    path('dashboard/ProcessResult/', views.ProcessResult, name='ProcessResult'), # NIMBUS



    path("dashboard/manual/", views.user_manual, name="user_manual"),
]

if settings.DEBUG:  # 只在开发环境生效
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.DOWNLOAD_URL, document_root=settings.DOWNLOAD_ROOT)