from django.urls import path
from . import views
from . import views_TecanIngest
from django.conf import settings
from django.conf.urls.static import static
from django.contrib.auth.decorators import login_required

urlpatterns = [
    path("", views.home, name="dashboard_home"),

    # 登陆和创建用户
    path('dashboard/login/', views.login_view, name='dashboard_login'),
    path('dashboard/logout/', views.logout_view, name='dashboard_logout'),
    path('dashboard/create_user/', views.create_user, name='create_user'),

    # 1 前端
    path('dashboard/frontend_entry/', views.frontend_entry, name='frontend_entry'),  # 前端入口

    path('dashboard/frontend_entry/Manual_sampling/', views.Manual_sampling, name='Manual_sampling'), # 手工取样

    path('dashboard/frontend_entry/NIMBUS_sampling/', views.NIMBUS_sampling, name='NIMBUS_sampling'), # NIMBUS取样

    path('dashboard/frontend_entry/Starlet_sampling/', views.Starlet_sampling, name='Starlet_sampling'), # Starlet取样
    path('dashboard/frontend_entry/Starlet_qyzl/', views.Starlet_qyzl, name='Starlet_qyzl'),
    path('dashboard/frontend_entry/Starlet_worksheet/', views.Starlet_worksheet, name='Starlet_worksheet'),

    path('dashboard/frontend_entry/Tecan_sampling/', views.Tecan_sampling, name='Tecan_sampling'), # Tecan取样

    # 2 标本查找
    path('dashboard/sample_search/', views.sample_search, name='sample_search'),  # 标本查找
    path("dashboard/sample_search_api/", views.sample_search_api, name="sample_search_api"),

    # ★ 新增：当天统计 API
    path("dashboard/sample_search_stats/", views.sample_search_stats_today, name="sample_search_stats_today"),

    path('dashboard/file_download/', views.file_download, name='file_download'),  # 文件下载
    path('dashboard/file_download_history/', views.file_download_history, name='file_download_history'),  # 历史文件下载
    path('dashboard/file_replace/', login_required(views.file_replace), name='file_replace'), # 文件替换
    path("dashboard/file_replace_sampled_codes/", views.file_replace_sampled_codes, name="file_replace_sampled_codes"),
    
    path(
        "dashboard/downloads/<str:platform>/<str:date_name>/<str:project>/<path:filename>",
        views.download_export,
        name="download_export",
    ),
    path("dashboard/export_files/", views.export_files, name="export_files"),

    path("preview_export/", views.preview_export, name="preview_export"),


    # 3 后台参数配置
    path('dashboard/project_config/', login_required(views.project_config), name='project_config'),  # 项目参数配置

    path('dashboard/project_config_create/', login_required(views.project_config_create), name='project_config_create'),  # 项目参数配置——新建
    path('dashboard/project_config_view/<int:pk>/', login_required(views.project_config_view), name='project_config_view'),  # 项目参数配置——预览
    path('dashboard/project_config_edit/<int:pk>/', login_required(views.project_config_edit), name='project_config_edit'),  # 项目参数配置——编辑
    path('dashboard/project_config_delete/<int:pk>/', login_required(views.project_config_delete), name='project_config_delete'),  # 项目参数配置——删除

    path('dashboard/vendor_config/', login_required(views.vendor_config), name='vendor_config'),  # 仪器厂家参数配置
    path('dashboard/vendor_config_create/', login_required(views.vendor_config_create), name='vendor_config_create'),  # 仪器厂家参数配置——新建
    path('dashboard/vendor_config_delete/<int:pk>/', login_required(views.vendor_config_delete), name='vendor_config_delete'),  # 仪器厂家参数配置——删除

    path('dashboard/injection_volume_config/', login_required(views.injection_volume_config), name='injection_volume_config'),  # 进样体积配置
    path('dashboard/injection_volume_config_create/', login_required(views.injection_volume_config_create), name='injection_volume_config_create'),  # 进样体积配置——新建
    path('dashboard/injection_volume_config_delete/<int:pk>/', login_required(views.injection_volume_config_delete), name='injection_volume_config_delete'),  # 进样体积配置——删除

    path('dashboard/injection_plate_config/', login_required(views.injection_plate_config), name='injection_plate_config'),  # 进样盘号配置
    path('dashboard/injection_plate_config_create/', login_required(views.injection_plate_config_create), name='injection_plate_config_create'),  # 进样盘号配置——新建
    path('dashboard/injection_plate_config_delete/<int:pk>/', login_required(views.injection_plate_config_delete), name='injection_plate_config_delete'),  # 进样盘号配置——删除

    # 获取后台配置的项目信息
    path('dashboard/frontend_entry/get_project_list/', views.get_project_list, name='get_project_list'),
    path('dashboard/frontend_entry/get_project_detail/<int:pk>/', views.get_project_detail, name='get_project_detail'),
    path('dashboard/frontend_entry/get_injection_plates/', views.get_injection_plates, name='get_injection_plates'),
    path('dashboard/frontend_entry/get_systerm_nums/', views.get_systerm_nums, name='get_systerm_nums'),

    # 4 结果处理(适用于NIMBUS和Starlet)，用户在前端功能入口处选择项目，上传文件并点击提交按钮后的处理逻辑
    path('dashboard/ProcessResult/', views.ProcessResult, name='ProcessResult'), # NIMBUS和Starlet

    path('dashboard/ManualProcessResult/', views.Manual_process_result, name='ManualProcessResult'), # NIMBUS和Starlet

    # 5 结果处理(适用于TECAN)
    path('dashboard/TecanIngest/', views_TecanIngest.tecaningest, name='TecanIngest'), # TECAN
    path('dashboard/TecanIngest/resolve/', views_TecanIngest.tecan_resolve_duplicates, name='TecanIngestResolve'),

    path('dashboard/TecanIngest/processed/list/',    views_TecanIngest.tecan_list_processed_files,   name='TecanProcessedList'),
    path('dashboard/TecanIngest/processed/manage/',  views_TecanIngest.tecan_manage_processed_file,  name='TecanProcessedManage'),
    path('dashboard/TecanIngest/processed/download/',views_TecanIngest.tecan_download_processed_file,name='TecanProcessedDownload'),


    path("dashboard/manual/", views.user_manual, name="user_manual"),
]

if settings.DEBUG:  # 只在开发环境生效
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.DOWNLOAD_URL, document_root=settings.DOWNLOAD_ROOT)