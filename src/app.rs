use crate::model::{
    AdjacentCompareMode, AggregateFunction, CompareOperator, DatasetRecord, JoinKind, LogicalType,
    PreviewRow, PriorityPlacement, StatisticFillStrategy, TextCaseMode, TimeDiffUnit, page_window,
};
use crate::service::AppService;
use anyhow::Result;
use rfd::FileDialog;
use slint::{ModelRc, SharedString, VecModel};
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

slint::include_modules!();

pub fn run() -> Result<(), slint::PlatformError> {
    let ui = MainWindow::new()?;
    let service = Rc::new(RefCell::new(AppService::new()));

    install_callbacks(&ui, service);
    refresh_ui(&ui, &AppService::new(), "等待导入 xlsx / csv / json 数据集");
    ui.run()
}

fn install_callbacks(ui: &MainWindow, service: Rc<RefCell<AppService>>) {
    let weak = ui.as_weak();
    ui.global::<Logic>().on_import_files({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let paths = FileDialog::new()
                    .add_filter("Data Files", &["xlsx", "csv", "json"])
                    .pick_files()
                    .unwrap_or_default();

                if paths.is_empty() {
                    refresh_ui(ui, service, "已取消导入");
                    return;
                }

                ui.global::<FormState>().set_preview_page(1);
                ui.global::<FormState>().set_field_page(1);
                let status = service
                    .import_paths(&paths)
                    .unwrap_or_else(|error| format!("导入失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_select_dataset({
        let service = service.clone();
        move |dataset_id| {
            with_ui(&weak, &service, |ui, service| {
                ui.global::<FormState>().set_preview_page(1);
                ui.global::<FormState>().set_field_page(1);
                let status = service
                    .select_dataset(dataset_id)
                    .map(|_| format!("已切换到数据集 #{dataset_id}"))
                    .unwrap_or_else(|error| format!("切换失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_delete_selected_dataset({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                ui.global::<FormState>().set_preview_page(1);
                ui.global::<FormState>().set_field_page(1);
                let status = service
                    .delete_selected_dataset()
                    .unwrap_or_else(|error| format!("删除数据集失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_preview_settings({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                ui.global::<FormState>().set_preview_page(1);
                refresh_ui(ui, service, "样表预览参数已更新");
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_preview_prev_page({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                form.set_preview_page((form.get_preview_page() - 1).max(1));
                refresh_ui(ui, service, "样表预览已切换到上一页");
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_preview_next_page({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                form.set_preview_page(form.get_preview_page() + 1);
                refresh_ui(ui, service, "样表预览已切换到下一页");
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_field_settings({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                ui.global::<FormState>().set_field_page(1);
                refresh_ui(ui, service, "字段台账分页参数已更新");
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_field_prev_page({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                form.set_field_page((form.get_field_page() - 1).max(1));
                refresh_ui(ui, service, "字段台账已切换到上一页");
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_field_next_page({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                form.set_field_page(form.get_field_page() + 1);
                refresh_ui(ui, service, "字段台账已切换到下一页");
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_quality_rules({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let status = service
                    .update_quality_rules(
                        &form.get_quality_primary_key().to_string(),
                        &form.get_quality_composite_keys().to_string(),
                        &form.get_quality_time_column().to_string(),
                        &form.get_quality_missing_threshold().to_string(),
                        &form.get_quality_range_column().to_string(),
                        &form.get_quality_range_min().to_string(),
                        &form.get_quality_range_max().to_string(),
                        &form.get_quality_text_column().to_string(),
                        &form.get_quality_max_length().to_string(),
                        &form.get_quality_time_gap_minutes().to_string(),
                    )
                    .unwrap_or_else(|error| format!("质量设置更新失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_undo({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let status = service.undo().unwrap_or_else(|error| format!("撤销失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_redo({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let status = service.redo().unwrap_or_else(|error| format!("重做失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    install_zero_arg_action(ui, service.clone(), "on_refresh_current", |service| {
        service.inspect_selected()
    });
    install_zero_arg_action(ui, service.clone(), "on_normalize_columns", |service| {
        service.normalize_columns()
    });
    install_zero_arg_action(ui, service.clone(), "on_trim_text_values", |service| {
        service.trim_text_values()
    });
    install_zero_arg_action(ui, service.clone(), "on_drop_empty_rows", |service| {
        service.drop_empty_rows()
    });
    install_zero_arg_action(ui, service.clone(), "on_deduplicate_rows", |service| {
        service.deduplicate_rows()
    });
    install_zero_arg_action(ui, service.clone(), "on_apply_recommended_mapping", |service| {
        service.apply_recommended_mapping()
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_row_operation({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let operation = form.get_row_operation().to_string();
                let status = match operation.as_str() {
                    "按关键词保留" => service.filter_rows_contains(
                        &form.get_filter_column().to_string(),
                        &form.get_filter_keyword().to_string(),
                    ),
                    "按关键词删除" => service.drop_rows_not_contains(
                        &form.get_filter_column().to_string(),
                        &form.get_filter_keyword().to_string(),
                    ),
                    "保留行范围" => service.keep_row_range(
                        parse_usize_or_default(&form.get_range_start().to_string(), 1),
                        parse_usize_or_default(&form.get_range_end().to_string(), 1),
                    ),
                    "删除行范围" => service.drop_row_range(
                        parse_usize_or_default(&form.get_range_start().to_string(), 1),
                        parse_usize_or_default(&form.get_range_end().to_string(), 1),
                    ),
                    "保留前N行" => service.keep_top_rows(
                        parse_usize_or_default(&form.get_top_row_count().to_string(), 100),
                    ),
                    "抽样N行" => service.sample_rows(
                        parse_usize_or_default(&form.get_sample_row_count().to_string(), 50),
                    ),
                    "保留缺失记录" => {
                        service.keep_rows_with_missing(split_csv_like(&form.get_missing_columns().to_string()))
                    }
                    "删除缺失记录" => {
                        service.drop_rows_with_missing(split_csv_like(&form.get_missing_columns().to_string()))
                    }
                    "按列去重" => {
                        service.deduplicate_by_columns(split_csv_like(&form.get_row_key_columns().to_string()))
                    }
                    "整表去重" => service.deduplicate_rows(),
                    _ => service.drop_empty_rows(),
                }
                .unwrap_or_else(|error| format!("行处理失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_column_operation({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let operation = form.get_column_operation().to_string();
                let status = match operation.as_str() {
                    "重命名列" => service.rename_column(
                        &form.get_rename_from().to_string(),
                        &form.get_rename_to().to_string(),
                    ),
                    "保留列" => service.keep_columns(split_csv_like(&form.get_column_list().to_string())),
                    "删除列" => service.drop_columns(split_csv_like(&form.get_column_list().to_string())),
                    "删除空列" => service.drop_empty_columns(),
                    "调整列顺序" => {
                        service.reorder_columns(split_csv_like(&form.get_column_order_list().to_string()))
                    }
                    "列名前缀" => {
                        service.add_column_name_affix(&form.get_column_name_prefix().to_string(), "")
                    }
                    "列名后缀" => {
                        service.add_column_name_affix("", &form.get_column_name_suffix().to_string())
                    }
                    "复制列" => service.duplicate_column(
                        &form.get_copy_source_column().to_string(),
                        &form.get_copy_target_column().to_string(),
                    ),
                    "合并列" => service.merge_columns(
                        split_csv_like(&form.get_merge_columns().to_string()),
                        &form.get_merge_target_column().to_string(),
                        &form.get_merge_separator().to_string(),
                    ),
                    "新增序号列" => service.add_row_number_column(
                        &form.get_index_column_name().to_string(),
                        parse_usize_or_default(&form.get_index_start().to_string(), 1),
                    ),
                    "按列排序" => service.sort_by(
                        &form.get_sort_column().to_string(),
                        form.get_sort_ascending(),
                    ),
                    _ => service.normalize_columns(),
                }
                .unwrap_or_else(|error| format!("列处理失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_text_operation({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let operation = form.get_text_operation().to_string();
                let status = match operation.as_str() {
                    "大小写统一" => service.transform_text_case(
                        &form.get_text_column().to_string(),
                        TextCaseMode::from_text(&form.get_text_case().to_string()),
                    ),
                    "文本替换" => service.replace_text(
                        &form.get_text_column().to_string(),
                        &form.get_replace_from().to_string(),
                        &form.get_replace_to().to_string(),
                    ),
                    "压缩空白" => service.squeeze_text_whitespace(&form.get_text_column().to_string()),
                    "移除指定字符" => service.remove_text_pattern(
                        &form.get_text_column().to_string(),
                        &form.get_text_remove_pattern().to_string(),
                    ),
                    "提取分隔符左侧" => service.extract_text_before(
                        &form.get_text_column().to_string(),
                        &form.get_text_delimiter().to_string(),
                    ),
                    "提取分隔符右侧" => service.extract_text_after(
                        &form.get_text_column().to_string(),
                        &form.get_text_delimiter().to_string(),
                    ),
                    "仅保留数字" => service.keep_digits_only(&form.get_text_column().to_string()),
                    "添加前后缀" => service.add_text_affix(
                        &form.get_text_column().to_string(),
                        &form.get_text_prefix().to_string(),
                        &form.get_text_suffix().to_string(),
                    ),
                    "文本截断" => service.truncate_text(
                        &form.get_text_column().to_string(),
                        parse_usize_or_default(&form.get_text_truncate_length().to_string(), 32),
                    ),
                    _ => service.trim_text_values(),
                }
                .unwrap_or_else(|error| format!("文本处理失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_value_operation({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let operation = form.get_value_operation().to_string();
                let status = match operation.as_str() {
                    "前值填充" => service.fill_null_forward(&form.get_fill_column().to_string()),
                    "后值填充" => service.fill_null_backward(&form.get_fill_column().to_string()),
                    "统计值填充" => service.fill_null_statistic(
                        &form.get_stat_fill_column().to_string(),
                        StatisticFillStrategy::from_text(&form.get_stat_fill_strategy().to_string()),
                    ),
                    "空字符串转空值" => service.empty_string_to_null(&form.get_fill_column().to_string()),
                    "零值转空值" => service.zero_to_null(&form.get_fill_column().to_string()),
                    "指定值替换" => service.replace_exact_value(
                        &form.get_value_replace_column().to_string(),
                        &form.get_value_replace_from().to_string(),
                        &form.get_value_replace_to().to_string(),
                    ),
                    "字符串转数值" => service.convert_string_to_numeric(&form.get_cast_column().to_string()),
                    "字符串转日期" => service.convert_string_to_datetime(&form.get_cast_column().to_string()),
                    "整型转浮点" => service.convert_integer_to_float(&form.get_cast_column().to_string()),
                    "布尔值转换" => service.convert_to_boolean(&form.get_bool_convert_column().to_string()),
                    "类型转换" => service.cast_column(
                        &form.get_cast_column().to_string(),
                        parse_logical_type(&form.get_cast_target().to_string()),
                    ),
                    "数值保留小数位" => service.round_numeric(
                        &form.get_round_column().to_string(),
                        parse_usize_or_default(&form.get_round_digits().to_string(), 2),
                    ),
                    "数值乘系数" => service.scale_numeric(
                        &form.get_round_column().to_string(),
                        parse_f64_or_default(&form.get_numeric_scale_factor().to_string(), 1.0),
                    ),
                    "数值加偏移" => service.shift_numeric(
                        &form.get_round_column().to_string(),
                        parse_f64_or_default(&form.get_numeric_offset().to_string(), 0.0),
                    ),
                    "数值裁剪" => service.clamp_numeric(
                        &form.get_round_column().to_string(),
                        parse_optional_f64(&form.get_numeric_clamp_min().to_string()),
                        parse_optional_f64(&form.get_numeric_clamp_max().to_string()),
                    ),
                    _ => service.fill_null_text(
                        &form.get_fill_column().to_string(),
                        &form.get_fill_value().to_string(),
                    ),
                }
                .unwrap_or_else(|error| format!("值处理失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_time_operation({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let operation = form.get_time_operation().to_string();
                let status = match operation.as_str() {
                    "时间戳转换" => {
                        service.convert_timestamp_to_datetime(&form.get_time_target_column().to_string())
                    }
                    "日期拆分" => service.split_datetime_parts(
                        &form.get_time_target_column().to_string(),
                        &form.get_time_output_prefix().to_string(),
                    ),
                    "年" => service.extract_year_to_column(
                        &form.get_time_target_column().to_string(),
                        &form.get_time_output_column().to_string(),
                    ),
                    "月" => service.extract_month_to_column(
                        &form.get_time_target_column().to_string(),
                        &form.get_time_output_column().to_string(),
                    ),
                    "日" => service.extract_day_to_column(
                        &form.get_time_target_column().to_string(),
                        &form.get_time_output_column().to_string(),
                    ),
                    "时" => service.extract_hour_to_column(
                        &form.get_time_target_column().to_string(),
                        &form.get_time_output_column().to_string(),
                    ),
                    "时间窗口筛选" => service.filter_rows_by_time_window(
                        &form.get_time_target_column().to_string(),
                        &form.get_time_window_start().to_string(),
                        &form.get_time_window_end().to_string(),
                    ),
                    "时间排序" => service.sort_by_datetime(
                        &form.get_time_target_column().to_string(),
                        form.get_time_sort_ascending(),
                    ),
                    "时间偏移(分钟)" => service.shift_datetime_by_minutes(
                        &form.get_time_target_column().to_string(),
                        parse_i64_or_default(&form.get_time_shift_minutes().to_string(), 60),
                    ),
                    "提取日期列" => service.extract_date_to_column(
                        &form.get_time_target_column().to_string(),
                        &form.get_time_output_column().to_string(),
                    ),
                    "提取小时列" => service.extract_hour_to_column(
                        &form.get_time_target_column().to_string(),
                        &form.get_time_output_column().to_string(),
                    ),
                    _ => service.normalize_datetime_format(&form.get_time_target_column().to_string()),
                }
                .unwrap_or_else(|error| format!("时间处理失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_rename({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let status = service
                    .rename_column(
                        &form.get_rename_from().to_string(),
                        &form.get_rename_to().to_string(),
                    )
                    .unwrap_or_else(|error| format!("重命名失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_keep_columns({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let columns = split_csv_like(&ui.global::<FormState>().get_column_list().to_string());
                let status = service
                    .keep_columns(columns)
                    .unwrap_or_else(|error| format!("保留列失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_drop_columns({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let columns = split_csv_like(&ui.global::<FormState>().get_column_list().to_string());
                let status = service
                    .drop_columns(columns)
                    .unwrap_or_else(|error| format!("删除列失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_sort({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let status = service
                    .sort_by(&form.get_sort_column().to_string(), form.get_sort_ascending())
                    .unwrap_or_else(|error| format!("排序失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_fill_null({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let status = service
                    .fill_null_text(
                        &form.get_fill_column().to_string(),
                        &form.get_fill_value().to_string(),
                    )
                    .unwrap_or_else(|error| format!("默认值填充失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_fill_forward({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let status = service
                    .fill_null_forward(&form.get_fill_column().to_string())
                    .unwrap_or_else(|error| format!("前值填充失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_cast_column({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let status = service
                    .cast_column(
                        &form.get_cast_column().to_string(),
                        parse_logical_type(&form.get_cast_target().to_string()),
                    )
                    .unwrap_or_else(|error| format!("类型转换失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_filter_contains({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let status = service
                    .filter_rows_contains(
                        &form.get_filter_column().to_string(),
                        &form.get_filter_keyword().to_string(),
                    )
                    .unwrap_or_else(|error| format!("筛选失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_keep_range({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let status = service
                    .keep_row_range(
                        parse_usize_or_default(&form.get_range_start().to_string(), 1),
                        parse_usize_or_default(&form.get_range_end().to_string(), 1),
                    )
                    .unwrap_or_else(|error| format!("保留行范围失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_keep_top_rows({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let count =
                    parse_usize_or_default(&ui.global::<FormState>().get_top_row_count().to_string(), 100);
                let status = service
                    .keep_top_rows(count)
                    .unwrap_or_else(|error| format!("保留前 N 行失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_sample_rows({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let count =
                    parse_usize_or_default(&ui.global::<FormState>().get_sample_row_count().to_string(), 50);
                let status = service
                    .sample_rows(count)
                    .unwrap_or_else(|error| format!("抽样失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_drop_missing_rows({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let columns = split_csv_like(&ui.global::<FormState>().get_missing_columns().to_string());
                let status = service
                    .drop_rows_with_missing(columns)
                    .unwrap_or_else(|error| format!("删除缺失记录失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_text_case({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let status = service
                    .transform_text_case(
                        &form.get_text_column().to_string(),
                        TextCaseMode::from_text(&form.get_text_case().to_string()),
                    )
                    .unwrap_or_else(|error| format!("大小写统一失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_replace_text({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let status = service
                    .replace_text(
                        &form.get_text_column().to_string(),
                        &form.get_replace_from().to_string(),
                        &form.get_replace_to().to_string(),
                    )
                    .unwrap_or_else(|error| format!("文本替换失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_round_numeric({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let status = service
                    .round_numeric(
                        &form.get_round_column().to_string(),
                        parse_usize_or_default(&form.get_round_digits().to_string(), 2),
                    )
                    .unwrap_or_else(|error| format!("数值保留小数位失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_group_aggregate({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let status = service
                    .group_aggregate(
                        split_csv_like(&form.get_aggregate_group_columns().to_string()),
                        &form.get_aggregate_target_column().to_string(),
                        parse_aggregate_function(&form.get_aggregate_function().to_string()),
                    )
                    .unwrap_or_else(|error| format!("分组聚合失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_derive_column({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let operation = form.get_derive_column_operation().to_string();
                let status = match operation.as_str() {
                    "常量列" => service.add_constant_column(
                        &form.get_derive_constant_target().to_string(),
                        &form.get_derive_constant_value().to_string(),
                    ),
                    "表达式计算列" => service.add_expression_column(
                        &form.get_derive_expression_target().to_string(),
                        &form.get_derive_expression().to_string(),
                    ),
                    "条件判断列" => service.add_conditional_column(
                        &form.get_derive_condition_target().to_string(),
                        &form.get_derive_condition_source_column().to_string(),
                        parse_compare_operator(&form.get_derive_condition_operator().to_string()),
                        &form.get_derive_condition_value().to_string(),
                        &form.get_derive_condition_true_value().to_string(),
                        &form.get_derive_condition_false_value().to_string(),
                    ),
                    "拼接列" => service.concat_columns(
                        split_csv_like(&form.get_derive_concat_columns().to_string()),
                        &form.get_derive_concat_target().to_string(),
                        &form.get_derive_concat_separator().to_string(),
                    ),
                    "时间差列" => service.add_time_diff_column(
                        &form.get_derive_time_diff_start_column().to_string(),
                        &form.get_derive_time_diff_end_column().to_string(),
                        &form.get_derive_time_diff_target().to_string(),
                        parse_time_diff_unit(&form.get_derive_time_diff_unit().to_string()),
                    ),
                    _ => Ok("未识别的新列生成操作".to_string()),
                }
                .unwrap_or_else(|error| format!("派生列生成失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_derive_group({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let status = service
                    .group_aggregate(
                        split_csv_like(&form.get_derive_group_columns().to_string()),
                        &form.get_derive_group_target_column().to_string(),
                        parse_aggregate_function(&form.get_derive_group_function().to_string()),
                    )
                    .unwrap_or_else(|error| format!("分组与聚合失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_derive_sort({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let operation = form.get_derive_sort_operation().to_string();
                let status = match operation.as_str() {
                    "单列排序" => service.sort_by(
                        &form.get_derive_sort_column().to_string(),
                        form.get_derive_sort_ascending(),
                    ),
                    "多列排序" => {
                        let columns = split_csv_like(&form.get_derive_sort_columns().to_string());
                        let directions = parse_sort_directions(&form.get_derive_sort_orders().to_string(), columns.len());
                        service.multi_sort(columns, directions)
                    }
                    "条件优先排序" => {
                        let columns = split_csv_like(&form.get_derive_sort_columns().to_string());
                        let directions = parse_sort_directions(&form.get_derive_sort_orders().to_string(), columns.len());
                        service.priority_sort(
                            &form.get_derive_priority_column().to_string(),
                            parse_compare_operator(&form.get_derive_priority_operator().to_string()),
                            &form.get_derive_priority_value().to_string(),
                            parse_priority_placement(&form.get_derive_priority_placement().to_string()),
                            columns,
                            directions,
                        )
                    }
                    "生成排名列" => {
                        let columns = if form.get_derive_sort_columns().to_string().trim().is_empty() {
                            vec![form.get_derive_sort_column().to_string()]
                        } else {
                            split_csv_like(&form.get_derive_sort_columns().to_string())
                        };
                        let directions = if form.get_derive_sort_columns().to_string().trim().is_empty() {
                            vec![form.get_derive_sort_ascending()]
                        } else {
                            parse_sort_directions(&form.get_derive_sort_orders().to_string(), columns.len())
                        };
                        service.add_rank_column(
                            &form.get_derive_rank_output_column().to_string(),
                            columns,
                            directions,
                        )
                    }
                    _ => Ok("未识别的排序操作".to_string()),
                }
                .unwrap_or_else(|error| format!("排序与排名失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_derive_window({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let group_columns = split_csv_like(&form.get_derive_window_group_columns().to_string());
                let window_size = parse_bounded_usize(&form.get_derive_window_size().to_string(), 3, 1, 9999);
                let status = match form.get_derive_window_operation().to_string().as_str() {
                    "滚动统计" => service.rolling_aggregate(
                        group_columns,
                        &form.get_derive_window_order_column().to_string(),
                        &form.get_derive_window_target_column().to_string(),
                        window_size,
                        parse_aggregate_function(&form.get_derive_window_function().to_string()),
                        &form.get_derive_window_output_column().to_string(),
                    ),
                    "累积和" => service.cumulative_sum(
                        group_columns,
                        &form.get_derive_window_order_column().to_string(),
                        &form.get_derive_window_target_column().to_string(),
                        &form.get_derive_window_output_column().to_string(),
                    ),
                    "滑动平均" => service.moving_average(
                        group_columns,
                        &form.get_derive_window_order_column().to_string(),
                        &form.get_derive_window_target_column().to_string(),
                        window_size,
                        &form.get_derive_window_output_column().to_string(),
                    ),
                    "邻近值比较" => service.compare_adjacent(
                        group_columns,
                        &form.get_derive_window_order_column().to_string(),
                        &form.get_derive_window_target_column().to_string(),
                        parse_adjacent_compare_mode(&form.get_derive_window_compare_mode().to_string()),
                        &form.get_derive_window_output_column().to_string(),
                    ),
                    _ => Ok("未识别的窗口操作".to_string()),
                }
                .unwrap_or_else(|error| format!("窗口类处理失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_apply_join({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let form = ui.global::<FormState>();
                let status = service
                    .join_selected_with(
                        form.get_join_dataset_id()
                            .to_string()
                            .trim()
                            .parse::<i32>()
                            .unwrap_or_default(),
                        split_csv_like(&form.get_join_left_key().to_string()),
                        split_csv_like(&form.get_join_right_key().to_string()),
                        JoinKind::from_text(&form.get_join_kind().to_string()),
                    )
                    .unwrap_or_else(|error| format!("融合失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_export_csv({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let status = export_with_dialog("导出当前数据集为 CSV", "csv", |path| {
                    service.export_selected_csv(&path)
                })
                .unwrap_or_else(|error| format!("导出失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_export_json({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let status = export_with_dialog("导出当前数据集为 JSON", "json", |path| {
                    service.export_selected_json(&path)
                })
                .unwrap_or_else(|error| format!("导出失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_export_quality_report({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let status = export_with_dialog("导出质量报告 PDF", "pdf", |path| {
                    service.export_quality_report(&path)
                })
                .unwrap_or_else(|error| format!("质量报告导出失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_save_pipeline_template({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let status = export_with_dialog("保存流程模板", "json", |path| {
                    service.save_pipeline_template(&path)
                })
                .unwrap_or_else(|error| format!("保存流程模板失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });

    let weak = ui.as_weak();
    ui.global::<Logic>().on_replay_pipeline_template({
        let service = service.clone();
        move || {
            with_ui(&weak, &service, |ui, service| {
                let status = FileDialog::new()
                    .add_filter("JSON", &["json"])
                    .pick_file()
                    .map(|path| service.replay_pipeline_template(&path))
                    .transpose()
                    .map(|message| message.unwrap_or_else(|| "已取消模板回放".to_string()))
                    .unwrap_or_else(|error| format!("模板回放失败：{error:#}"));
                refresh_ui(ui, service, &status);
            });
        }
    });
}

fn install_zero_arg_action<F>(
    ui: &MainWindow,
    service: Rc<RefCell<AppService>>,
    callback_name: &str,
    action: F,
) where
    F: Fn(&mut AppService) -> Result<String> + 'static,
{
    let weak = ui.as_weak();
    match callback_name {
        "on_refresh_current" => ui.global::<Logic>().on_refresh_current(move || {
            run_simple_action(&weak, &service, &action);
        }),
        "on_normalize_columns" => ui.global::<Logic>().on_normalize_columns(move || {
            run_simple_action(&weak, &service, &action);
        }),
        "on_trim_text_values" => ui.global::<Logic>().on_trim_text_values(move || {
            run_simple_action(&weak, &service, &action);
        }),
        "on_drop_empty_rows" => ui.global::<Logic>().on_drop_empty_rows(move || {
            run_simple_action(&weak, &service, &action);
        }),
        "on_deduplicate_rows" => ui.global::<Logic>().on_deduplicate_rows(move || {
            run_simple_action(&weak, &service, &action);
        }),
        "on_apply_recommended_mapping" => {
            ui.global::<Logic>().on_apply_recommended_mapping(move || {
                run_simple_action(&weak, &service, &action);
            })
        }
        _ => {}
    }
}

fn run_simple_action<F>(
    weak: &slint::Weak<MainWindow>,
    service: &Rc<RefCell<AppService>>,
    action: &F,
) where
    F: Fn(&mut AppService) -> Result<String>,
{
    with_ui(weak, service, |ui, service| {
        let status = action(service).unwrap_or_else(|error| format!("执行失败：{error:#}"));
        refresh_ui(ui, service, &status);
    });
}

fn refresh_ui(ui: &MainWindow, service: &AppService, status: &str) {
    let state = ui.global::<AppState>();
    let form = ui.global::<FormState>();
    let previous_dataset_id = state.get_selected_dataset_id();

    let dataset_rows = service
        .dataset_snapshots()
        .into_iter()
        .map(|snapshot| DatasetCardData {
            dataset_id: snapshot.dataset_id,
            name: snapshot.dataset_name.into(),
            format: snapshot.format.into(),
            size_label: snapshot.size_label.into(),
            imported_at: snapshot.imported_at.into(),
            import_duration: snapshot.import_duration.into(),
            sheet_name: snapshot.sheet_name.into(),
            overview: snapshot.overview.into(),
            key_hint: snapshot.key_hint.into(),
            time_hint: snapshot.time_hint.into(),
        })
        .collect::<Vec<_>>();
    state.set_datasets(ModelRc::new(VecModel::from(dataset_rows)));

    if let Some(record) = service.selected_dataset() {
        if previous_dataset_id != record.id {
            form.set_preview_page(1);
            form.set_field_page(1);
            form.set_quality_primary_key(record.quality_rules.primary_key.clone().into());
            form.set_quality_composite_keys(record.quality_rules.composite_keys.join(", ").into());
            form.set_quality_time_column(record.quality_rules.time_column.clone().into());
            form.set_quality_missing_threshold(
                format_threshold_percent(record.quality_rules.normalized_threshold()).into(),
            );
            form.set_quality_range_column(record.quality_rules.range_column.clone().into());
            form.set_quality_range_min(
                record
                    .quality_rules
                    .range_min
                    .map(|value| value.to_string())
                    .unwrap_or_default()
                    .into(),
            );
            form.set_quality_range_max(
                record
                    .quality_rules
                    .range_max
                    .map(|value| value.to_string())
                    .unwrap_or_default()
                    .into(),
            );
            form.set_quality_text_column(record.quality_rules.length_column.clone().into());
            form.set_quality_max_length(
                record
                    .quality_rules
                    .max_text_length
                    .map(|value| value.to_string())
                    .unwrap_or_default()
                    .into(),
            );
            form.set_quality_time_gap_minutes(
                record
                    .quality_rules
                    .time_gap_minutes
                    .map(|value| value.to_string())
                    .unwrap_or_default()
                    .into(),
            );
        }

        let preview_size = parse_bounded_usize(&form.get_preview_row_count().to_string(), 20, 1, 200);
        let field_size = parse_bounded_usize(&form.get_field_row_count().to_string(), 12, 5, 50);
        let preview_page = form.get_preview_page().max(1) as usize;
        let field_page = form.get_field_page().max(1) as usize;
        let preview_mode = form.get_preview_mode().to_string();

        let preview = build_preview_model(record, preview_mode.as_str(), preview_page, preview_size);
        let fields = build_column_model(record, field_page, field_size);
        form.set_preview_page(preview.page as i32);
        form.set_field_page(fields.page as i32);

        let source_name = record
            .source_path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default();

        state.set_selected_dataset_id(record.id);
        state.set_current_dataset_name(record.dataset_name.clone().into());
        state.set_current_dataset_overview(
            format!(
                "{} | {} 行 × {} 列 | {} | 导入 {}",
                record.format.as_str(),
                record.profile.row_count,
                record.profile.column_count,
                source_name,
                record.imported_at.format("%H:%M:%S")
            )
            .into(),
        );
        state.set_quality_summary(
            format!(
                "问题 {} 项，高缺失 {} 列，重复 {} 行，空记录 {} 行，自定义规则触发 {} 项",
                record.profile.quality_issues.len(),
                record.profile.quality_overview.high_missing_field_count,
                record.profile.quality_overview.duplicate_row_count,
                record.profile.quality_overview.fully_empty_row_count,
                record.profile.quality_overview.range_rule_issue_count
                    + record.profile.quality_overview.text_length_issue_count
                    + record.profile.quality_overview.time_gap_issue_count
            )
            .into(),
        );
        state.set_quality_rule_summary(
            build_quality_rule_summary(record).into(),
        );
        state.set_active_count_label(
            format!(
                "当前数据集：{} / 步骤 {} / 最近导入 {}",
                record.dataset_name,
                record.pipeline_steps.len(),
                record.imported_at.format("%Y-%m-%d %H:%M:%S")
            )
            .into(),
        );
        state.set_preview_columns(ModelRc::new(VecModel::from(map_preview_columns(
            &record.working_table.preview_header(record.working_table.width()),
        ))));
        state.set_preview_page_label(preview.page_label.into());
        state.set_preview_range_label(preview.range_label.into());
        state.set_field_page_label(fields.page_label.into());
        state.set_field_range_label(fields.range_label.into());
        state.set_field_page_size(field_size as i32);
        state.set_can_preview_prev(preview.can_prev);
        state.set_can_preview_next(preview.can_next);
        state.set_can_field_prev(fields.can_prev);
        state.set_can_field_next(fields.can_next);
        state.set_can_undo(service.can_undo());
        state.set_can_redo(service.can_redo());

        state.set_metrics(ModelRc::new(VecModel::from(build_metrics(record))));
        state.set_columns(ModelRc::new(VecModel::from(fields.rows)));
        state.set_preview_rows(ModelRc::new(VecModel::from(preview.rows)));
        state.set_issues(ModelRc::new(VecModel::from(
            record
                .profile
                .quality_issues
                .iter()
                .map(|issue| IssueRowData {
                    category: issue.category.clone().into(),
                    severity: issue.severity.clone().into(),
                    field: issue.field.clone().into(),
                    detail: issue.detail.clone().into(),
                })
                .collect::<Vec<_>>(),
        )));
        state.set_steps(ModelRc::new(VecModel::from(
            record
                .pipeline_steps
                .iter()
                .map(|step| StepRowData {
                    time: step.timestamp.format("%H:%M:%S").to_string().into(),
                    action: step.action.clone().into(),
                    detail: step.detail.clone().into(),
                    outcome: step.outcome.clone().into(),
                })
                .collect::<Vec<_>>(),
        )));
        state.set_mappings(ModelRc::new(VecModel::from(
            record
                .profile
                .mapping_suggestions
                .iter()
                .map(|mapping| MappingRowData {
                    source_name: mapping.source_name.clone().into(),
                    target_name: mapping.target_name.clone().into(),
                    confidence: mapping.confidence.clone().into(),
                    status: mapping.status.clone().into(),
                })
                .collect::<Vec<_>>(),
        )));
        state.set_join_suggestions(ModelRc::new(VecModel::from(
            service
                .join_suggestions()
                .iter()
                .map(|suggestion| JoinSuggestionData {
                    target_dataset: suggestion.target_dataset.clone().into(),
                    left_key: suggestion.left_key.clone().into(),
                    right_key: suggestion.right_key.clone().into(),
                    join_type: suggestion.join_type.clone().into(),
                    reason: suggestion.reason.clone().into(),
                })
                .collect::<Vec<_>>(),
        )));
    } else {
        state.set_selected_dataset_id(0);
        state.set_current_dataset_name("尚未导入数据".into());
        state.set_current_dataset_overview("点击左侧导入按钮开始".into());
        state.set_quality_summary("暂无分析结果".into());
        form.set_quality_primary_key(SharedString::new());
        form.set_quality_composite_keys(SharedString::new());
        form.set_quality_time_column(SharedString::new());
        form.set_quality_missing_threshold("30".into());
        form.set_quality_range_column(SharedString::new());
        form.set_quality_range_min(SharedString::new());
        form.set_quality_range_max(SharedString::new());
        form.set_quality_text_column(SharedString::new());
        form.set_quality_max_length(SharedString::new());
        form.set_quality_time_gap_minutes(SharedString::new());
        state.set_quality_rule_summary(
            "主键 [未识别] | 组合键 [未设置] | 时间列 [未识别] | 缺失阈值 [30%] | 数值范围 [未设置] | 文本长度 [未设置] | 时间间隔 [未设置]"
                .into(),
        );
        state.set_active_count_label("等待导入".into());
        state.set_preview_columns(ModelRc::new(VecModel::from(Vec::<SharedString>::new())));
        state.set_preview_page_label("第 0 / 0 页".into());
        state.set_preview_range_label("暂无记录".into());
        state.set_field_page_label("第 0 / 0 页".into());
        state.set_field_range_label("暂无字段".into());
        state.set_field_page_size(0);
        state.set_can_preview_prev(false);
        state.set_can_preview_next(false);
        state.set_can_field_prev(false);
        state.set_can_field_next(false);
        state.set_can_undo(false);
        state.set_can_redo(false);
        state.set_metrics(ModelRc::new(VecModel::from(Vec::<MetricCardData>::new())));
        state.set_columns(ModelRc::new(VecModel::from(Vec::<ColumnRowData>::new())));
        state.set_preview_rows(ModelRc::new(VecModel::from(Vec::<PreviewRowData>::new())));
        state.set_issues(ModelRc::new(VecModel::from(Vec::<IssueRowData>::new())));
        state.set_steps(ModelRc::new(VecModel::from(Vec::<StepRowData>::new())));
        state.set_mappings(ModelRc::new(VecModel::from(Vec::<MappingRowData>::new())));
        state.set_join_suggestions(ModelRc::new(VecModel::from(Vec::<JoinSuggestionData>::new())));
    }

    state.set_status_message(status.into());
}

fn build_metrics(record: &DatasetRecord) -> Vec<MetricCardData> {
    vec![
        MetricCardData {
            title: "总行数".into(),
            value: record.profile.row_count.to_string().into(),
            detail: "当前工作表记录规模".into(),
        },
        MetricCardData {
            title: "字段总数".into(),
            value: record.profile.column_count.to_string().into(),
            detail: format!(
                "数值列 {} / 时间列 {}",
                record.profile.numeric_columns.len(),
                record.profile.time_candidates.len()
            )
            .into(),
        },
        MetricCardData {
            title: "质量问题".into(),
            value: record.profile.quality_issues.len().to_string().into(),
            detail: format!(
                "高缺失 {} / 主键重复 {}",
                record.profile.quality_overview.high_missing_field_count,
                record.profile.quality_overview.primary_key_duplicate_count
            )
            .into(),
        },
        MetricCardData {
            title: "流程步骤".into(),
            value: record.pipeline_steps.len().to_string().into(),
            detail: "支持撤销、重做和模板回放".into(),
        },
    ]
}

fn map_preview_columns(header: &crate::model::PreviewHeader) -> Vec<SharedString> {
    header.cells.iter().cloned().map(SharedString::from).collect()
}

fn map_preview_row(row: &PreviewRow) -> PreviewRowData {
    PreviewRowData {
        row_label: row.row_label.clone().into(),
        cells: ModelRc::new(VecModel::from(
            row.cells
                .iter()
                .cloned()
                .map(SharedString::from)
                .collect::<Vec<_>>(),
        )),
    }
}

fn parse_logical_type(value: &str) -> LogicalType {
    match value.trim().to_ascii_lowercase().as_str() {
        "integer" | "int" | "整数" => LogicalType::Integer,
        "float" | "double" | "浮点" => LogicalType::Float,
        "bool" | "boolean" | "布尔" => LogicalType::Boolean,
        "datetime" | "time" | "时间" => LogicalType::DateTime,
        _ => LogicalType::Text,
    }
}

fn parse_aggregate_function(value: &str) -> AggregateFunction {
    AggregateFunction::from_text(value)
}

fn parse_compare_operator(value: &str) -> CompareOperator {
    CompareOperator::from_text(value)
}

fn parse_time_diff_unit(value: &str) -> TimeDiffUnit {
    TimeDiffUnit::from_text(value)
}

fn parse_priority_placement(value: &str) -> PriorityPlacement {
    PriorityPlacement::from_text(value)
}

fn parse_adjacent_compare_mode(value: &str) -> AdjacentCompareMode {
    AdjacentCompareMode::from_text(value)
}

fn parse_sort_directions(value: &str, width: usize) -> Vec<bool> {
    let parts = split_csv_like(value);
    if parts.is_empty() {
        return vec![true; width];
    }
    (0..width)
        .map(|index| {
            parts
                .get(index)
                .or_else(|| parts.last())
                .map(|item| !matches!(item.trim().to_ascii_lowercase().as_str(), "desc" | "降序"))
                .unwrap_or(true)
        })
        .collect()
}

fn parse_usize_or_default(value: &str, default_value: usize) -> usize {
    value.trim().parse::<usize>().unwrap_or(default_value)
}

fn parse_bounded_usize(value: &str, default_value: usize, min_value: usize, max_value: usize) -> usize {
    parse_usize_or_default(value, default_value).clamp(min_value, max_value)
}

fn parse_f64_or_default(value: &str, default_value: f64) -> f64 {
    value.trim().parse::<f64>().unwrap_or(default_value)
}

fn parse_i64_or_default(value: &str, default_value: i64) -> i64 {
    value.trim().parse::<i64>().unwrap_or(default_value)
}

fn parse_optional_f64(value: &str) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        trimmed.parse::<f64>().ok()
    }
}

fn split_csv_like(value: &str) -> Vec<String> {
    value
        .split([',', '，', ';', '；'])
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect()
}

fn export_with_dialog<F>(title: &str, extension: &str, action: F) -> Result<String>
where
    F: FnOnce(PathBuf) -> Result<String>,
{
    let Some(path) = FileDialog::new()
        .set_title(title)
        .add_filter(extension.to_ascii_uppercase(), &[extension])
        .save_file()
    else {
        return Ok("已取消导出".to_string());
    };

    action(path)
}

fn with_ui<F>(weak: &slint::Weak<MainWindow>, service: &Rc<RefCell<AppService>>, action: F)
where
    F: FnOnce(&MainWindow, &mut AppService),
{
    let Some(ui) = weak.upgrade() else {
        return;
    };

    let mut service = service.borrow_mut();
    action(&ui, &mut service);
}

struct PreviewModel {
    rows: Vec<PreviewRowData>,
    page: usize,
    page_label: String,
    range_label: String,
    can_prev: bool,
    can_next: bool,
}

struct ColumnPageModel {
    rows: Vec<ColumnRowData>,
    page: usize,
    page_label: String,
    range_label: String,
    can_prev: bool,
    can_next: bool,
}

fn build_preview_model(
    record: &DatasetRecord,
    preview_mode: &str,
    requested_page: usize,
    page_size: usize,
) -> PreviewModel {
    let (_, start, end) = page_window(record.working_table.height(), requested_page, page_size);
    let total_items = record.working_table.height();
    let actual_total_pages = if total_items == 0 {
        1
    } else {
        (total_items + page_size - 1) / page_size
    };
    let page = requested_page.clamp(1, actual_total_pages.max(1));
    let preview_rows = if preview_mode == "随机抽样" {
        record
            .working_table
            .preview_sample_rows(page, page_size, record.working_table.width())
    } else {
        record
            .working_table
            .preview_rows_window(page, page_size, record.working_table.width())
    };

    PreviewModel {
        rows: preview_rows.iter().map(map_preview_row).collect(),
        page,
        page_label: format!("第 {} / {} 页", page, actual_total_pages.max(1)),
        range_label: if total_items == 0 {
            "暂无记录".to_string()
        } else if preview_mode == "随机抽样" {
            format!("样本 {}-{} / {}", start + 1, end, total_items)
        } else {
            format!("行 {}-{} / {}", start + 1, end, total_items)
        },
        can_prev: page > 1,
        can_next: page < actual_total_pages,
    }
}

fn build_column_model(record: &DatasetRecord, requested_page: usize, page_size: usize) -> ColumnPageModel {
    let total_items = record.profile.columns.len();
    let total_pages = if total_items == 0 {
        1
    } else {
        (total_items + page_size - 1) / page_size
    };
    let page = requested_page.clamp(1, total_pages.max(1));
    let (_, start, end) = page_window(total_items, page, page_size);
    let rows = record.profile.columns[start..end]
        .iter()
        .map(|column| ColumnRowData {
            name: column.name.clone().into(),
            dtype: column.logical_type.clone().into(),
            non_null: column.non_null_count.to_string().into(),
            missing_count: column.missing_count.to_string().into(),
            missing_rate: format!("{:.1}%", column.missing_rate * 100.0).into(),
            unique_count: column.unique_count.to_string().into(),
            sample_value: column.sample_value.clone().into(),
            role_hint: column.role_hint.clone().into(),
        })
        .collect::<Vec<_>>();

    ColumnPageModel {
        rows,
        page,
        page_label: format!("第 {} / {} 页", page, total_pages.max(1)),
        range_label: if total_items == 0 {
            "暂无字段".to_string()
        } else {
            format!("字段 {}-{} / {}", start + 1, end, total_items)
        },
        can_prev: page > 1,
        can_next: page < total_pages,
    }
}

fn display_or_fallback(value: &str, fallback: &str) -> String {
    if value.trim().is_empty() {
        fallback.to_string()
    } else {
        value.to_string()
    }
}

fn format_threshold_percent(value: f32) -> String {
    format!("{:.0}", value * 100.0)
}

fn build_quality_rule_summary(record: &DatasetRecord) -> String {
    let composite = if record.profile.resolved_composite_keys.is_empty() {
        "未设置".to_string()
    } else {
        record.profile.resolved_composite_keys.join(", ")
    };

    format!(
        "主键 [{}] | 组合键 [{}] | 时间列 [{}] | 缺失阈值 [{}%] | 数值范围 [{}] | 文本长度 [{}] | 时间间隔 [{}]",
        display_or_fallback(&record.profile.resolved_primary_key, "未识别"),
        composite,
        display_or_fallback(&record.profile.resolved_time_column, "未识别"),
        format_threshold_percent(record.quality_rules.normalized_threshold()),
        describe_range_rule(record),
        describe_length_rule(record),
        describe_time_gap_rule(record),
    )
}

fn describe_range_rule(record: &DatasetRecord) -> String {
    let column = record.quality_rules.range_column.trim();
    if column.is_empty()
        || (record.quality_rules.range_min.is_none() && record.quality_rules.range_max.is_none())
    {
        return "未设置".to_string();
    }

    match (record.quality_rules.range_min, record.quality_rules.range_max) {
        (Some(min), Some(max)) => format!("{column}: {min} ~ {max}"),
        (Some(min), None) => format!("{column}: >= {min}"),
        (None, Some(max)) => format!("{column}: <= {max}"),
        (None, None) => "未设置".to_string(),
    }
}

fn describe_length_rule(record: &DatasetRecord) -> String {
    let column = record.quality_rules.length_column.trim();
    match (column.is_empty(), record.quality_rules.max_text_length) {
        (false, Some(limit)) => format!("{column} <= {limit}"),
        _ => "未设置".to_string(),
    }
}

fn describe_time_gap_rule(record: &DatasetRecord) -> String {
    match record.quality_rules.time_gap_minutes {
        Some(minutes) if minutes > 0 => format!("{minutes} 分钟"),
        _ => "未设置".to_string(),
    }
}
