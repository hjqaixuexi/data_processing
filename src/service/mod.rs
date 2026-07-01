use crate::exporter;
use crate::fusion::{self, FusionRequest};
use crate::inspector;
use crate::loader;
use crate::model::{
    AdjacentCompareMode, AggregateFunction, CompareOperator, DatasetHistory, DatasetRecord,
    DatasetSnapshot, FileFormat, JoinConflictStrategy, JoinKind, LoadedDataset, LogicalType,
    PipelineOperation, PipelineStep, PriorityPlacement, QualityRules, StatisticFillStrategy,
    TextCaseMode, TimeDiffUnit, format_bytes, format_duration_millis,
};
use crate::pipeline;
use crate::processor;
use crate::visualization::{
    self, VisualizationChartType, VisualizationFieldSuggestion, VisualizationReport,
    VisualizationRequest,
};
use anyhow::{Context, Result, bail};
use chrono::Local;
use std::path::Path;

pub struct AppService {
    next_dataset_id: i32,
    datasets: Vec<DatasetRecord>,
    pub selected_dataset_id: Option<i32>,
    last_join_report: Option<JoinReport>,
    last_fusion_report: Option<FusionRunReport>,
    last_visualization_preview: Option<VisualizationReport>,
}

#[derive(Clone, Debug)]
pub struct JoinReport {
    pub matched_rows: usize,
    pub unmatched_left: usize,
    pub unmatched_right: usize,
    pub conflict_fields: Vec<String>,
    pub conflict_strategy: String,
    pub data_loss_hint: String,
}

#[derive(Clone, Debug)]
pub struct FusionRunReport {
    pub source_summary: String,
    pub alignment_summary: String,
    pub quality_summary: String,
    pub output_summary: String,
    pub trace_summary: String,
    pub skipped_sources: Vec<String>,
}

impl AppService {
    pub fn new() -> Self {
        Self {
            next_dataset_id: 1,
            datasets: Vec::new(),
            selected_dataset_id: None,
            last_join_report: None,
            last_fusion_report: None,
            last_visualization_preview: None,
        }
    }

    pub fn import_paths(&mut self, paths: &[std::path::PathBuf]) -> Result<String> {
        let loaded = loader::load_paths(paths)?;
        let imported = loaded
            .into_iter()
            .map(|dataset| self.push_dataset(dataset))
            .collect::<Result<Vec<_>>>()?;

        if let Some(last) = imported.last() {
            self.selected_dataset_id = Some(*last);
        }
        self.invalidate_visualization_preview();

        Ok(format!("已导入 {} 个数据集", imported.len()))
    }

    pub fn dataset_snapshots(&self) -> Vec<DatasetSnapshot> {
        self.datasets
            .iter()
            .map(|record| DatasetSnapshot {
                dataset_id: record.id,
                dataset_name: record.dataset_name.clone(),
                format: record.format.as_str().to_string(),
                size_label: format_bytes(record.size_bytes),
                imported_at: record.imported_at.format("%Y-%m-%d %H:%M:%S").to_string(),
                import_duration: format_duration_millis(record.import_duration_ms),
                sheet_name: record.sheet_name.clone().unwrap_or_default(),
                overview: format!("{} 行 × {} 列", record.profile.row_count, record.profile.column_count),
                key_hint: if !record.profile.resolved_primary_key.is_empty() {
                    record.profile.resolved_primary_key.clone()
                } else if record.profile.key_candidates.is_empty() {
                    "未识别".to_string()
                } else {
                    record.profile.key_candidates.join(", ")
                },
                time_hint: if !record.profile.resolved_time_column.is_empty() {
                    record.profile.resolved_time_column.clone()
                } else if record.profile.time_candidates.is_empty() {
                    "未识别".to_string()
                } else {
                    record.profile.time_candidates.join(", ")
                },
            })
            .collect()
    }

    pub fn selected_dataset(&self) -> Option<&DatasetRecord> {
        let id = self.selected_dataset_id?;
        self.datasets.iter().find(|record| record.id == id)
    }

    pub fn join_target_hint(&self) -> String {
        let selected_id = self.selected_dataset_id;
        let options = self
            .datasets
            .iter()
            .filter(|record| Some(record.id) != selected_id)
            .map(|record| format!("{}={}", record.id, record.dataset_name))
            .collect::<Vec<_>>();
        if options.is_empty() {
            "当前没有可融合的目标数据集".to_string()
        } else {
            format!("可选目标数据集：{}", options.join(" | "))
        }
    }

    pub fn fusion_source_hint(&self) -> String {
        let selected_id = self.selected_dataset_id;
        let options = self
            .datasets
            .iter()
            .filter(|record| Some(record.id) != selected_id)
            .map(|record| format!("{}={}", record.id, record.dataset_name))
            .collect::<Vec<_>>();
        if options.is_empty() {
            "当前没有可用辅源数据集".to_string()
        } else {
            format!("可选辅源：{}", options.join(" | "))
        }
    }

    pub fn last_join_report(&self) -> Option<&JoinReport> {
        self.last_join_report.as_ref()
    }

    pub fn last_fusion_report(&self) -> Option<&FusionRunReport> {
        self.last_fusion_report.as_ref()
    }

    pub fn last_visualization_preview(&self) -> Option<&VisualizationReport> {
        self.last_visualization_preview.as_ref()
    }

    pub fn can_undo(&self) -> bool {
        self.selected_dataset()
            .map(|record| !record.undo_stack.is_empty())
            .unwrap_or(false)
    }

    pub fn can_redo(&self) -> bool {
        self.selected_dataset()
            .map(|record| !record.redo_stack.is_empty())
            .unwrap_or(false)
    }

    pub fn select_dataset(&mut self, dataset_id: i32) -> Result<()> {
        if self.datasets.iter().any(|record| record.id == dataset_id) {
            self.selected_dataset_id = Some(dataset_id);
            self.invalidate_visualization_preview();
            Ok(())
        } else {
            bail!("未找到数据集: {dataset_id}")
        }
    }

    pub fn delete_selected_dataset(&mut self) -> Result<String> {
        let selected_id = self.selected_dataset_id.context("当前没有选中数据集")?;
        let current_index = self
            .datasets
            .iter()
            .position(|record| record.id == selected_id)
            .context("数据集不存在")?;
        let removed = self.datasets.remove(current_index);

        self.selected_dataset_id = if self.datasets.is_empty() {
            None
        } else {
            let next_index = current_index.min(self.datasets.len().saturating_sub(1));
            Some(self.datasets[next_index].id)
        };
        self.invalidate_visualization_preview();

        Ok(format!("已删除数据集：{}", removed.dataset_name))
    }

    pub fn suggest_visualization_fields(&self, chart_type: &str) -> Result<VisualizationFieldSuggestion> {
        let record = self.selected_dataset().context("当前没有选中数据集")?;
        Ok(visualization::suggest_fields(
            &record.dataset_name,
            &record.working_table,
            &VisualizationChartType::from_text(chart_type),
        ))
    }

    pub fn render_visualization_preview(&mut self, request: VisualizationRequest) -> Result<String> {
        let record = self.selected_dataset().context("当前没有选中数据集")?;
        let report = visualization::render_preview(&record.working_table, &request)?;
        let status = format!("图表预览已更新：{} | {}", report.chart_name, report.summary);
        self.last_visualization_preview = Some(report);
        Ok(status)
    }

    pub fn export_visualization(&self, request: VisualizationRequest, path: &Path) -> Result<String> {
        let record = self.selected_dataset().context("当前没有选中数据集")?;
        let report = visualization::export_chart(&record.working_table, &request, path)?;
        Ok(format!(
            "已导出图表：{} | {} | {}",
            report.chart_name, report.output_format, report.output_path
        ))
    }

    pub fn inspect_selected(&mut self) -> Result<String> {
        self.apply_operation(PipelineOperation::Reinspect, "基于当前工作表重新生成结构和质量分析")
    }

    pub fn undo(&mut self) -> Result<String> {
        let record = self.selected_dataset_mut()?;
        let Some(previous) = record.undo_stack.pop() else {
            bail!("当前数据集没有可撤销的操作");
        };

        record.redo_stack.push(Self::capture_history(record));
        Self::restore_history(record, previous)?;
        let row_count = record.working_table.height();
        let column_count = record.working_table.width();
        self.invalidate_visualization_preview();
        Ok(format!(
            "已撤销，当前工作表 {} 行 {} 列",
            row_count,
            column_count
        ))
    }

    pub fn redo(&mut self) -> Result<String> {
        let record = self.selected_dataset_mut()?;
        let Some(next) = record.redo_stack.pop() else {
            bail!("当前数据集没有可重做的操作");
        };

        record.undo_stack.push(Self::capture_history(record));
        Self::restore_history(record, next)?;
        let row_count = record.working_table.height();
        let column_count = record.working_table.width();
        self.invalidate_visualization_preview();
        Ok(format!(
            "已重做，当前工作表 {} 行 {} 列",
            row_count,
            column_count
        ))
    }

    pub fn normalize_columns(&mut self) -> Result<String> {
        self.apply_operation(
            PipelineOperation::NormalizeColumnNames,
            "统一字段命名风格并消除重名冲突",
        )
    }

    pub fn trim_text_values(&mut self) -> Result<String> {
        self.apply_operation(
            PipelineOperation::TrimTextValues,
            "清除文本首尾空白，减少映射和融合时的脏值干扰",
        )
    }

    pub fn drop_empty_rows(&mut self) -> Result<String> {
        self.apply_operation(PipelineOperation::DropEmptyRows, "删除所有单元格均为空的记录")
    }

    pub fn deduplicate_rows(&mut self) -> Result<String> {
        self.apply_operation(PipelineOperation::DeduplicateRows, "按整行内容删除重复记录")
    }

    pub fn filter_rows_contains(&mut self, column: &str, keyword: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::FilterRowsContains {
                column: column.trim().to_string(),
                keyword: keyword.trim().to_string(),
            },
            "按关键字筛选记录，保留匹配行",
        )
    }

    pub fn keep_row_range(&mut self, start: usize, end: usize) -> Result<String> {
        self.apply_operation(
            PipelineOperation::KeepRowRange { start, end },
            "按行范围保留记录",
        )
    }

    pub fn keep_top_rows(&mut self, count: usize) -> Result<String> {
        self.apply_operation(
            PipelineOperation::KeepTopRows { count },
            "保留前 N 行，用于快速缩小工作集",
        )
    }

    pub fn sample_rows(&mut self, count: usize) -> Result<String> {
        self.apply_operation(
            PipelineOperation::SampleRows { count },
            "按全表均匀抽样，适合做预检和试验性处理",
        )
    }

    pub fn keep_rows_with_missing(&mut self, columns: Vec<String>) -> Result<String> {
        self.apply_operation(
            PipelineOperation::KeepRowsWithMissing { columns },
            "保留指定字段中存在缺失值的记录",
        )
    }

    pub fn drop_rows_with_missing(&mut self, columns: Vec<String>) -> Result<String> {
        self.apply_operation(
            PipelineOperation::DropRowsWithMissing { columns },
            "删除指定字段中存在缺失值的记录",
        )
    }

    pub fn drop_rows_not_contains(&mut self, column: &str, keyword: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::DropRowsNotContains {
                column: column.trim().to_string(),
                keyword: keyword.trim().to_string(),
            },
            "删除指定列中不包含关键字的记录",
        )
    }

    pub fn drop_row_range(&mut self, start: usize, end: usize) -> Result<String> {
        self.apply_operation(
            PipelineOperation::DropRowRange { start, end },
            "按行范围删除记录",
        )
    }

    pub fn deduplicate_by_columns(&mut self, columns: Vec<String>) -> Result<String> {
        self.apply_operation(
            PipelineOperation::DeduplicateByColumns { columns },
            "按指定字段组合去重，保留首条记录",
        )
    }

    pub fn rename_column(&mut self, from: &str, to: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::RenameColumn {
                from: from.trim().to_string(),
                to: to.trim().to_string(),
            },
            "手动重命名列",
        )
    }

    pub fn keep_columns(&mut self, columns: Vec<String>) -> Result<String> {
        self.apply_operation(PipelineOperation::KeepColumns { columns }, "保留指定列")
    }

    pub fn drop_columns(&mut self, columns: Vec<String>) -> Result<String> {
        self.apply_operation(PipelineOperation::DropColumns { columns }, "删除指定列")
    }

    pub fn drop_empty_columns(&mut self) -> Result<String> {
        self.apply_operation(PipelineOperation::DropEmptyColumns, "删除整列为空的字段")
    }

    pub fn reorder_columns(&mut self, columns: Vec<String>) -> Result<String> {
        self.apply_operation(
            PipelineOperation::ReorderColumns { columns },
            "按指定顺序重排字段，未列出的字段顺延到后面",
        )
    }

    pub fn add_column_name_affix(&mut self, prefix: &str, suffix: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::AddColumnNameAffix {
                prefix: prefix.to_string(),
                suffix: suffix.to_string(),
            },
            "批量为字段名添加前后缀",
        )
    }

    pub fn duplicate_column(&mut self, source: &str, target: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::DuplicateColumn {
                source: source.trim().to_string(),
                target: target.trim().to_string(),
            },
            "复制字段生成新列",
        )
    }

    pub fn merge_columns(&mut self, columns: Vec<String>, target: &str, separator: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::MergeColumns {
                columns,
                target: target.trim().to_string(),
                separator: separator.to_string(),
            },
            "将多个字段合并为一个文本列",
        )
    }

    pub fn add_row_number_column(&mut self, column: &str, start: usize) -> Result<String> {
        self.apply_operation(
            PipelineOperation::AddRowNumberColumn {
                column: column.trim().to_string(),
                start,
            },
            "新增行序号列",
        )
    }

    pub fn add_constant_column(&mut self, target: &str, value: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::AddConstantColumn {
                target: target.trim().to_string(),
                value: value.to_string(),
            },
            "新增整列相同的常量值字段",
        )
    }

    pub fn add_expression_column(&mut self, target: &str, expression: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::AddExpressionColumn {
                target: target.trim().to_string(),
                expression: expression.trim().to_string(),
            },
            "按表达式计算生成新列，支持 {列名} 引用",
        )
    }

    pub fn add_conditional_column(
        &mut self,
        target: &str,
        source_column: &str,
        operator: CompareOperator,
        compare_value: &str,
        true_value: &str,
        false_value: &str,
    ) -> Result<String> {
        self.apply_operation(
            PipelineOperation::AddConditionalColumn {
                target: target.trim().to_string(),
                source_column: source_column.trim().to_string(),
                operator,
                compare_value: compare_value.to_string(),
                true_value: true_value.to_string(),
                false_value: false_value.to_string(),
            },
            "根据字段条件生成判断列",
        )
    }

    pub fn concat_columns(&mut self, columns: Vec<String>, target: &str, separator: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::ConcatColumns {
                columns,
                target: target.trim().to_string(),
                separator: separator.to_string(),
            },
            "按顺序拼接多个字段生成新列",
        )
    }

    pub fn add_time_diff_column(
        &mut self,
        start_column: &str,
        end_column: &str,
        target: &str,
        unit: TimeDiffUnit,
    ) -> Result<String> {
        self.apply_operation(
            PipelineOperation::AddTimeDiffColumn {
                start_column: start_column.trim().to_string(),
                end_column: end_column.trim().to_string(),
                target: target.trim().to_string(),
                unit,
            },
            "根据开始时间和结束时间生成时间差列",
        )
    }

    pub fn sort_by(&mut self, column: &str, ascending: bool) -> Result<String> {
        self.apply_operation(
            PipelineOperation::SortBy {
                column: column.trim().to_string(),
                ascending,
            },
            "按指定列重新排序记录",
        )
    }

    pub fn multi_sort(&mut self, columns: Vec<String>, ascending: Vec<bool>) -> Result<String> {
        self.apply_operation(
            PipelineOperation::MultiSort { columns, ascending },
            "按多个字段依次排序当前数据集",
        )
    }

    pub fn priority_sort(
        &mut self,
        column: &str,
        operator: CompareOperator,
        value: &str,
        placement: PriorityPlacement,
        secondary_columns: Vec<String>,
        secondary_ascending: Vec<bool>,
    ) -> Result<String> {
        self.apply_operation(
            PipelineOperation::PrioritySort {
                column: column.trim().to_string(),
                operator,
                value: value.to_string(),
                placement,
                secondary_columns,
                secondary_ascending,
            },
            "将命中特定条件的记录优先置前或置后",
        )
    }

    pub fn add_rank_column(
        &mut self,
        target: &str,
        columns: Vec<String>,
        ascending: Vec<bool>,
    ) -> Result<String> {
        self.apply_operation(
            PipelineOperation::AddRankColumn {
                target: target.trim().to_string(),
                columns,
                ascending,
            },
            "按排序字段生成排名列",
        )
    }

    pub fn fill_null_text(&mut self, column: &str, value: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::FillNullText {
                column: column.trim().to_string(),
                value: value.to_string(),
            },
            "使用默认文本填充空值",
        )
    }

    pub fn fill_null_forward(&mut self, column: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::FillNullForward {
                column: column.trim().to_string(),
            },
            "使用前一个有效值向下填充空值",
        )
    }

    pub fn fill_null_backward(&mut self, column: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::FillNullBackward {
                column: column.trim().to_string(),
            },
            "使用后一个有效值向上回填空值",
        )
    }

    pub fn fill_null_statistic(&mut self, column: &str, strategy: StatisticFillStrategy) -> Result<String> {
        self.apply_operation(
            PipelineOperation::FillNullStatistic {
                column: column.trim().to_string(),
                strategy,
            },
            "使用统计值填充空值",
        )
    }

    pub fn empty_string_to_null(&mut self, column: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::EmptyStringToNull {
                column: column.trim().to_string(),
            },
            "将空字符串统一转为空值",
        )
    }

    pub fn zero_to_null(&mut self, column: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::ZeroToNull {
                column: column.trim().to_string(),
            },
            "将零值统一转为空值",
        )
    }

    pub fn replace_exact_value(&mut self, column: &str, from: &str, to: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::ReplaceExactValue {
                column: column.trim().to_string(),
                from: from.to_string(),
                to: to.to_string(),
            },
            "按完整值精确替换字段内容",
        )
    }

    pub fn convert_string_to_numeric(&mut self, column: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::ConvertStringToNumeric {
                column: column.trim().to_string(),
            },
            "将字符串字段转换为数值字段",
        )
    }

    pub fn convert_string_to_datetime(&mut self, column: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::ConvertStringToDateTime {
                column: column.trim().to_string(),
            },
            "将字符串字段转换为日期时间字段",
        )
    }

    pub fn convert_integer_to_float(&mut self, column: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::ConvertIntegerToFloat {
                column: column.trim().to_string(),
            },
            "将整数字段转换为浮点字段",
        )
    }

    pub fn convert_to_boolean(&mut self, column: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::ConvertToBoolean {
                column: column.trim().to_string(),
            },
            "将布尔语义字段统一转换为 true/false",
        )
    }

    pub fn cast_column(&mut self, column: &str, target: LogicalType) -> Result<String> {
        self.apply_operation(
            PipelineOperation::CastColumn {
                column: column.trim().to_string(),
                target,
            },
            "按目标逻辑类型重建字段解释方式",
        )
    }

    pub fn transform_text_case(&mut self, column: &str, mode: TextCaseMode) -> Result<String> {
        self.apply_operation(
            PipelineOperation::TransformTextCase {
                column: column.trim().to_string(),
                mode,
            },
            "统一文本大小写",
        )
    }

    pub fn replace_text(&mut self, column: &str, from: &str, to: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::ReplaceText {
                column: column.trim().to_string(),
                from: from.to_string(),
                to: to.to_string(),
            },
            "按字段执行文本替换",
        )
    }

    pub fn squeeze_text_whitespace(&mut self, column: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::SqueezeTextWhitespace {
                column: column.trim().to_string(),
            },
            "压缩文本中的连续空白",
        )
    }

    pub fn remove_text_pattern(&mut self, column: &str, pattern: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::RemoveTextPattern {
                column: column.trim().to_string(),
                pattern: pattern.to_string(),
            },
            "移除文本中的指定字符或片段",
        )
    }

    pub fn extract_text_before(&mut self, column: &str, delimiter: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::ExtractTextBefore {
                column: column.trim().to_string(),
                delimiter: delimiter.to_string(),
            },
            "提取分隔符左侧文本",
        )
    }

    pub fn extract_text_after(&mut self, column: &str, delimiter: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::ExtractTextAfter {
                column: column.trim().to_string(),
                delimiter: delimiter.to_string(),
            },
            "提取分隔符右侧文本",
        )
    }

    pub fn keep_digits_only(&mut self, column: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::KeepDigitsOnly {
                column: column.trim().to_string(),
            },
            "仅保留文本中的数字字符",
        )
    }

    pub fn add_text_affix(&mut self, column: &str, prefix: &str, suffix: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::AddTextAffix {
                column: column.trim().to_string(),
                prefix: prefix.to_string(),
                suffix: suffix.to_string(),
            },
            "为文本统一添加前后缀",
        )
    }

    pub fn truncate_text(&mut self, column: &str, max_chars: usize) -> Result<String> {
        self.apply_operation(
            PipelineOperation::TruncateText {
                column: column.trim().to_string(),
                max_chars,
            },
            "按指定长度截断文本",
        )
    }

    pub fn round_numeric(&mut self, column: &str, digits: usize) -> Result<String> {
        self.apply_operation(
            PipelineOperation::RoundNumeric {
                column: column.trim().to_string(),
                digits,
            },
            "统一数值保留小数位",
        )
    }

    pub fn scale_numeric(&mut self, column: &str, factor: f64) -> Result<String> {
        self.apply_operation(
            PipelineOperation::ScaleNumeric {
                column: column.trim().to_string(),
                factor,
            },
            "按系数缩放数值",
        )
    }

    pub fn shift_numeric(&mut self, column: &str, offset: f64) -> Result<String> {
        self.apply_operation(
            PipelineOperation::ShiftNumeric {
                column: column.trim().to_string(),
                offset,
            },
            "对数值整体加减偏移量",
        )
    }

    pub fn clamp_numeric(&mut self, column: &str, min: Option<f64>, max: Option<f64>) -> Result<String> {
        self.apply_operation(
            PipelineOperation::ClampNumeric {
                column: column.trim().to_string(),
                min,
                max,
            },
            "按上下界裁剪数值",
        )
    }

    pub fn normalize_datetime_format(&mut self, column: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::NormalizeDateTimeFormat {
                column: column.trim().to_string(),
            },
            "统一时间字段格式",
        )
    }

    pub fn convert_timestamp_to_datetime(&mut self, column: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::TimestampToDateTime {
                column: column.trim().to_string(),
            },
            "将时间戳字段转换为标准日期时间",
        )
    }

    pub fn shift_datetime_by_minutes(&mut self, column: &str, minutes: i64) -> Result<String> {
        self.apply_operation(
            PipelineOperation::ShiftDateTimeByMinutes {
                column: column.trim().to_string(),
                minutes,
            },
            "按分钟整体偏移时间字段",
        )
    }

    pub fn split_datetime_parts(&mut self, column: &str, prefix: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::SplitDateTimeParts {
                column: column.trim().to_string(),
                prefix: prefix.trim().to_string(),
            },
            "将时间字段拆分为年、月、日、时多个字段",
        )
    }

    pub fn extract_date_to_column(&mut self, column: &str, target: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::ExtractDateToColumn {
                column: column.trim().to_string(),
                target: target.trim().to_string(),
            },
            "从时间字段提取日期列",
        )
    }

    pub fn extract_year_to_column(&mut self, column: &str, target: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::ExtractYearToColumn {
                column: column.trim().to_string(),
                target: target.trim().to_string(),
            },
            "从时间字段提取年份列",
        )
    }

    pub fn extract_month_to_column(&mut self, column: &str, target: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::ExtractMonthToColumn {
                column: column.trim().to_string(),
                target: target.trim().to_string(),
            },
            "从时间字段提取月份列",
        )
    }

    pub fn extract_day_to_column(&mut self, column: &str, target: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::ExtractDayToColumn {
                column: column.trim().to_string(),
                target: target.trim().to_string(),
            },
            "从时间字段提取日列",
        )
    }

    pub fn extract_hour_to_column(&mut self, column: &str, target: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::ExtractHourToColumn {
                column: column.trim().to_string(),
                target: target.trim().to_string(),
            },
            "从时间字段提取小时列",
        )
    }

    pub fn filter_rows_by_time_window(&mut self, column: &str, start: &str, end: &str) -> Result<String> {
        self.apply_operation(
            PipelineOperation::FilterRowsByTimeWindow {
                column: column.trim().to_string(),
                start: start.trim().to_string(),
                end: end.trim().to_string(),
            },
            "按时间窗口筛选记录",
        )
    }

    pub fn sort_by_datetime(&mut self, column: &str, ascending: bool) -> Result<String> {
        self.apply_operation(
            PipelineOperation::SortByDateTime {
                column: column.trim().to_string(),
                ascending,
            },
            "按时间字段排序",
        )
    }

    pub fn update_quality_rules(
        &mut self,
        primary_key: &str,
        composite_keys: &str,
        time_column: &str,
        high_missing_threshold: &str,
        range_column: &str,
        range_min: &str,
        range_max: &str,
        length_column: &str,
        max_text_length: &str,
        time_gap_minutes: &str,
    ) -> Result<String> {
        let record = self.selected_dataset_mut()?;
        validate_optional_column(&record.working_table, primary_key)?;
        let composite_keys = split_csv_like(composite_keys);
        validate_column_list(&record.working_table, &composite_keys)?;
        validate_optional_column(&record.working_table, time_column)?;
        validate_optional_column(&record.working_table, range_column)?;
        validate_optional_column(&record.working_table, length_column)?;

        if !composite_keys.is_empty() && composite_keys.len() < 2 {
            bail!("组合键至少需要填写两个字段");
        }

        let high_missing_threshold =
            parse_threshold_percent(high_missing_threshold, record.quality_rules.normalized_threshold() * 100.0)?
                / 100.0;
        let range_min = parse_optional_f64(range_min, "范围最小值")?;
        let range_max = parse_optional_f64(range_max, "范围最大值")?;
        let max_text_length = parse_optional_usize(max_text_length, "最大长度")?;
        let time_gap_minutes = parse_optional_i64(time_gap_minutes, "时间间隔")?;

        let has_range_rule = !range_column.trim().is_empty() || range_min.is_some() || range_max.is_some();
        if has_range_rule {
            if range_column.trim().is_empty() {
                bail!("设置数值范围规则时必须指定字段");
            }
            if range_min.is_none() && range_max.is_none() {
                bail!("数值范围规则至少需要填写最小值或最大值");
            }
            if let (Some(min), Some(max)) = (range_min, range_max) {
                if min > max {
                    bail!("数值范围规则的最小值不能大于最大值");
                }
            }
        }

        let has_length_rule = !length_column.trim().is_empty() || max_text_length.is_some();
        if has_length_rule {
            if length_column.trim().is_empty() {
                bail!("设置文本长度规则时必须指定字段");
            }
            if max_text_length.is_none() {
                bail!("文本长度规则必须填写最大长度");
            }
        }

        if time_gap_minutes.is_some() && !record.working_table.columns.iter().any(|column| {
            column.name == time_column.trim()
                || (time_column.trim().is_empty()
                    && (column.logical_type == LogicalType::DateTime
                        || column.values.iter().flatten().any(|value| crate::model::looks_like_datetime(value))))
        }) {
            bail!("设置时间间隔规则前，请先指定可识别的时间字段");
        }

        record.quality_rules = QualityRules {
            primary_key: primary_key.trim().to_string(),
            composite_keys,
            time_column: time_column.trim().to_string(),
            high_missing_threshold,
            range_column: range_column.trim().to_string(),
            range_min,
            range_max,
            length_column: length_column.trim().to_string(),
            max_text_length,
            time_gap_minutes,
        };
        self.refresh_selected_record()?;

        let record = self.selected_dataset().context("当前没有选中数据集")?;
        Ok(format!(
            "质量规则已更新：主键 [{}]，组合键 [{}]，时间列 [{}]，缺失阈值 [{:.0}%]",
            display_optional_text(&record.quality_rules.primary_key),
            display_optional_vec(&record.quality_rules.composite_keys),
            display_optional_text(&record.quality_rules.time_column)
            ,
            record.quality_rules.normalized_threshold() * 100.0
        ))
    }

    pub fn group_aggregate(
        &mut self,
        group_columns: Vec<String>,
        target_column: &str,
        function: AggregateFunction,
    ) -> Result<String> {
        self.apply_operation(
            PipelineOperation::GroupAggregate {
                group_columns,
                target_column: target_column.trim().to_string(),
                function,
            },
            "按字段分组并生成聚合结果表",
        )
    }

    pub fn rolling_aggregate(
        &mut self,
        group_columns: Vec<String>,
        order_column: &str,
        target_column: &str,
        window_size: usize,
        function: AggregateFunction,
        output_column: &str,
    ) -> Result<String> {
        self.apply_operation(
            PipelineOperation::RollingAggregate {
                group_columns,
                order_column: order_column.trim().to_string(),
                target_column: target_column.trim().to_string(),
                window_size,
                function,
                output_column: output_column.trim().to_string(),
            },
            "按排序列执行滚动窗口统计并生成新列",
        )
    }

    pub fn cumulative_sum(
        &mut self,
        group_columns: Vec<String>,
        order_column: &str,
        target_column: &str,
        output_column: &str,
    ) -> Result<String> {
        self.apply_operation(
            PipelineOperation::CumulativeSum {
                group_columns,
                order_column: order_column.trim().to_string(),
                target_column: target_column.trim().to_string(),
                output_column: output_column.trim().to_string(),
            },
            "按排序列计算累积和并生成新列",
        )
    }

    pub fn moving_average(
        &mut self,
        group_columns: Vec<String>,
        order_column: &str,
        target_column: &str,
        window_size: usize,
        output_column: &str,
    ) -> Result<String> {
        self.apply_operation(
            PipelineOperation::MovingAverage {
                group_columns,
                order_column: order_column.trim().to_string(),
                target_column: target_column.trim().to_string(),
                window_size,
                output_column: output_column.trim().to_string(),
            },
            "按排序列计算滑动平均并生成新列",
        )
    }

    pub fn compare_adjacent(
        &mut self,
        group_columns: Vec<String>,
        order_column: &str,
        target_column: &str,
        mode: AdjacentCompareMode,
        output_column: &str,
    ) -> Result<String> {
        self.apply_operation(
            PipelineOperation::CompareAdjacent {
                group_columns,
                order_column: order_column.trim().to_string(),
                target_column: target_column.trim().to_string(),
                mode,
                output_column: output_column.trim().to_string(),
            },
            "按排序列比较相邻记录并生成派生列",
        )
    }

    pub fn apply_recommended_mapping(&mut self) -> Result<String> {
        let record = self.selected_dataset().context("当前没有选中数据集")?;
        let mappings = record
            .profile
            .mapping_suggestions
            .iter()
            .map(|entry| (entry.source_name.clone(), entry.target_name.clone()))
            .collect::<Vec<_>>();

        if mappings.is_empty() {
            bail!("当前数据集没有可应用的推荐映射")
        }

        self.apply_operation(
            PipelineOperation::ApplyMappings { mappings },
            "根据标准字段字典将别名列统一成工程标准名",
        )
    }

    pub fn run_multi_source_fusion(&mut self, request: FusionRequest) -> Result<String> {
        let primary = self.selected_dataset().context("当前没有选中主源数据集")?.clone();
        let mut secondary_ids = request.secondary_dataset_ids.clone();
        secondary_ids.retain(|dataset_id| *dataset_id != primary.id);
        secondary_ids.sort_unstable();
        secondary_ids.dedup();
        if secondary_ids.is_empty() {
            bail!("请至少选择一个有效辅源数据集");
        }

        let secondary_sources = self
            .datasets
            .iter()
            .filter(|record| secondary_ids.contains(&record.id))
            .map(|record| fusion::SourceBundle {
                dataset_name: &record.dataset_name,
                table: &record.working_table,
                profile: &record.profile,
            })
            .collect::<Vec<_>>();
        if secondary_sources.is_empty() {
            bail!("未找到要参与融合的辅源数据集");
        }

        let execution = fusion::execute(
            &request,
            fusion::SourceBundle {
                dataset_name: &primary.dataset_name,
                table: &primary.working_table,
                profile: &primary.profile,
            },
            &secondary_sources,
        )?;

        let secondary_names = secondary_sources
            .iter()
            .map(|source| source.dataset_name.to_string())
            .collect::<Vec<_>>();
        self.invalidate_runtime_reports();

        let fusion_base_name = format!("{}_多源融合", primary.dataset_name);
        let unified_name = format!("{fusion_base_name}_融合结果");
        let unified_id = self.push_generated_dataset(
            unified_name.clone(),
            primary.source_path.clone(),
            execution.unified_table,
            format!("主源 {} + 辅源 {}", primary.dataset_name, secondary_names.join(", ")),
        )?;

        self.selected_dataset_id = Some(unified_id);
        self.last_fusion_report = Some(FusionRunReport {
            source_summary: execution.report.source_summary,
            alignment_summary: execution.report.alignment_summary,
            quality_summary: execution.report.quality_summary,
            output_summary: format!(
                "{} | 新数据集：{}",
                execution.report.output_summary,
                unified_name
            ),
            trace_summary: execution.report.trace_summary,
            skipped_sources: execution.report.skipped_sources,
        });

        Ok(format!(
            "已完成多源融合：命中 {} / {} 行，平均质量分 {:.1}",
            execution.report.matched_rows,
            execution.report.total_rows,
            execution.report.average_quality_score
        ))
    }

    pub fn join_selected_with(
        &mut self,
        right_dataset_id: i32,
        left_keys: Vec<String>,
        right_keys: Vec<String>,
        join_kind: JoinKind,
        conflict_strategy: JoinConflictStrategy,
    ) -> Result<String> {
        let left_id = self.selected_dataset_id.context("当前没有选中数据集")?;
        if left_id == right_dataset_id {
            bail!("不能将数据集与自身融合");
        }

        let left = self
            .datasets
            .iter()
            .find(|record| record.id == left_id)
            .cloned()
            .context("左表不存在")?;
        let right = self
            .datasets
            .iter()
            .find(|record| record.id == right_dataset_id)
            .cloned()
            .context("右表不存在")?;

        let join_result = processor::join_tables(
            &left.working_table,
            &right.working_table,
            &left_keys,
            &right_keys,
            join_kind.clone(),
            conflict_strategy.clone(),
        )?;
        let processor::JoinExecution {
            table: joined,
            matched_rows,
            unmatched_left,
            unmatched_right,
            conflict_fields,
        } = join_result;

        let frame = joined.to_frame()?;
        let quality_rules = QualityRules::default();
        let profile = inspector::build_profile(&joined, &quality_rules);
        let dataset_id = self.next_dataset_id;
        self.next_dataset_id += 1;

        self.datasets.push(DatasetRecord {
            id: dataset_id,
            dataset_name: format!("{}_merge_{}", left.dataset_name, right.dataset_name),
            source_path: left.source_path.clone(),
            format: FileFormat::Csv,
            size_bytes: 0,
            imported_at: Local::now(),
            import_duration_ms: None,
            sheet_name: None,
            source_table: joined.clone(),
            working_table: joined,
            frame,
            quality_rules,
            profile,
            pipeline_steps: vec![PipelineStep {
                timestamp: Local::now(),
                action: format!("多表融合 {}", join_kind.as_str()),
                detail: format!(
                    "{}({}) + {}({})",
                    left.dataset_name,
                    left_keys.join(", "),
                    right.dataset_name,
                    right_keys.join(", ")
                ),
                outcome: "生成新的融合结果数据集".to_string(),
                operation: None,
            }],
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
        });

        let data_loss_hint = match join_kind {
            JoinKind::Left => {
                if unmatched_right > 0 {
                    format!("左连接未保留右表未匹配记录 {} 条", unmatched_right)
                } else {
                    "左连接未发现右表未匹配记录".to_string()
                }
            }
            JoinKind::Inner => {
                if unmatched_left > 0 || unmatched_right > 0 {
                    format!(
                        "内连接丢弃了未匹配记录：左表 {} 条，右表 {} 条",
                        unmatched_left, unmatched_right
                    )
                } else {
                    "内连接全部记录均成功匹配".to_string()
                }
            }
            JoinKind::Outer => {
                if unmatched_left > 0 || unmatched_right > 0 {
                    "外连接已保留未匹配记录，结果中可能出现空值列".to_string()
                } else {
                    "外连接未发现未匹配记录".to_string()
                }
            }
        };
        self.last_join_report = Some(JoinReport {
            matched_rows,
            unmatched_left,
            unmatched_right,
            conflict_fields,
            conflict_strategy: conflict_strategy.as_str().to_string(),
            data_loss_hint,
        });

        self.selected_dataset_id = Some(dataset_id);
        self.invalidate_visualization_preview();
        Ok(format!(
            "已生成融合数据集：左表 {} + 右表 {}",
            left.dataset_name, right.dataset_name
        ))
    }

    pub fn export_selected_csv(&self, path: &Path) -> Result<String> {
        let record = self.selected_dataset().context("当前没有选中数据集")?;
        exporter::export_dataset_csv(record, path)?;
        Ok(format!("已导出 CSV：{}", path.display()))
    }

    pub fn export_selected_tsv(&self, path: &Path) -> Result<String> {
        let record = self.selected_dataset().context("当前没有选中数据集")?;
        exporter::export_dataset_tsv(record, path)?;
        Ok(format!("已导出 TSV：{}", path.display()))
    }

    pub fn export_selected_txt(&self, path: &Path) -> Result<String> {
        let record = self.selected_dataset().context("当前没有选中数据集")?;
        exporter::export_dataset_txt(record, path)?;
        Ok(format!("已导出 TXT：{}", path.display()))
    }

    pub fn export_quality_report(&self, path: &Path) -> Result<String> {
        let record = self.selected_dataset().context("当前没有选中数据集")?;
        exporter::export_quality_report(record, path)?;
        Ok(format!("已导出质量报告：{}", path.display()))
    }

    pub fn save_pipeline_template(&self, path: &Path) -> Result<String> {
        let record = self.selected_dataset().context("当前没有选中数据集")?;
        let operations = record
            .pipeline_steps
            .iter()
            .filter_map(|step| step.operation.clone())
            .collect::<Vec<_>>();
        let template = pipeline::template_from_operations(record.dataset_name.clone(), operations);
        exporter::export_pipeline_template(&template, path)?;
        Ok(format!("已保存流程模板：{}", path.display()))
    }

    pub fn replay_pipeline_template(&mut self, path: &Path) -> Result<String> {
        let template = exporter::import_pipeline_template(path)?;
        let record = self.selected_dataset_mut()?;
        record.working_table = record.source_table.clone();
        record.pipeline_steps.clear();
        self.refresh_selected_record()?;
        self.invalidate_visualization_preview();

        for operation in template.operations {
            let description = operation.to_string();
            self.apply_operation(operation, &description)?;
        }

        Ok(format!("已回放流程模板：{}", path.display()))
    }

    pub fn join_suggestions(&self) -> Vec<crate::model::JoinSuggestion> {
        let Some(selected) = self.selected_dataset() else {
            return Vec::new();
        };

        let others = self
            .datasets
            .iter()
            .map(|record| (record.dataset_name.as_str(), &record.working_table))
            .collect::<Vec<_>>();

        inspector::build_join_suggestions(
            &selected.dataset_name,
            &selected.working_table,
            &others,
        )
    }

    fn selected_dataset_mut(&mut self) -> Result<&mut DatasetRecord> {
        let id = self.selected_dataset_id.context("当前没有选中数据集")?;
        self.datasets
            .iter_mut()
            .find(|record| record.id == id)
            .context("数据集不存在")
    }

    fn push_dataset(&mut self, dataset: LoadedDataset) -> Result<i32> {
        let frame = dataset.table.to_frame()?;
        let quality_rules = QualityRules::default();
        let profile = inspector::build_profile(&dataset.table, &quality_rules);
        let dataset_id = self.next_dataset_id;
        self.next_dataset_id += 1;

        self.datasets.push(DatasetRecord {
            id: dataset_id,
            dataset_name: dataset.dataset_name,
            source_path: dataset.source_path,
            format: dataset.format,
            size_bytes: dataset.size_bytes,
            imported_at: dataset.imported_at,
            import_duration_ms: dataset.import_duration_ms,
            sheet_name: dataset.sheet_name,
            source_table: dataset.table.clone(),
            working_table: dataset.table,
            frame,
            quality_rules,
            profile,
            pipeline_steps: Vec::new(),
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
        });

        Ok(dataset_id)
    }

    fn push_generated_dataset(
        &mut self,
        dataset_name: String,
        source_path: std::path::PathBuf,
        table: crate::model::DataTable,
        detail: String,
    ) -> Result<i32> {
        let frame = table.to_frame()?;
        let quality_rules = QualityRules::default();
        let profile = inspector::build_profile(&table, &quality_rules);
        let dataset_id = self.next_dataset_id;
        self.next_dataset_id += 1;

        self.datasets.push(DatasetRecord {
            id: dataset_id,
            dataset_name,
            source_path,
            format: FileFormat::Csv,
            size_bytes: 0,
            imported_at: Local::now(),
            import_duration_ms: None,
            sheet_name: None,
            source_table: table.clone(),
            working_table: table,
            frame,
            quality_rules,
            profile,
            pipeline_steps: vec![PipelineStep {
                timestamp: Local::now(),
                action: "多源数据融合".to_string(),
                detail,
                outcome: "生成新的融合结果数据集".to_string(),
                operation: None,
            }],
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
        });

        Ok(dataset_id)
    }

    fn apply_operation(&mut self, operation: PipelineOperation, detail: &str) -> Result<String> {
        let record = self.selected_dataset_mut()?;
        let before_rows = record.working_table.height();
        let before_cols = record.working_table.width();
        let snapshot = Self::capture_history(record);
        let next = processor::apply_operation(&record.working_table, &operation)?;
        record.undo_stack.push(snapshot);
        record.redo_stack.clear();
        record.working_table = next;
        self.refresh_selected_record()?;
        self.invalidate_visualization_preview();

        let record = self.selected_dataset_mut()?;
        record.pipeline_steps.push(pipeline::build_step(
            &operation,
            detail.to_string(),
            format!(
                "行列变化：{}x{} -> {}x{}",
                before_rows,
                before_cols,
                record.working_table.height(),
                record.working_table.width()
            ),
        ));

        Ok(format!(
            "{} 完成，当前工作表 {} 行 {} 列",
            operation,
            record.working_table.height(),
            record.working_table.width()
        ))
    }

    fn refresh_selected_record(&mut self) -> Result<()> {
        let record = self.selected_dataset_mut()?;
        record.frame = record.working_table.to_frame()?;
        record.profile = inspector::build_profile(&record.working_table, &record.quality_rules);
        Ok(())
    }

    fn capture_history(record: &DatasetRecord) -> DatasetHistory {
        DatasetHistory {
            working_table: record.working_table.clone(),
            pipeline_steps: record.pipeline_steps.clone(),
        }
    }

    fn restore_history(record: &mut DatasetRecord, history: DatasetHistory) -> Result<()> {
        record.working_table = history.working_table;
        record.pipeline_steps = history.pipeline_steps;
        record.frame = record.working_table.to_frame()?;
        record.profile = inspector::build_profile(&record.working_table, &record.quality_rules);
        Ok(())
    }

    fn invalidate_runtime_reports(&mut self) {
        self.last_fusion_report = None;
        self.last_visualization_preview = None;
    }

    fn invalidate_visualization_preview(&mut self) {
        self.last_fusion_report = None;
        self.last_visualization_preview = None;
    }
}

fn validate_optional_column(table: &crate::model::DataTable, column_name: &str) -> Result<()> {
    let column_name = column_name.trim();
    if column_name.is_empty() {
        return Ok(());
    }

    if table.columns.iter().any(|column| column.name == column_name) {
        Ok(())
    } else {
        bail!("未找到字段: {column_name}")
    }
}

fn validate_column_list(table: &crate::model::DataTable, column_names: &[String]) -> Result<()> {
    for column_name in column_names {
        validate_optional_column(table, column_name)?;
    }
    Ok(())
}

fn display_optional_text(value: &str) -> &str {
    if value.trim().is_empty() {
        "未设置"
    } else {
        value
    }
}

fn display_optional_vec(values: &[String]) -> String {
    if values.is_empty() {
        "未设置".to_string()
    } else {
        values.join(", ")
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

fn parse_threshold_percent(value: &str, default_percent: f32) -> Result<f32> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(default_percent);
    }

    let parsed = trimmed
        .parse::<f32>()
        .with_context(|| format!("缺失阈值解析失败: {trimmed}"))?;
    if !(5.0..=95.0).contains(&parsed) {
        bail!("缺失阈值必须在 5 到 95 之间");
    }
    Ok(parsed)
}

fn parse_optional_f64(value: &str, label: &str) -> Result<Option<f64>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let parsed = trimmed
        .parse::<f64>()
        .with_context(|| format!("{label} 解析失败: {trimmed}"))?;
    Ok(Some(parsed))
}

fn parse_optional_usize(value: &str, label: &str) -> Result<Option<usize>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let parsed = trimmed
        .parse::<usize>()
        .with_context(|| format!("{label} 解析失败: {trimmed}"))?;
    if parsed == 0 {
        bail!("{label} 必须大于 0");
    }
    Ok(Some(parsed))
}

fn parse_optional_i64(value: &str, label: &str) -> Result<Option<i64>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let parsed = trimmed
        .parse::<i64>()
        .with_context(|| format!("{label} 解析失败: {trimmed}"))?;
    if parsed <= 0 {
        bail!("{label} 必须大于 0");
    }
    Ok(Some(parsed))
}
