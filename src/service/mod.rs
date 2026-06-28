use crate::exporter;
use crate::inspector;
use crate::loader;
use crate::model::{
    AggregateFunction, DatasetHistory, DatasetRecord, DatasetSnapshot, FileFormat, JoinKind,
    LoadedDataset, LogicalType, PipelineOperation, PipelineStep, QualityRules, TextCaseMode,
    format_bytes,
};
use crate::pipeline;
use crate::processor;
use anyhow::{Context, Result, bail};
use chrono::Local;
use std::path::Path;

pub struct AppService {
    next_dataset_id: i32,
    datasets: Vec<DatasetRecord>,
    pub selected_dataset_id: Option<i32>,
}

impl AppService {
    pub fn new() -> Self {
        Self {
            next_dataset_id: 1,
            datasets: Vec::new(),
            selected_dataset_id: None,
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

        Ok(format!("已删除数据集：{}", removed.dataset_name))
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
        Ok(format!(
            "已撤销，当前工作表 {} 行 {} 列",
            record.working_table.height(),
            record.working_table.width()
        ))
    }

    pub fn redo(&mut self) -> Result<String> {
        let record = self.selected_dataset_mut()?;
        let Some(next) = record.redo_stack.pop() else {
            bail!("当前数据集没有可重做的操作");
        };

        record.undo_stack.push(Self::capture_history(record));
        Self::restore_history(record, next)?;
        Ok(format!(
            "已重做，当前工作表 {} 行 {} 列",
            record.working_table.height(),
            record.working_table.width()
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

    pub fn drop_rows_with_missing(&mut self, columns: Vec<String>) -> Result<String> {
        self.apply_operation(
            PipelineOperation::DropRowsWithMissing { columns },
            "删除指定字段中存在缺失值的记录",
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

    pub fn sort_by(&mut self, column: &str, ascending: bool) -> Result<String> {
        self.apply_operation(
            PipelineOperation::SortBy {
                column: column.trim().to_string(),
                ascending,
            },
            "按指定列重新排序记录",
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

    pub fn round_numeric(&mut self, column: &str, digits: usize) -> Result<String> {
        self.apply_operation(
            PipelineOperation::RoundNumeric {
                column: column.trim().to_string(),
                digits,
            },
            "统一数值保留小数位",
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

    pub fn join_selected_with(
        &mut self,
        right_dataset_id: i32,
        left_keys: Vec<String>,
        right_keys: Vec<String>,
        join_kind: JoinKind,
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

        let joined = processor::join_tables(
            &left.working_table,
            &right.working_table,
            &left_keys,
            &right_keys,
            join_kind.clone(),
        )?;

        let frame = joined.to_frame()?;
        let quality_rules = QualityRules::default();
        let profile = inspector::build_profile(&joined, &quality_rules);
        let dataset_id = self.next_dataset_id;
        self.next_dataset_id += 1;

        self.datasets.push(DatasetRecord {
            id: dataset_id,
            dataset_name: format!("{}_merge_{}", left.dataset_name, right.dataset_name),
            source_path: left.source_path.clone(),
            format: FileFormat::Json,
            size_bytes: 0,
            imported_at: Local::now(),
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

        self.selected_dataset_id = Some(dataset_id);
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

    pub fn export_selected_json(&self, path: &Path) -> Result<String> {
        let record = self.selected_dataset().context("当前没有选中数据集")?;
        exporter::export_dataset_json(record, path)?;
        Ok(format!("已导出 JSON：{}", path.display()))
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
