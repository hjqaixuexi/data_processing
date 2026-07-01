use crate::model::{DataTable, DatasetProfile, LogicalType, TableColumn, infer_logical_type};
use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[allow(dead_code)]
const DEFAULT_WINDOW_SECONDS: i64 = 5;
#[allow(dead_code)]
const DEFAULT_RESAMPLE_SECONDS: i64 = 60;
#[allow(dead_code)]
const MAX_FEATURE_COLUMNS: usize = 12;
#[derive(Clone, Debug)]
pub enum FusionAlignmentMode {
    ExactTime,
    ExactThenNearest,
    NearestWithinWindow,
    ExactThenWindow,
    WindowAggregation,
    ResampleNearest,
}

impl FusionAlignmentMode {
    pub fn from_text(value: &str) -> Self {
        match value.trim() {
            "精确优先，最近邻兜底" => Self::ExactThenNearest,
            "按时间窗聚合" => Self::WindowAggregation,
            "精确优先，时间窗聚合兜底" => Self::ExactThenWindow,
            "按重采样对齐" => Self::ResampleNearest,
            "按最近邻对齐" => Self::NearestWithinWindow,
            _ => Self::ExactTime,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ExactTime => "按时间精确对齐",
            Self::ExactThenNearest => "精确优先，最近邻兜底",
            Self::NearestWithinWindow => "按最近邻对齐",
            Self::ExactThenWindow => "精确优先，时间窗聚合兜底",
            Self::WindowAggregation => "按时间窗聚合",
            Self::ResampleNearest => "按重采样对齐",
        }
    }
}

#[derive(Clone, Debug)]
pub enum FusionMissingStrategy {
    KeepNull,
    ForwardFill,
    BackwardFill,
    NearestFill,
    LinearInterpolate,
    WindowMean,
}

impl FusionMissingStrategy {
    pub fn from_text(value: &str) -> Self {
        match value.trim() {
            "前向填充" => Self::ForwardFill,
            "后向填充" => Self::BackwardFill,
            "前后就近填充" => Self::NearestFill,
            "线性插值" => Self::LinearInterpolate,
            "窗口均值" => Self::WindowMean,
            _ => Self::KeepNull,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::KeepNull => "保持空值并标记",
            Self::ForwardFill => "前向填充",
            Self::BackwardFill => "后向填充",
            Self::NearestFill => "前后就近填充",
            Self::LinearInterpolate => "线性插值",
            Self::WindowMean => "窗口均值",
        }
    }
}

#[derive(Clone, Debug)]
pub enum FusionStrategy {
    PrimaryFirst,
    SecondaryFirst,
    ConfidenceWeighted,
    NumericAverage,
    ComplementaryFill,
    ConflictRetention,
}

impl FusionStrategy {
    pub fn from_text(value: &str) -> Self {
        match value.trim() {
            "辅源优先" => Self::SecondaryFirst,
            "质量加权" => Self::ConfidenceWeighted,
            "数值均值" => Self::NumericAverage,
            "互补填充" => Self::ComplementaryFill,
            "冲突保留" => Self::ConflictRetention,
            _ => Self::PrimaryFirst,
        }
    }

}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct FusionRequest {
    pub secondary_dataset_ids: Vec<i32>,
    pub object_keys: Vec<String>,
    pub time_column: String,
    pub alignment_mode: FusionAlignmentMode,
    pub time_window_seconds: i64,
    pub resample_seconds: i64,
    pub missing_strategy: FusionMissingStrategy,
    pub fusion_strategy: FusionStrategy,
    pub deduplicate_packets: bool,
    pub clean_outliers: bool,
    pub score_quality: bool,
    pub generate_features: bool,
    pub generate_events: bool,
    pub generate_alerts: bool,
    pub outlier_zscore: f64,
    pub alert_threshold: f64,
}

#[derive(Clone, Debug)]
pub struct FusionReport {
    pub source_summary: String,
    pub alignment_summary: String,
    pub quality_summary: String,
    pub output_summary: String,
    pub trace_summary: String,
    pub matched_rows: usize,
    pub total_rows: usize,
    pub average_quality_score: f64,
    pub skipped_sources: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct FusionExecution {
    pub unified_table: DataTable,
    pub report: FusionReport,
}

#[derive(Clone, Copy)]
pub struct SourceBundle<'a> {
    pub dataset_name: &'a str,
    pub table: &'a DataTable,
    pub profile: &'a DatasetProfile,
}

#[derive(Clone, Debug)]
struct PreparedRow {
    object_key: String,
    timestamp: Option<NaiveDateTime>,
    cells: Vec<Option<String>>,
    anomaly_cells: usize,
}

#[derive(Clone, Debug)]
struct OutputBinding {
    source_index: usize,
    output_name: String,
    logical_type: LogicalType,
    overlaps_primary: Option<usize>,
}

#[derive(Clone, Debug)]
struct PreparedSource {
    dataset_name: String,
    trace_label: String,
    rows_by_key: BTreeMap<String, Vec<PreparedRow>>,
    bindings: Vec<OutputBinding>,
    confidence: f64,
    deduplicated_rows: usize,
}

#[derive(Clone, Debug)]
struct MatchResult {
    cells: Vec<Option<String>>,
    time_distance_seconds: Option<i64>,
    anomaly_cells: usize,
    confidence_weight: f64,
    trace: String,
}

#[derive(Clone, Debug)]
struct FusionAnchorRow {
    object_key: String,
    timestamp: Option<NaiveDateTime>,
    cells: Vec<Option<String>>,
    trace_parts: Vec<String>,
    corrections: BTreeSet<String>,
    quality_score: f64,
}

pub fn execute(
    request: &FusionRequest,
    primary: SourceBundle<'_>,
    secondary_sources: &[SourceBundle<'_>],
) -> Result<FusionExecution> {
    if secondary_sources.is_empty() {
        bail!("至少需要一个辅源数据集");
    }

    let object_keys = resolve_required_keys(primary.table, &request.object_keys)
        .context("主源对象键配置无效")?;
    let time_column = resolve_required_time(primary.table, &request.time_column)
        .context("主源时间列配置无效")?;

    let primary_prepared = prepare_primary_source(
        primary,
        &object_keys,
        &time_column,
        request.deduplicate_packets,
        request.clean_outliers,
        request.outlier_zscore,
    )?;

    let primary_name_map = primary
        .table
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| (normalize_name(&column.name), index))
        .collect::<HashMap<_, _>>();

    let mut secondary_prepared = Vec::new();
    let mut skipped_sources = Vec::new();
    for source in secondary_sources {
        match prepare_secondary_source(
            *source,
            &object_keys,
            &time_column,
            &primary_name_map,
            request,
        ) {
            Ok(mut prepared) => {
                prepared.trace_label = format!("辅源{}", secondary_prepared.len() + 1);
                secondary_prepared.push(prepared);
            }
            Err(error) => {
                skipped_sources.push(format!("{}: {error}", source.dataset_name));
            }
        }
    }

    if secondary_prepared.is_empty() {
        bail!("所有辅源都无法映射到主源，请检查对象键和时间字段");
    }

    let quality_field_count = secondary_prepared
        .iter()
        .map(|source| source.bindings.len())
        .sum::<usize>();
    let (headers, logical_types) = build_output_schema(primary.table, &mut secondary_prepared);
    let data_width = headers.len();
    let primary_width = primary.table.width();

    let mut anchor_rows = Vec::with_capacity(primary_prepared.len());
    let mut matched_rows = 0usize;
    let mut total_quality = 0.0;
    let mut total_time_distance = 0i64;
    let mut time_distance_count = 0usize;
    let mut total_missing_fields = 0usize;
    let mut total_anomaly_cells = 0usize;

    for primary_row in &primary_prepared {
        let mut row = primary_row.cells.clone();
        let mut primary_weights = row
            .iter()
            .map(|value| if is_non_empty(value.as_deref()) { 1.0 } else { 0.0 })
            .collect::<Vec<_>>();
        let mut raw_secondary_cells = Vec::new();
        let mut matched_source_count = 0usize;
        let mut trace_parts = Vec::new();
        let mut corrections = BTreeSet::new();
        let mut missing_fields = 0usize;
        let mut anomaly_cells = primary_row.anomaly_cells;

        for source in &secondary_prepared {
            let match_result = select_secondary_match(source, primary_row, request)?;
            if let Some(found) = match_result {
                matched_source_count += 1;
                trace_parts.push(found.trace.clone());
                if found.anomaly_cells > 0 {
                    corrections.insert("异常值剔除".to_string());
                }
                if let Some(distance) = found.time_distance_seconds {
                    total_time_distance += distance.abs();
                    time_distance_count += 1;
                }
                anomaly_cells += found.anomaly_cells;

                for binding in &source.bindings {
                    let cell = found.cells.get(binding.source_index).cloned().unwrap_or(None);
                    if cell.is_none() {
                        missing_fields += 1;
                    }
                    if let Some(primary_index) = binding.overlaps_primary {
                        apply_fusion_strategy(
                            &mut row[primary_index],
                            &cell,
                            &mut primary_weights[primary_index],
                            found.confidence_weight,
                            &request.fusion_strategy,
                        );
                    } else {
                        raw_secondary_cells.push(cell);
                    }
                }
            } else {
                missing_fields += source.bindings.len();
                raw_secondary_cells.extend(
                    source
                        .bindings
                        .iter()
                        .filter(|binding| binding.overlaps_primary.is_none())
                        .map(|_| None),
                );
                trace_parts.push(format!("{}: 未命中", source.trace_label));
            }
        }

        if matched_source_count > 0 {
            matched_rows += 1;
        }

        let quality_score = if request.score_quality {
            compute_quality_score(
                matched_source_count,
                secondary_prepared.len(),
                quality_field_count,
                missing_fields,
                anomaly_cells,
                &trace_parts,
            )
        } else {
            100.0
        };
        total_quality += quality_score;
        total_missing_fields += missing_fields;
        total_anomaly_cells += anomaly_cells;

        row.extend(raw_secondary_cells);
        debug_assert_eq!(row.len(), data_width);
        anchor_rows.push(FusionAnchorRow {
            object_key: primary_row.object_key.clone(),
            timestamp: primary_row.timestamp,
            cells: row,
            trace_parts,
            corrections,
            quality_score,
        });
    }

    apply_missing_strategy(
        &mut anchor_rows,
        &request.missing_strategy,
        primary_width,
        &logical_types,
    );

    let mut final_headers = headers;
    final_headers.extend([
        "fusion_quality_score".to_string(),
        "fusion_trace".to_string(),
        "fusion_corrections".to_string(),
    ]);
    let mut final_types = logical_types;
    final_types.extend([
        LogicalType::Float,
        LogicalType::Text,
        LogicalType::Text,
    ]);

    let final_rows = anchor_rows
        .iter_mut()
        .map(|row| {
            let mut cells = row.cells.clone();
            cells.extend([
                Some(format_number(row.quality_score)),
                Some(join_or_default(&row.trace_parts, "无融合轨迹")),
                Some(join_or_default(
                    &row.corrections.iter().cloned().collect::<Vec<_>>(),
                    request.missing_strategy.as_str(),
                )),
            ]);
            cells
        })
        .collect::<Vec<_>>();

    let unified_table = build_table_from_rows(final_headers, final_types, final_rows);
    let average_quality_score = if anchor_rows.is_empty() {
        0.0
    } else {
        total_quality / anchor_rows.len() as f64
    };
    let average_time_distance = if time_distance_count == 0 {
        0.0
    } else {
        total_time_distance as f64 / time_distance_count as f64
    };

    let output_summary = format!(
        "融合结果表 {} 行 x {} 列",
        unified_table.height(),
        unified_table.width()
    );

    let report = FusionReport {
        source_summary: format!(
            "主源 {}，辅源 {} 个：{}",
            primary.dataset_name,
            secondary_prepared.len(),
            secondary_prepared
                .iter()
                .map(|source| source.dataset_name.clone())
                .collect::<Vec<_>>()
                .join("、")
        ),
        alignment_summary: format!(
            "{} | 对象键 [{}] | 时间列 [{}] | 命中 {} / {} 行 | 平均时间偏差 {:.1}s",
            request.alignment_mode.as_str(),
            object_keys.join(", "),
            time_column,
            matched_rows,
            primary_prepared.len(),
            average_time_distance
        ),
        quality_summary: format!(
            "{} | 去重 {} 行 | 异常剔除 {} 个单元 | 缺失填补 {} | 平均质量分 {:.1}",
            if request.score_quality { "已启用质量评分" } else { "质量评分关闭" },
            secondary_prepared.iter().map(|source| source.deduplicated_rows).sum::<usize>(),
            total_anomaly_cells,
            request.missing_strategy.as_str(),
            average_quality_score
        ),
        output_summary,
        trace_summary: format!(
            "保留 3 个追踪字段；辅源平均缺失补位 {:.1} 个字段；未映射辅源 {} 个",
            if primary_prepared.is_empty() {
                0.0
            } else {
                total_missing_fields as f64 / primary_prepared.len() as f64
            },
            skipped_sources.len()
        ),
        matched_rows,
        total_rows: primary_prepared.len(),
        average_quality_score,
        skipped_sources,
    };

    Ok(FusionExecution {
        unified_table,
        report,
    })
}

fn prepare_primary_source(
    source: SourceBundle<'_>,
    object_keys: &[String],
    time_column: &str,
    deduplicate_packets: bool,
    clean_outliers: bool,
    outlier_zscore: f64,
) -> Result<Vec<PreparedRow>> {
    let object_indexes = find_indexes(source.table, object_keys)?;
    let time_index = find_index(source.table, time_column)?;
    let mut rows = table_rows(source.table)
        .into_iter()
        .map(|cells| PreparedRow {
            object_key: compose_key(&cells, &object_indexes),
            timestamp: parse_datetime_cell(cells.get(time_index).and_then(|cell| cell.as_ref())),
            cells,
            anomaly_cells: 0,
        })
        .collect::<Vec<_>>();

    if deduplicate_packets {
        rows = deduplicate_prepared_rows(rows);
    }
    sort_prepared_rows(&mut rows);
    if clean_outliers {
        clean_outliers_in_rows(&mut rows, source.table, outlier_zscore);
    }
    Ok(rows)
}

fn prepare_secondary_source(
    source: SourceBundle<'_>,
    primary_object_keys: &[String],
    primary_time_column: &str,
    primary_name_map: &HashMap<String, usize>,
    request: &FusionRequest,
) -> Result<PreparedSource> {
    let mapped_object_keys = map_secondary_keys(source.table, primary_object_keys)?;
    let time_column = map_secondary_time_column(source.table, primary_time_column)?;
    let object_indexes = find_indexes(source.table, &mapped_object_keys)?;
    let time_index = find_index(source.table, &time_column)?;

    let mut rows = table_rows(source.table)
        .into_iter()
        .map(|cells| PreparedRow {
            object_key: compose_key(&cells, &object_indexes),
            timestamp: parse_datetime_cell(cells.get(time_index).and_then(|cell| cell.as_ref())),
            cells,
            anomaly_cells: 0,
        })
        .collect::<Vec<_>>();

    let original_len = rows.len();
    if request.deduplicate_packets {
        rows = deduplicate_prepared_rows(rows);
    }
    sort_prepared_rows(&mut rows);
    if request.clean_outliers {
        clean_outliers_in_rows(&mut rows, source.table, request.outlier_zscore);
    }
    apply_missing_strategy_to_rows(&mut rows, source.table, &request.missing_strategy, primary_object_keys.len());

    let rows_by_key = rows.into_iter().fold(BTreeMap::new(), |mut acc, row| {
        acc.entry(row.object_key.clone()).or_insert_with(Vec::new).push(row);
        acc
    });
    let deduplicated_rows = original_len.saturating_sub(
        rows_by_key.values().map(|group| group.len()).sum::<usize>(),
    );
    let bindings = build_output_bindings(source, &mapped_object_keys, &time_column, primary_name_map);
    if bindings.is_empty() {
        bail!("没有可供融合的有效字段");
    }

    let missing_ratio = compute_missing_ratio(source.table);
    Ok(PreparedSource {
        dataset_name: source.dataset_name.to_string(),
        trace_label: String::new(),
        rows_by_key,
        bindings,
        confidence: source_confidence(source, missing_ratio),
        deduplicated_rows,
    })
}

fn apply_missing_strategy_to_rows(
    rows: &mut [PreparedRow],
    table: &DataTable,
    strategy: &FusionMissingStrategy,
    object_key_width: usize,
) {
    if rows.is_empty() || matches!(strategy, FusionMissingStrategy::KeepNull) {
        return;
    }

    let numeric_indexes = table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| matches!(column.logical_type, LogicalType::Integer | LogicalType::Float))
        .map(|(index, _)| index)
        .collect::<BTreeSet<_>>();
    let fillable_indexes = (0..table.width())
        .filter(|index| *index >= object_key_width)
        .collect::<Vec<_>>();
    let groups = group_row_indexes(rows);

    for indexes in groups.values() {
        match strategy {
            FusionMissingStrategy::ForwardFill => {
                for column_index in &fillable_indexes {
                    let mut last = None::<String>;
                    for row_index in indexes {
                        if let Some(value) = rows[*row_index].cells[*column_index].as_ref().filter(|value| !value.trim().is_empty()) {
                            last = Some(value.clone());
                        } else if let Some(value) = last.as_ref() {
                            rows[*row_index].cells[*column_index] = Some(value.clone());
                        }
                    }
                }
            }
            FusionMissingStrategy::BackwardFill => {
                for column_index in &fillable_indexes {
                    let mut next = None::<String>;
                    for row_index in indexes.iter().rev() {
                        if let Some(value) = rows[*row_index].cells[*column_index].as_ref().filter(|value| !value.trim().is_empty()) {
                            next = Some(value.clone());
                        } else if let Some(value) = next.as_ref() {
                            rows[*row_index].cells[*column_index] = Some(value.clone());
                        }
                    }
                }
            }
            FusionMissingStrategy::NearestFill => {
                for column_index in &fillable_indexes {
                    fill_group_with_nearest(rows, indexes, *column_index);
                }
            }
            FusionMissingStrategy::LinearInterpolate => {
                for column_index in &fillable_indexes {
                    if numeric_indexes.contains(column_index) {
                        interpolate_group(rows, indexes, *column_index);
                    }
                }
            }
            FusionMissingStrategy::WindowMean => {
                for column_index in &fillable_indexes {
                    if numeric_indexes.contains(column_index) {
                        fill_with_window_mean(rows, indexes, *column_index);
                    }
                }
            }
            FusionMissingStrategy::KeepNull => {}
        }
    }
}

fn build_output_schema(
    primary: &DataTable,
    secondary_sources: &mut [PreparedSource],
) -> (Vec<String>, Vec<LogicalType>) {
    let mut headers = primary.column_names();
    let mut logical_types = primary
        .columns
        .iter()
        .map(|column| column.logical_type.clone())
        .collect::<Vec<_>>();
    let mut seen_names = headers
        .iter()
        .map(|name| normalize_name(name))
        .collect::<BTreeSet<_>>();

    for source in secondary_sources.iter_mut() {
        for binding in source.bindings.iter_mut() {
            if binding.overlaps_primary.is_some() {
                continue;
            }
            let output_name = unique_output_name(&binding.output_name, &mut seen_names);
            binding.output_name = output_name.clone();
            headers.push(output_name);
            logical_types.push(binding.logical_type.clone());
        }
    }
    (headers, logical_types)
}

fn build_output_bindings(
    source: SourceBundle<'_>,
    object_keys: &[String],
    time_column: &str,
    primary_name_map: &HashMap<String, usize>,
) -> Vec<OutputBinding> {
    let object_key_names = object_keys.iter().map(|name| normalize_name(name)).collect::<BTreeSet<_>>();
    let time_name = normalize_name(time_column);

    source
        .table
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            let normalized = normalize_name(&column.name);
            if object_key_names.contains(&normalized) || normalized == time_name {
                return None;
            }
            Some(OutputBinding {
                source_index: index,
                output_name: column.name.clone(),
                logical_type: column.logical_type.clone(),
                overlaps_primary: primary_name_map.get(&normalized).copied(),
            })
        })
        .collect()
}

fn select_secondary_match(
    source: &PreparedSource,
    primary_row: &PreparedRow,
    request: &FusionRequest,
) -> Result<Option<MatchResult>> {
    let Some(group_rows) = source.rows_by_key.get(&primary_row.object_key) else {
        return Ok(None);
    };
    let Some(primary_time) = primary_row.timestamp else {
        return Ok(None);
    };

    let window_seconds = request.time_window_seconds.max(1);
    let resample_seconds = request.resample_seconds.max(1);
    let matched = match request.alignment_mode {
        FusionAlignmentMode::ExactTime => group_rows
            .iter()
            .find(|row| row.timestamp == Some(primary_time))
            .map(|row| MatchResult {
                cells: row.cells.clone(),
                time_distance_seconds: Some(0),
                anomaly_cells: row.anomaly_cells,
                confidence_weight: source.confidence,
                trace: format!("{}: 精确命中", source.trace_label),
            }),
        FusionAlignmentMode::ExactThenNearest => group_rows
            .iter()
            .find(|row| row.timestamp == Some(primary_time))
            .map(|row| MatchResult {
                cells: row.cells.clone(),
                time_distance_seconds: Some(0),
                anomaly_cells: row.anomaly_cells,
                confidence_weight: source.confidence,
                trace: format!("{}: 精确命中", source.trace_label),
            })
            .or_else(|| nearest_match(group_rows, primary_time, window_seconds, source)),
        FusionAlignmentMode::NearestWithinWindow => nearest_match(group_rows, primary_time, window_seconds, source),
        FusionAlignmentMode::ExactThenWindow => group_rows
            .iter()
            .find(|row| row.timestamp == Some(primary_time))
            .map(|row| MatchResult {
                cells: row.cells.clone(),
                time_distance_seconds: Some(0),
                anomaly_cells: row.anomaly_cells,
                confidence_weight: source.confidence,
                trace: format!("{}: 精确命中", source.trace_label),
            })
            .or_else(|| window_match(group_rows, primary_time, window_seconds, source)),
        FusionAlignmentMode::WindowAggregation => window_match(group_rows, primary_time, window_seconds, source),
        FusionAlignmentMode::ResampleNearest => resample_match(group_rows, primary_time, resample_seconds, source),
    };
    Ok(matched)
}

fn nearest_match(
    rows: &[PreparedRow],
    primary_time: NaiveDateTime,
    window_seconds: i64,
    source: &PreparedSource,
) -> Option<MatchResult> {
    rows.iter()
        .filter_map(|row| {
            let timestamp = row.timestamp?;
            let distance = (timestamp - primary_time).num_seconds().abs();
            (distance <= window_seconds).then_some((row, distance))
        })
        .min_by_key(|(_, distance)| *distance)
        .map(|(row, distance)| MatchResult {
            cells: row.cells.clone(),
            time_distance_seconds: Some(distance),
            anomaly_cells: row.anomaly_cells,
            confidence_weight: time_weight(source.confidence, distance, window_seconds),
            trace: format!("{}: 最近邻 {}s", source.trace_label, distance),
        })
}

fn window_match(
    rows: &[PreparedRow],
    primary_time: NaiveDateTime,
    window_seconds: i64,
    source: &PreparedSource,
) -> Option<MatchResult> {
    let selected = rows
        .iter()
        .filter_map(|row| {
            let timestamp = row.timestamp?;
            let distance = (timestamp - primary_time).num_seconds().abs();
            (distance <= window_seconds).then_some((row, distance))
        })
        .collect::<Vec<_>>();
    if selected.is_empty() {
        return None;
    }

    let width = selected[0].0.cells.len();
    let cells = (0..width)
        .map(|column_index| aggregate_window_column(&selected, column_index))
        .collect::<Vec<_>>();
    let avg_distance = selected.iter().map(|(_, distance)| *distance).sum::<i64>() / selected.len() as i64;
    Some(MatchResult {
        cells,
        time_distance_seconds: Some(avg_distance),
        anomaly_cells: selected.iter().map(|(row, _)| row.anomaly_cells).sum::<usize>(),
        confidence_weight: time_weight(source.confidence, avg_distance, window_seconds),
        trace: format!("{}: 时间窗聚合 {} 行", source.trace_label, selected.len()),
    })
}

fn resample_match(
    rows: &[PreparedRow],
    primary_time: NaiveDateTime,
    resample_seconds: i64,
    source: &PreparedSource,
) -> Option<MatchResult> {
    let target_bucket = bucket_epoch(primary_time, resample_seconds);
    rows.iter()
        .filter_map(|row| {
            let timestamp = row.timestamp?;
            let bucket = bucket_epoch(timestamp, resample_seconds);
            (bucket == target_bucket).then_some((row, (timestamp - primary_time).num_seconds().abs()))
        })
        .min_by_key(|(_, distance)| *distance)
        .map(|(row, distance)| MatchResult {
            cells: row.cells.clone(),
            time_distance_seconds: Some(distance),
            anomaly_cells: row.anomaly_cells,
            confidence_weight: time_weight(source.confidence, distance, resample_seconds),
            trace: format!("{}: 重采样桶命中 {}s", source.trace_label, distance),
        })
}

fn apply_fusion_strategy(
    current: &mut Option<String>,
    candidate: &Option<String>,
    current_weight: &mut f64,
    candidate_weight: f64,
    strategy: &FusionStrategy,
) {
    if !is_non_empty(candidate.as_deref()) {
        return;
    }

    match strategy {
        FusionStrategy::PrimaryFirst => {
            if !is_non_empty(current.as_deref()) {
                *current = candidate.clone();
                *current_weight = candidate_weight;
            }
        }
        FusionStrategy::SecondaryFirst => {
            *current = candidate.clone();
            *current_weight = candidate_weight;
        }
        FusionStrategy::ComplementaryFill => {
            if !is_non_empty(current.as_deref()) {
                *current = candidate.clone();
                *current_weight = candidate_weight;
            }
        }
        FusionStrategy::ConflictRetention => {
            if !is_non_empty(current.as_deref()) {
                *current = candidate.clone();
                *current_weight = candidate_weight;
            }
        }
        FusionStrategy::NumericAverage => {
            if !is_non_empty(current.as_deref()) {
                *current = candidate.clone();
                *current_weight = candidate_weight;
                return;
            }
            let Some(current_numeric) = current.as_ref().and_then(|value| parse_numeric(value)) else {
                *current = candidate.clone();
                *current_weight = candidate_weight;
                return;
            };
            let Some(candidate_numeric) = candidate.as_ref().and_then(|value| parse_numeric(value)) else {
                return;
            };
            *current = Some(format_number((current_numeric + candidate_numeric) / 2.0));
            *current_weight = (*current_weight).max(candidate_weight);
        }
        FusionStrategy::ConfidenceWeighted => {
            if !is_non_empty(current.as_deref()) {
                *current = candidate.clone();
                *current_weight = candidate_weight;
                return;
            }
            let Some(current_numeric) = current.as_ref().and_then(|value| parse_numeric(value)) else {
                if candidate_weight > *current_weight {
                    *current = candidate.clone();
                    *current_weight = candidate_weight;
                }
                return;
            };
            let Some(candidate_numeric) = candidate.as_ref().and_then(|value| parse_numeric(value)) else {
                if candidate_weight > *current_weight {
                    *current = candidate.clone();
                    *current_weight = candidate_weight;
                }
                return;
            };
            let total_weight = (*current_weight + candidate_weight).max(f64::EPSILON);
            let fused = (current_numeric * *current_weight + candidate_numeric * candidate_weight) / total_weight;
            *current = Some(format_number(fused));
            *current_weight = total_weight;
        }
    }
}

fn apply_missing_strategy(
    rows: &mut [FusionAnchorRow],
    strategy: &FusionMissingStrategy,
    primary_width: usize,
    logical_types: &[LogicalType],
) {
    if rows.is_empty() || matches!(strategy, FusionMissingStrategy::KeepNull) {
        return;
    }

    let groups = group_anchor_indexes(rows);
    let numeric_indexes = logical_types
        .iter()
        .enumerate()
        .filter(|(_, logical_type)| matches!(logical_type, LogicalType::Integer | LogicalType::Float))
        .map(|(index, _)| index)
        .collect::<BTreeSet<_>>();
    let fillable_indexes = (0..logical_types.len())
        .filter(|index| *index >= primary_width)
        .chain((0..primary_width).filter(|index| *index > 0))
        .collect::<Vec<_>>();

    for indexes in groups.values() {
        match strategy {
            FusionMissingStrategy::ForwardFill => {
                for column_index in &fillable_indexes {
                    let mut last = None::<String>;
                    for row_index in indexes {
                        if let Some(value) = rows[*row_index].cells[*column_index].as_ref().filter(|value| !value.trim().is_empty()) {
                            last = Some(value.clone());
                        } else if let Some(value) = last.as_ref() {
                            rows[*row_index].cells[*column_index] = Some(value.clone());
                            rows[*row_index].corrections.insert("前向填充".to_string());
                        }
                    }
                }
            }
            FusionMissingStrategy::BackwardFill => {
                for column_index in &fillable_indexes {
                    let mut next = None::<String>;
                    for row_index in indexes.iter().rev() {
                        if let Some(value) = rows[*row_index].cells[*column_index].as_ref().filter(|value| !value.trim().is_empty()) {
                            next = Some(value.clone());
                        } else if let Some(value) = next.as_ref() {
                            rows[*row_index].cells[*column_index] = Some(value.clone());
                            rows[*row_index].corrections.insert("后向填充".to_string());
                        }
                    }
                }
            }
            FusionMissingStrategy::NearestFill => {
                for column_index in &fillable_indexes {
                    fill_anchor_with_nearest(rows, indexes, *column_index);
                }
            }
            FusionMissingStrategy::LinearInterpolate => {
                for column_index in &fillable_indexes {
                    if numeric_indexes.contains(column_index) {
                        interpolate_anchor_group(rows, indexes, *column_index);
                    }
                }
            }
            FusionMissingStrategy::WindowMean => {
                for column_index in &fillable_indexes {
                    if numeric_indexes.contains(column_index) {
                        fill_anchor_with_window_mean(rows, indexes, *column_index);
                    }
                }
            }
            FusionMissingStrategy::KeepNull => {}
        }
    }
}

#[allow(dead_code)]
fn build_feature_table(table: &DataTable, object_keys: &[String], time_column: &str) -> Result<DataTable> {
    let key_indexes = find_indexes(table, object_keys)?;
    let time_index = find_index(table, time_column)?;
    let numeric_columns = table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| {
            !column.name.starts_with("fusion_")
                && matches!(column.logical_type, LogicalType::Integer | LogicalType::Float)
        })
        .take(MAX_FEATURE_COLUMNS)
        .collect::<Vec<_>>();

    let mut headers = object_keys.to_vec();
    headers.extend([
        "row_count".to_string(),
        "start_time".to_string(),
        "end_time".to_string(),
    ]);
    for (_, column) in &numeric_columns {
        headers.extend([
            format!("{}_mean", column.name),
            format!("{}_max", column.name),
            format!("{}_min", column.name),
            format!("{}_last", column.name),
            format!("{}_delta", column.name),
            format!("{}_missing_rate", column.name),
        ]);
    }

    let mut grouped = BTreeMap::<String, Vec<Vec<Option<String>>>>::new();
    for row in table_rows(table) {
        grouped.entry(compose_key(&row, &key_indexes)).or_default().push(row);
    }

    let rows = grouped
        .into_values()
        .map(|mut group_rows| {
            group_rows.sort_by(|left, right| compare_datetime_cells(left.get(time_index), right.get(time_index)));
            let mut row = key_indexes
                .iter()
                .map(|index| group_rows[0].get(*index).cloned().unwrap_or(None))
                .collect::<Vec<_>>();
            row.push(Some(group_rows.len().to_string()));
            row.push(group_rows.first().and_then(|cells| cells.get(time_index).cloned().unwrap_or(None)));
            row.push(group_rows.last().and_then(|cells| cells.get(time_index).cloned().unwrap_or(None)));

            for (column_index, _) in &numeric_columns {
                let values = group_rows
                    .iter()
                    .filter_map(|cells| cells.get(*column_index).and_then(|cell| cell.as_ref()).and_then(|value| parse_numeric(value)))
                    .collect::<Vec<_>>();
                let missing_rate = 1.0 - values.len() as f64 / group_rows.len().max(1) as f64;
                if values.is_empty() {
                    row.extend((0..6).map(|_| None));
                    continue;
                }
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let max = values.iter().copied().reduce(f64::max).unwrap_or(mean);
                let min = values.iter().copied().reduce(f64::min).unwrap_or(mean);
                let last = *values.last().unwrap_or(&mean);
                let first = *values.first().unwrap_or(&mean);
                row.extend([
                    Some(format_number(mean)),
                    Some(format_number(max)),
                    Some(format_number(min)),
                    Some(format_number(last)),
                    Some(format_number(last - first)),
                    Some(format_number(missing_rate * 100.0)),
                ]);
            }
            row
        })
        .collect::<Vec<_>>();

    Ok(build_table_from_rows(
        headers,
        Vec::new(),
        rows,
    ))
}

#[allow(dead_code)]
fn build_event_table(
    table: &DataTable,
    object_keys: &[String],
    time_column: &str,
    request: &FusionRequest,
) -> Result<DataTable> {
    let key_indexes = find_indexes(table, object_keys)?;
    let time_index = find_index(table, time_column)?;
    let quality_index = find_index(table, "fusion_quality_score")?;
    let trace_index = find_index(table, "fusion_trace")?;
    let gap_threshold = request.time_window_seconds.max(request.resample_seconds).max(60);

    let headers = vec![
        "object_key".to_string(),
        "segment_id".to_string(),
        "start_time".to_string(),
        "end_time".to_string(),
        "row_count".to_string(),
        "avg_quality_score".to_string(),
        "trace_excerpt".to_string(),
    ];

    let mut rows = Vec::new();
    let mut grouped = BTreeMap::<String, Vec<Vec<Option<String>>>>::new();
    for row in table_rows(table) {
        grouped.entry(compose_key(&row, &key_indexes)).or_default().push(row);
    }

    for (object_key, mut group_rows) in grouped {
        group_rows.sort_by(|left, right| compare_datetime_cells(left.get(time_index), right.get(time_index)));
        let mut segment = Vec::new();
        let mut segment_id = 1usize;
        let mut previous_time = None::<NaiveDateTime>;

        for row in group_rows {
            let current_time = row.get(time_index).and_then(|cell| cell.as_ref()).and_then(|value| parse_datetime_value(value));
            let should_split = match (previous_time, current_time) {
                (Some(previous), Some(current)) => (current - previous).num_seconds().abs() > gap_threshold,
                _ => false,
            };
            if should_split && !segment.is_empty() {
                rows.push(build_event_row(&object_key, segment_id, &segment, quality_index, trace_index, time_index));
                segment.clear();
                segment_id += 1;
            }
            previous_time = current_time.or(previous_time);
            segment.push(row);
        }
        if !segment.is_empty() {
            rows.push(build_event_row(&object_key, segment_id, &segment, quality_index, trace_index, time_index));
        }
    }

    Ok(build_table_from_rows(headers, Vec::new(), rows))
}

#[allow(dead_code)]
fn build_alert_table(
    table: &DataTable,
    object_keys: &[String],
    time_column: &str,
    request: &FusionRequest,
) -> Result<DataTable> {
    let key_indexes = find_indexes(table, object_keys)?;
    let time_index = find_index(table, time_column)?;
    let quality_index = find_index(table, "fusion_quality_score")?;
    let trace_index = find_index(table, "fusion_trace")?;
    let correction_index = find_index(table, "fusion_corrections")?;

    let rows = table_rows(table)
        .into_iter()
        .filter_map(|row| {
            let quality = row
                .get(quality_index)
                .and_then(|cell| cell.as_ref())
                .and_then(|value| parse_numeric(value))
                .unwrap_or(100.0);
            let corrections = row
                .get(correction_index)
                .and_then(|cell| cell.as_ref())
                .cloned()
                .unwrap_or_default();
            let has_correction = !corrections.trim().is_empty()
                && corrections.trim() != "保持空值并标记"
                && corrections.trim() != "前向填充"
                && corrections.trim() != "后向填充"
                && corrections.trim() != "线性插值"
                && corrections.trim() != "窗口均值";
            let alert_needed = quality < request.alert_threshold || has_correction;
            if !alert_needed {
                return None;
            }
            let level = if quality < request.alert_threshold * 0.75 {
                "高"
            } else if quality < request.alert_threshold || has_correction {
                "中"
            } else {
                "低"
            };
            let reason = if quality < request.alert_threshold {
                format!("质量分 {:.1} 低于阈值 {:.1}", quality, request.alert_threshold)
            } else {
                format!("检测到修正记录：{}", corrections)
            };
            Some(vec![
                Some(compose_key(&row, &key_indexes)),
                row.get(time_index).cloned().unwrap_or(None),
                Some(level.to_string()),
                Some(reason),
                row.get(quality_index).cloned().unwrap_or(None),
                row.get(trace_index).cloned().unwrap_or(None),
            ])
        })
        .collect::<Vec<_>>();

    Ok(build_table_from_rows(
        vec![
            "object_key".to_string(),
            "event_time".to_string(),
            "severity".to_string(),
            "reason".to_string(),
            "quality_score".to_string(),
            "trace".to_string(),
        ],
        Vec::new(),
        rows,
    ))
}

#[allow(dead_code)]
fn build_event_row(
    object_key: &str,
    segment_id: usize,
    segment: &[Vec<Option<String>>],
    quality_index: usize,
    trace_index: usize,
    time_index: usize,
) -> Vec<Option<String>> {
    let qualities = segment
        .iter()
        .filter_map(|row| row.get(quality_index).and_then(|cell| cell.as_ref()).and_then(|value| parse_numeric(value)))
        .collect::<Vec<_>>();
    let avg_quality = if qualities.is_empty() {
        None
    } else {
        Some(format_number(qualities.iter().sum::<f64>() / qualities.len() as f64))
    };
    let trace_excerpt = segment
        .iter()
        .filter_map(|row| row.get(trace_index).and_then(|cell| cell.as_ref()).cloned())
        .filter(|trace| !trace.trim().is_empty() && trace != "无融合轨迹")
        .take(2)
        .collect::<Vec<_>>()
        .join(" | ");
    vec![
        Some(object_key.to_string()),
        Some(segment_id.to_string()),
        segment.first().and_then(|row| row.get(time_index).cloned().unwrap_or(None)),
        segment.last().and_then(|row| row.get(time_index).cloned().unwrap_or(None)),
        Some(segment.len().to_string()),
        avg_quality,
        Some(trace_excerpt),
    ]
}

fn resolve_required_keys(table: &DataTable, requested: &[String]) -> Result<Vec<String>> {
    let keys = requested
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    if keys.is_empty() {
        bail!("请手工填写至少一个对象键");
    }
    if let Some(key) = keys.iter().find(|key| !has_column(table, key)) {
        bail!("主源缺少对象键 {key}");
    }
    Ok(keys)
}

fn resolve_required_time(table: &DataTable, requested: &str) -> Result<String> {
    let column = requested.trim();
    if column.is_empty() {
        bail!("请手工填写时间列");
    }
    if !has_column(table, column) {
        bail!("主源缺少时间列 {column}");
    }
    Ok(column.to_string())
}

fn map_secondary_keys(table: &DataTable, primary_keys: &[String]) -> Result<Vec<String>> {
    primary_keys
        .iter()
        .map(|primary_key| map_secondary_column(table, primary_key))
        .collect()
}

fn map_secondary_time_column(table: &DataTable, primary_time: &str) -> Result<String> {
    map_secondary_column(table, primary_time)
}

fn map_secondary_column(table: &DataTable, primary_name: &str) -> Result<String> {
    let primary_normalized = normalize_name(primary_name);
    table.columns
        .iter()
        .find(|column| normalize_name(&column.name) == primary_normalized)
        .map(|column| column.name.clone())
        .ok_or_else(|| anyhow!("辅源未找到与 {primary_name} 同名的字段"))
}

fn source_confidence(source: SourceBundle<'_>, missing_ratio: f64) -> f64 {
    let mut score: f64 = 0.62;
    let name = source.dataset_name.to_ascii_lowercase();

    if source.profile.resolved_time_column.len() > 0 || !source.profile.time_candidates.is_empty() {
        score += 0.08;
    }
    if missing_ratio < 0.12 {
        score += 0.10;
    } else if missing_ratio > 0.35 {
        score -= 0.12;
    }
    if source.profile.quality_overview.duplicate_row_count > 0 {
        score -= 0.06;
    }
    if name.contains("label") || name.contains("manual") || source.dataset_name.contains("标签") {
        score += 0.12;
    }
    if name.contains("control") || name.contains("command") || source.dataset_name.contains("指令") {
        score += 0.06;
    }
    if name.contains("sim") || source.dataset_name.contains("仿真") {
        score -= 0.08;
    }
    score.clamp(0.25, 1.0)
}

fn compute_quality_score(
    matched_sources: usize,
    source_count: usize,
    secondary_field_count: usize,
    missing_fields: usize,
    anomaly_cells: usize,
    traces: &[String],
) -> f64 {
    let source_coverage_penalty = if source_count == 0 {
        0.0
    } else {
        (1.0 - matched_sources as f64 / source_count as f64) * 30.0
    };
    let missing_penalty = if secondary_field_count == 0 {
        0.0
    } else {
        (missing_fields as f64 / secondary_field_count as f64).min(1.0) * 30.0
    };
    let anomaly_penalty = (anomaly_cells as f64 * 4.0).min(24.0);
    let mismatch_penalty = traces.iter().filter(|trace| trace.contains("未命中")).count() as f64 * 4.0;
    (100.0 - source_coverage_penalty - missing_penalty - anomaly_penalty - mismatch_penalty).clamp(0.0, 100.0)
}

fn clean_outliers_in_rows(rows: &mut [PreparedRow], table: &DataTable, zscore_threshold: f64) -> usize {
    if rows.is_empty() {
        return 0;
    }

    let numeric_indexes = table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| matches!(column.logical_type, LogicalType::Integer | LogicalType::Float))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let mut total = 0usize;

    for column_index in numeric_indexes {
        let values = rows
            .iter()
            .filter_map(|row| row.cells.get(column_index).and_then(|cell| cell.as_ref()).and_then(|value| parse_numeric(value)))
            .collect::<Vec<_>>();
        if values.len() < 3 {
            continue;
        }
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance = values
            .iter()
            .map(|value| {
                let diff = *value - mean;
                diff * diff
            })
            .sum::<f64>()
            / values.len() as f64;
        let std_dev = variance.sqrt();
        if std_dev <= f64::EPSILON {
            continue;
        }

        for row in rows.iter_mut() {
            let Some(value) = row.cells.get(column_index).and_then(|cell| cell.as_ref()).and_then(|value| parse_numeric(value)) else {
                continue;
            };
            let zscore = ((value - mean) / std_dev).abs();
            if zscore > zscore_threshold.max(2.0) {
                row.cells[column_index] = None;
                row.anomaly_cells += 1;
                total += 1;
            }
        }
    }

    total
}

fn deduplicate_prepared_rows(rows: Vec<PreparedRow>) -> Vec<PreparedRow> {
    let mut deduplicated = BTreeMap::<(String, Option<NaiveDateTime>), PreparedRow>::new();
    for row in rows {
        let key = (row.object_key.clone(), row.timestamp);
        deduplicated
            .entry(key)
            .and_modify(|existing| {
                if count_non_empty(&row.cells) >= count_non_empty(&existing.cells) {
                    *existing = row.clone();
                }
            })
            .or_insert(row);
    }
    deduplicated.into_values().collect()
}

fn sort_prepared_rows(rows: &mut [PreparedRow]) {
    rows.sort_by(|left, right| {
        left.object_key
            .cmp(&right.object_key)
            .then_with(|| left.timestamp.cmp(&right.timestamp))
    });
}

fn interpolate_group(rows: &mut [PreparedRow], indexes: &[usize], column_index: usize) {
    let mut position = 0usize;
    while position < indexes.len() {
        let row_index = indexes[position];
        if rows[row_index].cells[column_index].is_some() {
            position += 1;
            continue;
        }

        let start = position;
        while position < indexes.len() && rows[indexes[position]].cells[column_index].is_none() {
            position += 1;
        }
        if start == 0 || position >= indexes.len() {
            continue;
        }

        let left_index = indexes[start - 1];
        let right_index = indexes[position];
        let Some(left_value) = rows[left_index].cells[column_index].as_ref().and_then(|value| parse_numeric(value)) else {
            continue;
        };
        let Some(right_value) = rows[right_index].cells[column_index].as_ref().and_then(|value| parse_numeric(value)) else {
            continue;
        };
        let total_gap = rows[right_index]
            .timestamp
            .zip(rows[left_index].timestamp)
            .map(|(right, left)| (right - left).num_seconds().abs())
            .unwrap_or((position - start + 1) as i64);
        if total_gap == 0 {
            continue;
        }

        for missing_position in start..position {
            let current_index = indexes[missing_position];
            let step = rows[current_index]
                .timestamp
                .zip(rows[left_index].timestamp)
                .map(|(current, left)| (current - left).num_seconds().abs())
                .unwrap_or((missing_position - start + 1) as i64);
            let ratio = step as f64 / total_gap as f64;
            let value = left_value + (right_value - left_value) * ratio;
            rows[current_index].cells[column_index] = Some(format_number(value));
        }
    }
}

fn fill_with_window_mean(rows: &mut [PreparedRow], indexes: &[usize], column_index: usize) {
    for position in 0..indexes.len() {
        let row_index = indexes[position];
        if rows[row_index].cells[column_index].is_some() {
            continue;
        }
        let previous = indexes[..position]
            .iter()
            .rev()
            .find_map(|index| rows[*index].cells[column_index].as_ref().and_then(|value| parse_numeric(value)));
        let next = indexes[position + 1..]
            .iter()
            .find_map(|index| rows[*index].cells[column_index].as_ref().and_then(|value| parse_numeric(value)));
        if let (Some(previous), Some(next)) = (previous, next) {
            rows[row_index].cells[column_index] = Some(format_number((previous + next) / 2.0));
        }
    }
}

fn fill_group_with_nearest(rows: &mut [PreparedRow], indexes: &[usize], column_index: usize) {
    for position in 0..indexes.len() {
        let row_index = indexes[position];
        if rows[row_index].cells[column_index].is_some() {
            continue;
        }
        let previous = indexes[..position]
            .iter()
            .rev()
            .find_map(|index| rows[*index].cells[column_index].as_ref().cloned());
        let next = indexes[position + 1..]
            .iter()
            .find_map(|index| rows[*index].cells[column_index].as_ref().cloned());
        rows[row_index].cells[column_index] = previous.or(next);
    }
}

fn interpolate_anchor_group(rows: &mut [FusionAnchorRow], indexes: &[usize], column_index: usize) {
    let mut position = 0usize;
    while position < indexes.len() {
        let row_index = indexes[position];
        if rows[row_index].cells[column_index].is_some() {
            position += 1;
            continue;
        }

        let start = position;
        while position < indexes.len() && rows[indexes[position]].cells[column_index].is_none() {
            position += 1;
        }
        if start == 0 || position >= indexes.len() {
            continue;
        }

        let left_index = indexes[start - 1];
        let right_index = indexes[position];
        let Some(left_value) = rows[left_index].cells[column_index].as_ref().and_then(|value| parse_numeric(value)) else {
            continue;
        };
        let Some(right_value) = rows[right_index].cells[column_index].as_ref().and_then(|value| parse_numeric(value)) else {
            continue;
        };
        let total_gap = rows[right_index]
            .timestamp
            .zip(rows[left_index].timestamp)
            .map(|(right, left)| (right - left).num_seconds().abs())
            .unwrap_or((position - start + 1) as i64);
        if total_gap == 0 {
            continue;
        }

        for missing_position in start..position {
            let current_index = indexes[missing_position];
            let step = rows[current_index]
                .timestamp
                .zip(rows[left_index].timestamp)
                .map(|(current, left)| (current - left).num_seconds().abs())
                .unwrap_or((missing_position - start + 1) as i64);
            let ratio = step as f64 / total_gap as f64;
            let value = left_value + (right_value - left_value) * ratio;
            rows[current_index].cells[column_index] = Some(format_number(value));
            rows[current_index].corrections.insert("线性插值".to_string());
        }
    }
}

fn fill_anchor_with_window_mean(rows: &mut [FusionAnchorRow], indexes: &[usize], column_index: usize) {
    for position in 0..indexes.len() {
        let row_index = indexes[position];
        if rows[row_index].cells[column_index].is_some() {
            continue;
        }
        let previous = indexes[..position]
            .iter()
            .rev()
            .find_map(|index| rows[*index].cells[column_index].as_ref().and_then(|value| parse_numeric(value)));
        let next = indexes[position + 1..]
            .iter()
            .find_map(|index| rows[*index].cells[column_index].as_ref().and_then(|value| parse_numeric(value)));
        if let (Some(previous), Some(next)) = (previous, next) {
            rows[row_index].cells[column_index] = Some(format_number((previous + next) / 2.0));
            rows[row_index].corrections.insert("窗口均值".to_string());
        }
    }
}

fn fill_anchor_with_nearest(rows: &mut [FusionAnchorRow], indexes: &[usize], column_index: usize) {
    for position in 0..indexes.len() {
        let row_index = indexes[position];
        if rows[row_index].cells[column_index].is_some() {
            continue;
        }
        let previous = indexes[..position]
            .iter()
            .rev()
            .find_map(|index| rows[*index].cells[column_index].as_ref().cloned());
        let next = indexes[position + 1..]
            .iter()
            .find_map(|index| rows[*index].cells[column_index].as_ref().cloned());
        if let Some(value) = previous.or(next) {
            rows[row_index].cells[column_index] = Some(value);
            rows[row_index].corrections.insert("就近填充".to_string());
        }
    }
}

fn aggregate_window_column(selected: &[(&PreparedRow, i64)], column_index: usize) -> Option<String> {
    let numeric_values = selected
        .iter()
        .filter_map(|(row, _)| row.cells.get(column_index).and_then(|cell| cell.as_ref()).and_then(|value| parse_numeric(value)))
        .collect::<Vec<_>>();
    if !numeric_values.is_empty() {
        let mean = numeric_values.iter().sum::<f64>() / numeric_values.len() as f64;
        return Some(format_number(mean));
    }

    selected
        .iter()
        .min_by_key(|(_, distance)| *distance)
        .and_then(|(row, _)| row.cells.get(column_index).cloned().unwrap_or(None))
}

fn group_row_indexes(rows: &[PreparedRow]) -> BTreeMap<String, Vec<usize>> {
    rows.iter().enumerate().fold(BTreeMap::new(), |mut acc, (index, row)| {
        acc.entry(row.object_key.clone()).or_default().push(index);
        acc
    })
}

fn group_anchor_indexes(rows: &[FusionAnchorRow]) -> BTreeMap<String, Vec<usize>> {
    rows.iter().enumerate().fold(BTreeMap::new(), |mut acc, (index, row)| {
        acc.entry(row.object_key.clone()).or_default().push(index);
        acc
    })
}

fn build_table_from_rows(
    headers: Vec<String>,
    logical_types: Vec<LogicalType>,
    rows: Vec<Vec<Option<String>>>,
) -> DataTable {
    let width = headers.len();
    let mut columns = Vec::with_capacity(width);
    for index in 0..width {
        let values = rows
            .iter()
            .map(|row| row.get(index).cloned().unwrap_or(None))
            .collect::<Vec<_>>();
        let logical_type = logical_types
            .get(index)
            .cloned()
            .unwrap_or_else(|| infer_logical_type(&values));
        columns.push(TableColumn {
            name: headers
                .get(index)
                .cloned()
                .unwrap_or_else(|| format!("column_{}", index + 1)),
            logical_type,
            values,
        });
    }
    DataTable { columns }
}

fn table_rows(table: &DataTable) -> Vec<Vec<Option<String>>> {
    (0..table.height()).map(|index| table.row(index)).collect()
}

fn find_index(table: &DataTable, column_name: &str) -> Result<usize> {
    table
        .columns
        .iter()
        .position(|column| column.name == column_name)
        .ok_or_else(|| anyhow!("未找到字段 {column_name}"))
}

fn find_indexes(table: &DataTable, column_names: &[String]) -> Result<Vec<usize>> {
    column_names
        .iter()
        .map(|column_name| find_index(table, column_name))
        .collect()
}

fn has_column(table: &DataTable, column_name: &str) -> bool {
    table.columns.iter().any(|column| column.name == column_name)
}

fn compose_key(row: &[Option<String>], indexes: &[usize]) -> String {
    indexes
        .iter()
        .map(|index| {
            row.get(*index)
                .cloned()
                .unwrap_or(None)
                .unwrap_or_else(|| "NULL".to_string())
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn normalize_name(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn unique_output_name(base: &str, seen_names: &mut BTreeSet<String>) -> String {
    let trimmed = base.trim();
    let candidate = if trimmed.is_empty() { "fusion_field" } else { trimmed };
    let normalized = normalize_name(candidate);
    if !seen_names.contains(&normalized) {
        seen_names.insert(normalized);
        return candidate.to_string();
    }

    let mut suffix = 2usize;
    loop {
        let name = format!("{candidate}_{suffix}");
        let normalized = normalize_name(&name);
        if !seen_names.contains(&normalized) {
            seen_names.insert(normalized);
            return name;
        }
        suffix += 1;
    }
}

fn parse_datetime_cell(value: Option<&String>) -> Option<NaiveDateTime> {
    value.and_then(|value| parse_datetime_value(value))
}

fn parse_datetime_value(value: &str) -> Option<NaiveDateTime> {
    const DATETIME_PATTERNS: [&str; 8] = [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y%m%d",
        "%+",
    ];

    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value.trim()) {
        return Some(timestamp.naive_utc());
    }
    for pattern in DATETIME_PATTERNS {
        if let Ok(value) = NaiveDateTime::parse_from_str(value.trim(), pattern) {
            return Some(value);
        }
        if let Ok(value) = NaiveDate::parse_from_str(value.trim(), pattern) {
            if let Some(value) = value.and_hms_opt(0, 0, 0) {
                return Some(value);
            }
        }
    }
    parse_timestamp(value.trim())
}

fn parse_timestamp(value: &str) -> Option<NaiveDateTime> {
    if value.is_empty() || !value.chars().all(|ch| ch == '-' || ch.is_ascii_digit()) {
        return None;
    }
    let timestamp = value.parse::<i64>().ok()?;
    let abs = timestamp.abs();
    let (seconds, nanos) = if abs >= 1_000_000_000_000_000_000 {
        (timestamp / 1_000_000_000, (timestamp % 1_000_000_000).unsigned_abs() as u32)
    } else if abs >= 1_000_000_000_000_000 {
        (timestamp / 1_000_000, ((timestamp % 1_000_000).unsigned_abs() as u32) * 1_000)
    } else if abs >= 1_000_000_000_000 {
        (timestamp / 1_000, ((timestamp % 1_000).unsigned_abs() as u32) * 1_000_000)
    } else {
        (timestamp, 0)
    };
    DateTime::<Utc>::from_timestamp(seconds, nanos).map(|value| value.naive_utc())
}

fn parse_numeric(value: &str) -> Option<f64> {
    value.trim().replace([',', ' '], "").parse::<f64>().ok()
}

#[allow(dead_code)]
fn compare_datetime_cells(left: Option<&Option<String>>, right: Option<&Option<String>>) -> Ordering {
    let left = left.and_then(|cell| cell.as_ref()).and_then(|value| parse_datetime_value(value));
    let right = right.and_then(|cell| cell.as_ref()).and_then(|value| parse_datetime_value(value));
    left.cmp(&right)
}

fn compute_missing_ratio(table: &DataTable) -> f64 {
    let total = table.height().saturating_mul(table.width());
    if total == 0 {
        return 0.0;
    }
    let missing = table
        .columns
        .iter()
        .flat_map(|column| column.values.iter())
        .filter(|value| !is_non_empty(value.as_deref()))
        .count();
    missing as f64 / total as f64
}

fn count_non_empty(cells: &[Option<String>]) -> usize {
    cells.iter().filter(|value| is_non_empty(value.as_deref())).count()
}

fn is_non_empty(value: Option<&str>) -> bool {
    value.map(|value| !value.trim().is_empty()).unwrap_or(false)
}

fn time_weight(base: f64, distance: i64, window_seconds: i64) -> f64 {
    let closeness = 1.0 - (distance as f64 / window_seconds.max(1) as f64).min(1.0);
    (base * (0.55 + closeness * 0.45)).clamp(0.1, 1.0)
}

#[allow(dead_code)]
fn dataset_alias(dataset_id: i32, dataset_name: &str) -> String {
    let mut slug = dataset_name
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch.to_ascii_lowercase() } else { '_' })
        .collect::<String>();
    slug = slug.trim_matches('_').to_string();
    if slug.is_empty() {
        format!("s{dataset_id}")
    } else {
        format!("s{dataset_id}_{}", slug)
    }
}

#[allow(dead_code)]
fn guess_role_label(dataset_name: &str) -> &'static str {
    let name = dataset_name.to_ascii_lowercase();
    if name.contains("sensor") || dataset_name.contains("传感") || dataset_name.contains("采样") {
        "传感采样"
    } else if name.contains("state") || dataset_name.contains("状态") {
        "设备状态"
    } else if name.contains("command") || dataset_name.contains("指令") || dataset_name.contains("控制") {
        "控制指令"
    } else if name.contains("label") || dataset_name.contains("标签") {
        "人工标签"
    } else if name.contains("sim") || dataset_name.contains("仿真") {
        "仿真结果"
    } else if dataset_name.contains("工况") || dataset_name.contains("条件") {
        "试验工况"
    } else {
        "工程数据"
    }
}

#[allow(dead_code)]
fn looks_like_time_name(value: &str) -> bool {
    let normalized = normalize_name(value);
    normalized.contains("time") || normalized.contains("timestamp") || value.contains("时间")
}

fn bucket_epoch(timestamp: NaiveDateTime, bucket_size: i64) -> i64 {
    timestamp.and_utc().timestamp() / bucket_size.max(1)
}

fn join_or_default(values: &[String], default_value: &str) -> String {
    if values.is_empty() {
        default_value.to_string()
    } else {
        values.join(" | ")
    }
}

fn format_number(value: f64) -> String {
    if (value.fract()).abs() < 1e-9 {
        format!("{}", value as i64)
    } else {
        format!("{value:.6}")
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    }
}
