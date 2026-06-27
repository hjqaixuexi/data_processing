use anyhow::{Result, bail};
use chrono::{DateTime, Local};
use polars::prelude::{Column, DataFrame, NamedFrom, Series};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum FileFormat {
    Csv,
    Json,
    Xlsx,
}

impl FileFormat {
    pub fn from_path(path: &std::path::Path) -> Result<Self> {
        match path
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| value.to_ascii_lowercase())
            .as_deref()
        {
            Some("csv") => Ok(Self::Csv),
            Some("json") => Ok(Self::Json),
            Some("xlsx") => Ok(Self::Xlsx),
            _ => bail!("仅支持 csv / json / xlsx 文件"),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Csv => "CSV",
            Self::Json => "JSON",
            Self::Xlsx => "XLSX",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogicalType {
    Integer,
    Float,
    Boolean,
    DateTime,
    Text,
}

impl LogicalType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Integer => "整数",
            Self::Float => "浮点",
            Self::Boolean => "布尔",
            Self::DateTime => "时间",
            Self::Text => "文本",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableColumn {
    pub name: String,
    pub logical_type: LogicalType,
    pub values: Vec<Option<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataTable {
    pub columns: Vec<TableColumn>,
}

impl DataTable {
    pub fn width(&self) -> usize {
        self.columns.len()
    }

    pub fn height(&self) -> usize {
        self.columns.first().map(|column| column.values.len()).unwrap_or(0)
    }

    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|column| column.name.clone()).collect()
    }

    pub fn row(&self, index: usize) -> Vec<Option<String>> {
        self.columns
            .iter()
            .map(|column| column.values.get(index).cloned().unwrap_or(None))
            .collect()
    }

    pub fn preview_header(&self, limit: usize) -> PreviewHeader {
        let titles = self
            .columns
            .iter()
            .take(limit.min(8))
            .map(|column| compact_preview_text(Some(&column.name), 18))
            .collect::<Vec<_>>();

        PreviewHeader::from_cells(titles)
    }

    pub fn preview_rows(&self, row_limit: usize, col_limit: usize) -> Vec<PreviewRow> {
        let visible_cols = col_limit.min(8);
        (0..self.height().min(row_limit))
            .map(|row_index| {
                let cells = self
                    .row(row_index)
                    .into_iter()
                    .take(visible_cols)
                    .map(|value| compact_preview_text(value.as_ref(), 28))
                    .collect::<Vec<_>>();

                PreviewRow {
                    row_label: (row_index + 1).to_string(),
                    ..PreviewRow::from_cells(cells)
                }
            })
            .collect()
    }

    pub fn preview_rows_window(
        &self,
        page: usize,
        page_size: usize,
        col_limit: usize,
    ) -> Vec<PreviewRow> {
        if self.height() == 0 {
            return Vec::new();
        }

        let page_size = page_size.max(1);
        let total_pages = total_pages(self.height(), page_size);
        let page = clamp_page(page, total_pages);
        let start = (page - 1) * page_size;
        let end = (start + page_size).min(self.height());
        let indexes = (start..end).collect::<Vec<_>>();
        self.preview_rows_by_indexes(&indexes, col_limit)
    }

    pub fn preview_sample_rows(
        &self,
        page: usize,
        page_size: usize,
        col_limit: usize,
    ) -> Vec<PreviewRow> {
        let indexes = pseudo_random_row_indexes(self.height(), page, page_size);
        self.preview_rows_by_indexes(&indexes, col_limit)
    }

    fn preview_rows_by_indexes(&self, indexes: &[usize], col_limit: usize) -> Vec<PreviewRow> {
        let visible_cols = col_limit.min(8);
        indexes
            .iter()
            .filter(|row_index| **row_index < self.height())
            .map(|row_index| {
                let cells = self
                    .row(*row_index)
                    .into_iter()
                    .take(visible_cols)
                    .map(|value| compact_preview_text(value.as_ref(), 32))
                    .collect::<Vec<_>>();

                PreviewRow {
                    row_label: (row_index + 1).to_string(),
                    ..PreviewRow::from_cells(cells)
                }
            })
            .collect()
    }

    pub fn to_frame(&self) -> Result<DataFrame> {
        let mut columns = Vec::with_capacity(self.columns.len());

        for column in &self.columns {
            let series = match column.logical_type {
                LogicalType::Integer => {
                    let values = column
                        .values
                        .iter()
                        .map(|value| value.as_deref().and_then(|entry| entry.parse::<i64>().ok()))
                        .collect::<Vec<_>>();
                    Series::new(column.name.as_str().into(), values)
                }
                LogicalType::Float => {
                    let values = column
                        .values
                        .iter()
                        .map(|value| value.as_deref().and_then(|entry| entry.parse::<f64>().ok()))
                        .collect::<Vec<_>>();
                    Series::new(column.name.as_str().into(), values)
                }
                LogicalType::Boolean => {
                    let values = column
                        .values
                        .iter()
                        .map(|value| value.as_deref().and_then(parse_bool))
                        .collect::<Vec<_>>();
                    Series::new(column.name.as_str().into(), values)
                }
                LogicalType::DateTime | LogicalType::Text => {
                    let values = column.values.clone();
                    Series::new(column.name.as_str().into(), values)
                }
            };

            columns.push(Column::from(series));
        }

        DataFrame::new(columns).map_err(anyhow::Error::from)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct PreviewHeader {
    pub col1: String,
    pub col2: String,
    pub col3: String,
    pub col4: String,
    pub col5: String,
    pub col6: String,
    pub col7: String,
    pub col8: String,
}

impl PreviewHeader {
    pub fn from_cells(cells: Vec<String>) -> Self {
        Self {
            col1: cell_at(&cells, 0),
            col2: cell_at(&cells, 1),
            col3: cell_at(&cells, 2),
            col4: cell_at(&cells, 3),
            col5: cell_at(&cells, 4),
            col6: cell_at(&cells, 5),
            col7: cell_at(&cells, 6),
            col8: cell_at(&cells, 7),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct PreviewRow {
    pub row_label: String,
    pub col1: String,
    pub col2: String,
    pub col3: String,
    pub col4: String,
    pub col5: String,
    pub col6: String,
    pub col7: String,
    pub col8: String,
}

impl PreviewRow {
    pub fn from_cells(cells: Vec<String>) -> Self {
        Self {
            row_label: String::new(),
            col1: cell_at(&cells, 0),
            col2: cell_at(&cells, 1),
            col3: cell_at(&cells, 2),
            col4: cell_at(&cells, 3),
            col5: cell_at(&cells, 4),
            col6: cell_at(&cells, 5),
            col7: cell_at(&cells, 6),
            col8: cell_at(&cells, 7),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnProfile {
    pub name: String,
    pub logical_type: String,
    pub non_null_count: usize,
    pub missing_count: usize,
    pub missing_rate: f32,
    pub unique_count: usize,
    pub sample_value: String,
    pub role_hint: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QualityIssue {
    pub category: String,
    pub severity: String,
    pub field: String,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MappingSuggestion {
    pub source_name: String,
    pub target_name: String,
    pub confidence: String,
    pub status: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinSuggestion {
    pub target_dataset: String,
    pub left_key: String,
    pub right_key: String,
    pub join_type: String,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QualityRules {
    pub primary_key: String,
    pub composite_keys: Vec<String>,
    pub time_column: String,
    pub high_missing_threshold: f32,
}

impl Default for QualityRules {
    fn default() -> Self {
        Self {
            primary_key: String::new(),
            composite_keys: Vec::new(),
            time_column: String::new(),
            high_missing_threshold: 0.3,
        }
    }
}

impl QualityRules {
    pub fn normalized_threshold(&self) -> f32 {
        if self.high_missing_threshold.is_finite() {
            self.high_missing_threshold.clamp(0.05, 0.95)
        } else {
            0.3
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct QualityOverview {
    pub high_missing_field_count: usize,
    pub fully_empty_row_count: usize,
    pub duplicate_row_count: usize,
    pub primary_key_duplicate_count: usize,
    pub composite_duplicate_count: usize,
    pub primary_key_empty_count: usize,
    pub numeric_invalid_column_count: usize,
    pub mixed_type_column_count: usize,
    pub time_order_issue_count: usize,
    pub range_rule_issue_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatasetProfile {
    pub row_count: usize,
    pub column_count: usize,
    pub key_candidates: Vec<String>,
    pub time_candidates: Vec<String>,
    pub numeric_columns: Vec<String>,
    pub resolved_primary_key: String,
    pub resolved_composite_keys: Vec<String>,
    pub resolved_time_column: String,
    pub preview_header: PreviewHeader,
    pub preview_rows: Vec<PreviewRow>,
    pub columns: Vec<ColumnProfile>,
    pub quality_overview: QualityOverview,
    pub quality_issues: Vec<QualityIssue>,
    pub mapping_suggestions: Vec<MappingSuggestion>,
}

#[derive(Clone, Debug)]
pub struct DatasetRecord {
    pub id: i32,
    pub dataset_name: String,
    pub source_path: PathBuf,
    pub format: FileFormat,
    pub size_bytes: u64,
    pub imported_at: DateTime<Local>,
    pub sheet_name: Option<String>,
    pub source_table: DataTable,
    pub working_table: DataTable,
    pub frame: DataFrame,
    pub quality_rules: QualityRules,
    pub profile: DatasetProfile,
    pub pipeline_steps: Vec<PipelineStep>,
    pub undo_stack: Vec<DatasetHistory>,
    pub redo_stack: Vec<DatasetHistory>,
}

#[derive(Clone, Debug)]
pub struct DatasetHistory {
    pub working_table: DataTable,
    pub pipeline_steps: Vec<PipelineStep>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineStep {
    pub timestamp: DateTime<Local>,
    pub action: String,
    pub detail: String,
    pub outcome: String,
    pub operation: Option<PipelineOperation>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum JoinKind {
    Left,
    Inner,
    Outer,
}

impl JoinKind {
    pub fn from_text(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "inner" | "内连接" => Self::Inner,
            "outer" | "full" | "外连接" => Self::Outer,
            _ => Self::Left,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Left => "左连接",
            Self::Inner => "内连接",
            Self::Outer => "外连接",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TextCaseMode {
    Upper,
    Lower,
}

impl TextCaseMode {
    pub fn from_text(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "lower" | "小写" => Self::Lower,
            _ => Self::Upper,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Upper => "大写",
            Self::Lower => "小写",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AggregateFunction {
    Sum,
    Mean,
    Max,
    Min,
    Count,
    CountDistinct,
}

impl AggregateFunction {
    pub fn from_text(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "sum" | "求和" => Self::Sum,
            "mean" | "avg" | "average" | "平均值" => Self::Mean,
            "max" | "最大值" => Self::Max,
            "min" | "最小值" => Self::Min,
            "nunique" | "count_distinct" | "去重计数" => Self::CountDistinct,
            _ => Self::Count,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sum => "求和",
            Self::Mean => "平均值",
            Self::Max => "最大值",
            Self::Min => "最小值",
            Self::Count => "计数",
            Self::CountDistinct => "去重计数",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PipelineOperation {
    Reinspect,
    NormalizeColumnNames,
    TrimTextValues,
    DropEmptyRows,
    DeduplicateRows,
    FilterRowsContains {
        column: String,
        keyword: String,
    },
    KeepRowRange {
        start: usize,
        end: usize,
    },
    KeepTopRows {
        count: usize,
    },
    SampleRows {
        count: usize,
    },
    DropRowsWithMissing {
        columns: Vec<String>,
    },
    RenameColumn {
        from: String,
        to: String,
    },
    KeepColumns {
        columns: Vec<String>,
    },
    DropColumns {
        columns: Vec<String>,
    },
    SortBy {
        column: String,
        ascending: bool,
    },
    FillNullText {
        column: String,
        value: String,
    },
    FillNullForward {
        column: String,
    },
    CastColumn {
        column: String,
        target: LogicalType,
    },
    TransformTextCase {
        column: String,
        mode: TextCaseMode,
    },
    ReplaceText {
        column: String,
        from: String,
        to: String,
    },
    RoundNumeric {
        column: String,
        digits: usize,
    },
    GroupAggregate {
        group_columns: Vec<String>,
        target_column: String,
        function: AggregateFunction,
    },
    ApplyMappings {
        mappings: Vec<(String, String)>,
    },
}

impl fmt::Display for PipelineOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reinspect => write!(formatter, "重新分析"),
            Self::NormalizeColumnNames => write!(formatter, "列名标准化"),
            Self::TrimTextValues => write!(formatter, "文本去空白"),
            Self::DropEmptyRows => write!(formatter, "删除空行"),
            Self::DeduplicateRows => write!(formatter, "整表去重"),
            Self::FilterRowsContains { column, keyword } => {
                write!(formatter, "按列筛选 {} 包含 {}", column, keyword)
            }
            Self::KeepRowRange { start, end } => write!(formatter, "保留行范围 {}-{}", start, end),
            Self::KeepTopRows { count } => write!(formatter, "保留前 {} 行", count),
            Self::SampleRows { count } => write!(formatter, "抽样 {} 行", count),
            Self::DropRowsWithMissing { columns } => {
                if columns.is_empty() {
                    write!(formatter, "删除含缺失值记录")
                } else {
                    write!(formatter, "删除缺失记录 {}", columns.join(", "))
                }
            }
            Self::RenameColumn { from, to } => write!(formatter, "列重命名 {} -> {}", from, to),
            Self::KeepColumns { columns } => write!(formatter, "保留列 {}", columns.join(", ")),
            Self::DropColumns { columns } => write!(formatter, "删除列 {}", columns.join(", ")),
            Self::SortBy { column, ascending } => write!(
                formatter,
                "按列排序 {} {}",
                column,
                if *ascending { "升序" } else { "降序" }
            ),
            Self::FillNullText { column, value } => {
                write!(formatter, "默认值填充 {} = {}", column, value)
            }
            Self::FillNullForward { column } => write!(formatter, "前值填充 {}", column),
            Self::CastColumn { column, target } => {
                write!(formatter, "类型转换 {} -> {}", column, target.as_str())
            }
            Self::TransformTextCase { column, mode } => {
                write!(formatter, "文本大小写统一 {} -> {}", column, mode.as_str())
            }
            Self::ReplaceText { column, from, to } => {
                write!(formatter, "文本替换 {}: {} -> {}", column, from, to)
            }
            Self::RoundNumeric { column, digits } => {
                write!(formatter, "数值保留小数 {} -> {} 位", column, digits)
            }
            Self::GroupAggregate {
                group_columns,
                target_column,
                function,
            } => {
                if matches!(function, AggregateFunction::Count) {
                    write!(formatter, "分组聚合 {} -> {}", group_columns.join(", "), function.as_str())
                } else {
                    write!(
                        formatter,
                        "分组聚合 {} / {} -> {}",
                        group_columns.join(", "),
                        target_column,
                        function.as_str()
                    )
                }
            }
            Self::ApplyMappings { mappings } => {
                write!(formatter, "应用推荐映射 {} 项", mappings.len())
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineTemplate {
    pub dataset_name: String,
    pub created_at: DateTime<Local>,
    pub operations: Vec<PipelineOperation>,
}

#[derive(Clone, Debug)]
pub struct LoadedDataset {
    pub dataset_name: String,
    pub source_path: PathBuf,
    pub format: FileFormat,
    pub size_bytes: u64,
    pub imported_at: DateTime<Local>,
    pub sheet_name: Option<String>,
    pub table: DataTable,
}

#[derive(Clone, Debug)]
pub struct DatasetSnapshot {
    pub dataset_id: i32,
    pub dataset_name: String,
    pub format: String,
    pub size_label: String,
    pub imported_at: String,
    pub sheet_name: String,
    pub overview: String,
    pub key_hint: String,
    pub time_hint: String,
}

pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
    let mut value = bytes as f64;
    let mut unit_index = 0usize;

    while value >= 1024.0 && unit_index < UNITS.len() - 1 {
        value /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{value:.2} {}", UNITS[unit_index])
    }
}

pub fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "y" | "是" => Some(true),
        "false" | "0" | "no" | "n" | "否" => Some(false),
        _ => None,
    }
}

pub fn looks_like_datetime(value: &str) -> bool {
    const PATTERNS: [&str; 8] = [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y%m%d",
        "%+",
    ];

    PATTERNS
        .iter()
        .any(|pattern| chrono::NaiveDateTime::parse_from_str(value, pattern).is_ok())
        || PATTERNS
            .iter()
            .any(|pattern| chrono::NaiveDate::parse_from_str(value, pattern).is_ok())
}

pub fn normalize_headers(headers: &[String]) -> Vec<String> {
    let mut counts = HashMap::<String, usize>::new();

    headers
        .iter()
        .enumerate()
        .map(|(index, header)| {
            let mut candidate = header
                .trim()
                .replace(['\r', '\n', '\t'], " ")
                .replace('　', " ")
                .split_whitespace()
                .collect::<Vec<_>>()
                .join("_")
                .to_ascii_lowercase();

            if candidate.is_empty() {
                candidate = format!("column_{}", index + 1);
            }

            let current = counts.entry(candidate.clone()).or_insert(0);
            *current += 1;

            if *current > 1 {
                format!("{candidate}_{}", current)
            } else {
                candidate
            }
        })
        .collect()
}

pub fn null_marked(value: &str) -> bool {
    matches!(
        value.trim(),
        "" | "null" | "NULL" | "NaN" | "N/A" | "NA" | "None" | "无" | "-"
    )
}

pub fn infer_logical_type(values: &[Option<String>]) -> LogicalType {
    let non_null = values.iter().flatten().collect::<Vec<_>>();
    if non_null.is_empty() {
        return LogicalType::Text;
    }

    if non_null.iter().all(|value| value.parse::<i64>().is_ok()) {
        return LogicalType::Integer;
    }

    if non_null.iter().all(|value| value.parse::<f64>().is_ok()) {
        return LogicalType::Float;
    }

    if non_null.iter().all(|value| parse_bool(value).is_some()) {
        return LogicalType::Boolean;
    }

    let matched = non_null
        .iter()
        .filter(|value| looks_like_datetime(value))
        .count();

    if matched * 100 / non_null.len() >= 80 {
        return LogicalType::DateTime;
    }

    LogicalType::Text
}

pub fn row_signature(row: &[Option<String>]) -> String {
    row.iter()
        .map(|cell| cell.clone().unwrap_or_default())
        .collect::<Vec<_>>()
        .join("|")
}

pub fn total_pages(total_items: usize, page_size: usize) -> usize {
    if total_items == 0 {
        1
    } else {
        (total_items + page_size.max(1) - 1) / page_size.max(1)
    }
}

pub fn clamp_page(page: usize, total_pages: usize) -> usize {
    page.max(1).min(total_pages.max(1))
}

pub fn page_window(total_items: usize, page: usize, page_size: usize) -> (usize, usize, usize) {
    let page_size = page_size.max(1);
    let total_pages = total_pages(total_items, page_size);
    let page = clamp_page(page, total_pages);
    let start = total_items.min((page - 1) * page_size);
    let end = total_items.min(start + page_size);
    (page, start, end)
}

pub fn pseudo_random_row_indexes(total_rows: usize, page: usize, page_size: usize) -> Vec<usize> {
    if total_rows == 0 {
        return Vec::new();
    }

    let page_size = page_size.max(1);
    let total_pages = total_pages(total_rows, page_size);
    let page = clamp_page(page, total_pages);
    let offset = (page - 1) * page_size;
    let seed = ((page as u64) << 32) ^ total_rows as u64 ^ 0x9E37_79B9_7F4A_7C15;
    let start = seed as usize % total_rows;
    let mut step = if total_rows <= 1 {
        1
    } else {
        ((seed.rotate_left(17) as usize) % (total_rows - 1)).max(1)
    };

    while step < total_rows && greatest_common_divisor(step, total_rows) != 1 {
        step += 1;
    }
    if step >= total_rows {
        step = total_rows.saturating_sub(1).max(1);
    }

    (0..page_size)
        .map(|index| offset + index)
        .take_while(|position| *position < total_rows)
        .map(|position| (start + position * step) % total_rows)
        .collect()
}

fn cell_at(cells: &[String], index: usize) -> String {
    cells.get(index).cloned().unwrap_or_default()
}

fn greatest_common_divisor(mut left: usize, mut right: usize) -> usize {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn compact_preview_text(value: Option<&String>, max_chars: usize) -> String {
    let mut text = value
        .cloned()
        .unwrap_or_else(|| "∅".to_string())
        .replace(['\r', '\n', '\t'], " ");
    text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.is_empty() {
        return "∅".to_string();
    }

    if text.chars().count() > max_chars {
        text.chars().take(max_chars - 1).collect::<String>() + "…"
    } else {
        text
    }
}
