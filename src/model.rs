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
            .take(limit.min(self.columns.len()))
            .map(|column| compact_preview_text(Some(&column.name), 18))
            .collect::<Vec<_>>();

        PreviewHeader { cells: titles }
    }

    pub fn preview_rows(&self, row_limit: usize, col_limit: usize) -> Vec<PreviewRow> {
        let visible_cols = col_limit.min(self.columns.len());
        (0..self.height().min(row_limit))
            .map(|row_index| {
                let cells = self
                    .row(row_index)
                    .into_iter()
                    .take(visible_cols)
                    .map(|value| compact_preview_text(value.as_ref(), 28))
                    .collect::<Vec<_>>();

                PreviewRow { row_label: (row_index + 1).to_string(), cells }
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
        let visible_cols = col_limit.min(self.columns.len());
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

                PreviewRow { row_label: (row_index + 1).to_string(), cells }
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
    pub cells: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct PreviewRow {
    pub row_label: String,
    pub cells: Vec<String>,
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
    pub range_column: String,
    pub range_min: Option<f64>,
    pub range_max: Option<f64>,
    pub length_column: String,
    pub max_text_length: Option<usize>,
    pub time_gap_minutes: Option<i64>,
}

impl Default for QualityRules {
    fn default() -> Self {
        Self {
            primary_key: String::new(),
            composite_keys: Vec::new(),
            time_column: String::new(),
            high_missing_threshold: 0.3,
            range_column: String::new(),
            range_min: None,
            range_max: None,
            length_column: String::new(),
            max_text_length: None,
            time_gap_minutes: None,
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
    pub text_length_issue_count: usize,
    pub time_gap_issue_count: usize,
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
    pub import_duration_ms: Option<u64>,
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
pub enum JoinConflictStrategy {
    AppendRightSuffix,
    PrefixRightMarker,
    KeepLeftOnly,
}

impl JoinConflictStrategy {
    pub fn from_text(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "right_prefix" | "冲突列追加right前缀" => Self::PrefixRightMarker,
            "keep_left_only" | "仅保留主表字段" => Self::KeepLeftOnly,
            _ => Self::AppendRightSuffix,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AppendRightSuffix => "冲突列追加_right",
            Self::PrefixRightMarker => "冲突列追加right前缀",
            Self::KeepLeftOnly => "仅保留主表字段",
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
pub enum CompareOperator {
    Eq,
    NotEq,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    Contains,
    IsEmpty,
}

impl CompareOperator {
    pub fn from_text(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "!=" | "<>" | "不等于" => Self::NotEq,
            ">" | "大于" => Self::Greater,
            ">=" | "大于等于" => Self::GreaterEqual,
            "<" | "小于" => Self::Less,
            "<=" | "小于等于" => Self::LessEqual,
            "contains" | "包含" => Self::Contains,
            "empty" | "is_empty" | "为空" => Self::IsEmpty,
            _ => Self::Eq,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::NotEq => "!=",
            Self::Greater => ">",
            Self::GreaterEqual => ">=",
            Self::Less => "<",
            Self::LessEqual => "<=",
            Self::Contains => "包含",
            Self::IsEmpty => "为空",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TimeDiffUnit {
    Seconds,
    Minutes,
    Hours,
    Days,
}

impl TimeDiffUnit {
    pub fn from_text(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "秒" | "second" | "seconds" => Self::Seconds,
            "小时" | "hour" | "hours" => Self::Hours,
            "天" | "day" | "days" => Self::Days,
            _ => Self::Minutes,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Seconds => "秒",
            Self::Minutes => "分钟",
            Self::Hours => "小时",
            Self::Days => "天",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PriorityPlacement {
    First,
    Last,
}

impl PriorityPlacement {
    pub fn from_text(value: &str) -> Self {
        match value.trim() {
            "优先置后" => Self::Last,
            _ => Self::First,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::First => "优先置前",
            Self::Last => "优先置后",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AdjacentCompareMode {
    Difference,
    ChangeRate,
    IncreaseFlag,
    DecreaseFlag,
}

impl AdjacentCompareMode {
    pub fn from_text(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "变化率" | "rate" => Self::ChangeRate,
            "是否上升" | "increase" => Self::IncreaseFlag,
            "是否下降" | "decrease" => Self::DecreaseFlag,
            _ => Self::Difference,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Difference => "差值",
            Self::ChangeRate => "变化率",
            Self::IncreaseFlag => "是否上升",
            Self::DecreaseFlag => "是否下降",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StatisticFillStrategy {
    Mean,
    Median,
    Mode,
}

impl StatisticFillStrategy {
    pub fn from_text(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "median" | "中位数" => Self::Median,
            "mode" | "众数" => Self::Mode,
            _ => Self::Mean,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Mean => "均值",
            Self::Median => "中位数",
            Self::Mode => "众数",
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
    KeepRowsWithMissing {
        columns: Vec<String>,
    },
    DropRowsWithMissing {
        columns: Vec<String>,
    },
    DropRowsNotContains {
        column: String,
        keyword: String,
    },
    DropRowRange {
        start: usize,
        end: usize,
    },
    DeduplicateByColumns {
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
    DropEmptyColumns,
    ReorderColumns {
        columns: Vec<String>,
    },
    AddColumnNameAffix {
        prefix: String,
        suffix: String,
    },
    DuplicateColumn {
        source: String,
        target: String,
    },
    MergeColumns {
        columns: Vec<String>,
        target: String,
        separator: String,
    },
    AddRowNumberColumn {
        column: String,
        start: usize,
    },
    AddConstantColumn {
        target: String,
        value: String,
    },
    AddExpressionColumn {
        target: String,
        expression: String,
    },
    AddConditionalColumn {
        target: String,
        source_column: String,
        operator: CompareOperator,
        compare_value: String,
        true_value: String,
        false_value: String,
    },
    ConcatColumns {
        columns: Vec<String>,
        target: String,
        separator: String,
    },
    AddTimeDiffColumn {
        start_column: String,
        end_column: String,
        target: String,
        unit: TimeDiffUnit,
    },
    SortBy {
        column: String,
        ascending: bool,
    },
    MultiSort {
        columns: Vec<String>,
        ascending: Vec<bool>,
    },
    PrioritySort {
        column: String,
        operator: CompareOperator,
        value: String,
        placement: PriorityPlacement,
        secondary_columns: Vec<String>,
        secondary_ascending: Vec<bool>,
    },
    AddRankColumn {
        target: String,
        columns: Vec<String>,
        ascending: Vec<bool>,
    },
    FillNullText {
        column: String,
        value: String,
    },
    FillNullForward {
        column: String,
    },
    FillNullBackward {
        column: String,
    },
    FillNullStatistic {
        column: String,
        strategy: StatisticFillStrategy,
    },
    EmptyStringToNull {
        column: String,
    },
    ZeroToNull {
        column: String,
    },
    ReplaceExactValue {
        column: String,
        from: String,
        to: String,
    },
    ConvertStringToNumeric {
        column: String,
    },
    ConvertStringToDateTime {
        column: String,
    },
    ConvertIntegerToFloat {
        column: String,
    },
    ConvertToBoolean {
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
    SqueezeTextWhitespace {
        column: String,
    },
    RemoveTextPattern {
        column: String,
        pattern: String,
    },
    ExtractTextBefore {
        column: String,
        delimiter: String,
    },
    ExtractTextAfter {
        column: String,
        delimiter: String,
    },
    KeepDigitsOnly {
        column: String,
    },
    AddTextAffix {
        column: String,
        prefix: String,
        suffix: String,
    },
    TruncateText {
        column: String,
        max_chars: usize,
    },
    RoundNumeric {
        column: String,
        digits: usize,
    },
    ScaleNumeric {
        column: String,
        factor: f64,
    },
    ShiftNumeric {
        column: String,
        offset: f64,
    },
    ClampNumeric {
        column: String,
        min: Option<f64>,
        max: Option<f64>,
    },
    NormalizeDateTimeFormat {
        column: String,
    },
    TimestampToDateTime {
        column: String,
    },
    ShiftDateTimeByMinutes {
        column: String,
        minutes: i64,
    },
    SplitDateTimeParts {
        column: String,
        prefix: String,
    },
    ExtractDateToColumn {
        column: String,
        target: String,
    },
    ExtractYearToColumn {
        column: String,
        target: String,
    },
    ExtractMonthToColumn {
        column: String,
        target: String,
    },
    ExtractDayToColumn {
        column: String,
        target: String,
    },
    ExtractHourToColumn {
        column: String,
        target: String,
    },
    FilterRowsByTimeWindow {
        column: String,
        start: String,
        end: String,
    },
    SortByDateTime {
        column: String,
        ascending: bool,
    },
    GroupAggregate {
        group_columns: Vec<String>,
        target_column: String,
        function: AggregateFunction,
    },
    RollingAggregate {
        group_columns: Vec<String>,
        order_column: String,
        target_column: String,
        window_size: usize,
        function: AggregateFunction,
        output_column: String,
    },
    CumulativeSum {
        group_columns: Vec<String>,
        order_column: String,
        target_column: String,
        output_column: String,
    },
    MovingAverage {
        group_columns: Vec<String>,
        order_column: String,
        target_column: String,
        window_size: usize,
        output_column: String,
    },
    CompareAdjacent {
        group_columns: Vec<String>,
        order_column: String,
        target_column: String,
        mode: AdjacentCompareMode,
        output_column: String,
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
            Self::KeepRowsWithMissing { columns } => {
                if columns.is_empty() {
                    write!(formatter, "保留含缺失值记录")
                } else {
                    write!(formatter, "保留缺失记录 {}", columns.join(", "))
                }
            }
            Self::DropRowsWithMissing { columns } => {
                if columns.is_empty() {
                    write!(formatter, "删除含缺失值记录")
                } else {
                    write!(formatter, "删除缺失记录 {}", columns.join(", "))
                }
            }
            Self::DropRowsNotContains { column, keyword } => {
                write!(formatter, "删除 {} 不包含 {}", column, keyword)
            }
            Self::DropRowRange { start, end } => write!(formatter, "删除行范围 {}-{}", start, end),
            Self::DeduplicateByColumns { columns } => {
                write!(formatter, "按列去重 {}", columns.join(", "))
            }
            Self::RenameColumn { from, to } => write!(formatter, "列重命名 {} -> {}", from, to),
            Self::KeepColumns { columns } => write!(formatter, "保留列 {}", columns.join(", ")),
            Self::DropColumns { columns } => write!(formatter, "删除列 {}", columns.join(", ")),
            Self::DropEmptyColumns => write!(formatter, "删除空列"),
            Self::ReorderColumns { columns } => write!(formatter, "调整列顺序 {}", columns.join(", ")),
            Self::AddColumnNameAffix { prefix, suffix } => {
                write!(formatter, "批量修改列名前后缀 {}..{}", prefix, suffix)
            }
            Self::DuplicateColumn { source, target } => {
                write!(formatter, "复制列 {} -> {}", source, target)
            }
            Self::MergeColumns { columns, target, .. } => {
                write!(formatter, "合并列 {} -> {}", columns.join(", "), target)
            }
            Self::AddRowNumberColumn { column, start } => {
                write!(formatter, "新增序号列 {} 从 {}", column, start)
            }
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
            Self::FillNullBackward { column } => write!(formatter, "后值填充 {}", column),
            Self::FillNullStatistic { column, strategy } => {
                write!(formatter, "统计值填充 {} -> {}", column, strategy.as_str())
            }
            Self::EmptyStringToNull { column } => write!(formatter, "空字符串转空值 {}", column),
            Self::ZeroToNull { column } => write!(formatter, "零值转空值 {}", column),
            Self::ReplaceExactValue { column, from, to } => {
                write!(formatter, "指定值替换 {}: {} -> {}", column, from, to)
            }
            Self::ConvertStringToNumeric { column } => write!(formatter, "字符串转数值 {}", column),
            Self::ConvertStringToDateTime { column } => write!(formatter, "字符串转日期 {}", column),
            Self::ConvertIntegerToFloat { column } => write!(formatter, "整型转浮点 {}", column),
            Self::ConvertToBoolean { column } => write!(formatter, "布尔值转换 {}", column),
            Self::CastColumn { column, target } => {
                write!(formatter, "类型转换 {} -> {}", column, target.as_str())
            }
            Self::TransformTextCase { column, mode } => {
                write!(formatter, "文本大小写统一 {} -> {}", column, mode.as_str())
            }
            Self::ReplaceText { column, from, to } => {
                write!(formatter, "文本替换 {}: {} -> {}", column, from, to)
            }
            Self::SqueezeTextWhitespace { column } => write!(formatter, "压缩空白 {}", column),
            Self::RemoveTextPattern { column, pattern } => {
                write!(formatter, "移除字符 {}: {}", column, pattern)
            }
            Self::ExtractTextBefore { column, delimiter } => {
                write!(formatter, "提取分隔符左侧 {} @ {}", column, delimiter)
            }
            Self::ExtractTextAfter { column, delimiter } => {
                write!(formatter, "提取分隔符右侧 {} @ {}", column, delimiter)
            }
            Self::KeepDigitsOnly { column } => write!(formatter, "仅保留数字 {}", column),
            Self::AddTextAffix { column, prefix, suffix } => {
                write!(formatter, "添加前后缀 {}: {}..{}", column, prefix, suffix)
            }
            Self::TruncateText { column, max_chars } => {
                write!(formatter, "文本截断 {} -> {} 字符", column, max_chars)
            }
            Self::RoundNumeric { column, digits } => {
                write!(formatter, "数值保留小数 {} -> {} 位", column, digits)
            }
            Self::ScaleNumeric { column, factor } => {
                write!(formatter, "数值缩放 {} × {}", column, factor)
            }
            Self::ShiftNumeric { column, offset } => {
                write!(formatter, "数值偏移 {} + {}", column, offset)
            }
            Self::ClampNumeric { column, min, max } => match (min, max) {
                (Some(min), Some(max)) => write!(formatter, "数值裁剪 {} [{} , {}]", column, min, max),
                (Some(min), None) => write!(formatter, "数值裁剪 {} >= {}", column, min),
                (None, Some(max)) => write!(formatter, "数值裁剪 {} <= {}", column, max),
                (None, None) => write!(formatter, "数值裁剪 {}", column),
            },
            Self::NormalizeDateTimeFormat { column } => {
                write!(formatter, "时间格式标准化 {}", column)
            }
            Self::TimestampToDateTime { column } => write!(formatter, "时间戳转换 {}", column),
            Self::ShiftDateTimeByMinutes { column, minutes } => {
                write!(formatter, "时间偏移 {} {} 分钟", column, minutes)
            }
            Self::SplitDateTimeParts { column, prefix } => {
                write!(formatter, "日期拆分 {} -> {}", column, prefix)
            }
            Self::ExtractDateToColumn { column, target } => {
                write!(formatter, "提取日期 {} -> {}", column, target)
            }
            Self::ExtractYearToColumn { column, target } => {
                write!(formatter, "提取年 {} -> {}", column, target)
            }
            Self::ExtractMonthToColumn { column, target } => {
                write!(formatter, "提取月 {} -> {}", column, target)
            }
            Self::ExtractDayToColumn { column, target } => {
                write!(formatter, "提取日 {} -> {}", column, target)
            }
            Self::ExtractHourToColumn { column, target } => {
                write!(formatter, "提取小时 {} -> {}", column, target)
            }
            Self::FilterRowsByTimeWindow { column, start, end } => {
                write!(formatter, "时间窗口筛选 {} [{} , {}]", column, start, end)
            }
            Self::SortByDateTime { column, ascending } => write!(
                formatter,
                "时间排序 {} {}",
                column,
                if *ascending { "升序" } else { "降序" }
            ),
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
            Self::AddConstantColumn { target, value } => {
                write!(formatter, "常量列 {} = {}", target, value)
            }
            Self::AddExpressionColumn { target, expression } => {
                write!(formatter, "表达式列 {} = {}", target, expression)
            }
            Self::AddConditionalColumn {
                target,
                source_column,
                operator,
                compare_value,
                ..
            } => write!(
                formatter,
                "条件判断列 {}: {} {} {}",
                target,
                source_column,
                operator.as_str(),
                compare_value
            ),
            Self::ConcatColumns { columns, target, .. } => {
                write!(formatter, "拼接列 {} -> {}", columns.join(", "), target)
            }
            Self::AddTimeDiffColumn {
                start_column,
                end_column,
                target,
                unit,
            } => write!(
                formatter,
                "时间差列 {} = {} - {} ({})",
                target,
                end_column,
                start_column,
                unit.as_str()
            ),
            Self::MultiSort { columns, ascending } => {
                let detail = columns
                    .iter()
                    .enumerate()
                    .map(|(index, column)| {
                        format!(
                            "{} {}",
                            column,
                            if ascending.get(index).copied().unwrap_or(true) {
                                "升序"
                            } else {
                                "降序"
                            }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(formatter, "多列排序 {}", detail)
            }
            Self::PrioritySort {
                column,
                operator,
                value,
                placement,
                ..
            } => write!(
                formatter,
                "条件优先排序 {} {} {} ({})",
                column,
                operator.as_str(),
                value,
                placement.as_str()
            ),
            Self::AddRankColumn { target, columns, .. } => {
                write!(formatter, "生成排名列 {} by {}", target, columns.join(", "))
            }
            Self::RollingAggregate {
                order_column,
                target_column,
                window_size,
                function,
                output_column,
                ..
            } => write!(
                formatter,
                "滚动统计 {} by {} -> {} (窗口 {}, {})",
                target_column,
                order_column,
                output_column,
                window_size,
                function.as_str()
            ),
            Self::CumulativeSum {
                order_column,
                target_column,
                output_column,
                ..
            } => write!(
                formatter,
                "累积和 {} by {} -> {}",
                target_column,
                order_column,
                output_column
            ),
            Self::MovingAverage {
                order_column,
                target_column,
                window_size,
                output_column,
                ..
            } => write!(
                formatter,
                "滑动平均 {} by {} -> {} (窗口 {})",
                target_column,
                order_column,
                output_column,
                window_size
            ),
            Self::CompareAdjacent {
                order_column,
                target_column,
                mode,
                output_column,
                ..
            } => write!(
                formatter,
                "邻近值比较 {} by {} -> {} ({})",
                target_column,
                order_column,
                output_column,
                mode.as_str()
            ),
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
    pub import_duration_ms: Option<u64>,
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
    pub import_duration: String,
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

pub fn format_duration_millis(duration_ms: Option<u64>) -> String {
    let Some(duration_ms) = duration_ms else {
        return "耗时 -".to_string();
    };

    if duration_ms < 1_000 {
        return format!("耗时 {} ms", duration_ms);
    }

    if duration_ms < 60_000 {
        return format!("耗时 {:.2} s", duration_ms as f64 / 1_000.0);
    }

    let minutes = duration_ms / 60_000;
    let seconds = (duration_ms % 60_000) as f64 / 1_000.0;
    format!("耗时 {} min {:.1} s", minutes, seconds)
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
