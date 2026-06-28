use crate::model::{
    AggregateFunction, DataTable, JoinKind, LogicalType, PipelineOperation, StatisticFillStrategy,
    TableColumn, TextCaseMode, infer_logical_type, normalize_headers, row_signature,
};
use anyhow::{Result, bail};
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, Timelike, Utc};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub fn apply_operation(table: &DataTable, operation: &PipelineOperation) -> Result<DataTable> {
    match operation {
        PipelineOperation::Reinspect => Ok(table.clone()),
        PipelineOperation::NormalizeColumnNames => Ok(normalize_column_names(table)),
        PipelineOperation::TrimTextValues => Ok(trim_text_values(table)),
        PipelineOperation::DropEmptyRows => Ok(drop_empty_rows(table)),
        PipelineOperation::DeduplicateRows => Ok(deduplicate_rows(table)),
        PipelineOperation::FilterRowsContains { column, keyword } => {
            filter_rows_contains(table, column, keyword)
        }
        PipelineOperation::KeepRowRange { start, end } => keep_row_range(table, *start, *end),
        PipelineOperation::KeepTopRows { count } => keep_top_rows(table, *count),
        PipelineOperation::SampleRows { count } => sample_rows(table, *count),
        PipelineOperation::KeepRowsWithMissing { columns } => keep_rows_with_missing(table, columns),
        PipelineOperation::DropRowsWithMissing { columns } => drop_rows_with_missing(table, columns),
        PipelineOperation::DropRowsNotContains { column, keyword } => {
            drop_rows_not_contains(table, column, keyword)
        }
        PipelineOperation::DropRowRange { start, end } => drop_row_range(table, *start, *end),
        PipelineOperation::DeduplicateByColumns { columns } => deduplicate_by_columns(table, columns),
        PipelineOperation::RenameColumn { from, to } => rename_column(table, from, to),
        PipelineOperation::KeepColumns { columns } => keep_columns(table, columns),
        PipelineOperation::DropColumns { columns } => drop_columns(table, columns),
        PipelineOperation::DropEmptyColumns => drop_empty_columns(table),
        PipelineOperation::ReorderColumns { columns } => reorder_columns(table, columns),
        PipelineOperation::AddColumnNameAffix { prefix, suffix } => {
            add_column_name_affix(table, prefix, suffix)
        }
        PipelineOperation::DuplicateColumn { source, target } => duplicate_column(table, source, target),
        PipelineOperation::MergeColumns {
            columns,
            target,
            separator,
        } => merge_columns(table, columns, target, separator),
        PipelineOperation::AddRowNumberColumn { column, start } => {
            add_row_number_column(table, column, *start)
        }
        PipelineOperation::SortBy { column, ascending } => sort_by_column(table, column, *ascending),
        PipelineOperation::FillNullText { column, value } => fill_null_text(table, column, value),
        PipelineOperation::FillNullForward { column } => fill_null_forward(table, column),
        PipelineOperation::FillNullBackward { column } => fill_null_backward(table, column),
        PipelineOperation::FillNullStatistic { column, strategy } => {
            fill_null_statistic(table, column, strategy)
        }
        PipelineOperation::EmptyStringToNull { column } => empty_string_to_null(table, column),
        PipelineOperation::ZeroToNull { column } => zero_to_null(table, column),
        PipelineOperation::ReplaceExactValue { column, from, to } => {
            replace_exact_value(table, column, from, to)
        }
        PipelineOperation::ConvertStringToNumeric { column } => convert_string_to_numeric(table, column),
        PipelineOperation::ConvertStringToDateTime { column } => {
            convert_string_to_datetime(table, column)
        }
        PipelineOperation::ConvertIntegerToFloat { column } => convert_integer_to_float(table, column),
        PipelineOperation::ConvertToBoolean { column } => convert_to_boolean(table, column),
        PipelineOperation::CastColumn { column, target } => cast_column(table, column, target),
        PipelineOperation::TransformTextCase { column, mode } => {
            transform_text_case(table, column, mode)
        }
        PipelineOperation::ReplaceText { column, from, to } => replace_text(table, column, from, to),
        PipelineOperation::SqueezeTextWhitespace { column } => squeeze_text_whitespace(table, column),
        PipelineOperation::RemoveTextPattern { column, pattern } => {
            remove_text_pattern(table, column, pattern)
        }
        PipelineOperation::ExtractTextBefore { column, delimiter } => {
            extract_text_before(table, column, delimiter)
        }
        PipelineOperation::ExtractTextAfter { column, delimiter } => {
            extract_text_after(table, column, delimiter)
        }
        PipelineOperation::KeepDigitsOnly { column } => keep_digits_only(table, column),
        PipelineOperation::AddTextAffix {
            column,
            prefix,
            suffix,
        } => add_text_affix(table, column, prefix, suffix),
        PipelineOperation::TruncateText { column, max_chars } => {
            truncate_text(table, column, *max_chars)
        }
        PipelineOperation::RoundNumeric { column, digits } => round_numeric(table, column, *digits),
        PipelineOperation::ScaleNumeric { column, factor } => scale_numeric(table, column, *factor),
        PipelineOperation::ShiftNumeric { column, offset } => shift_numeric(table, column, *offset),
        PipelineOperation::ClampNumeric { column, min, max } => clamp_numeric(table, column, *min, *max),
        PipelineOperation::NormalizeDateTimeFormat { column } => normalize_datetime_format(table, column),
        PipelineOperation::TimestampToDateTime { column } => convert_timestamp_to_datetime(table, column),
        PipelineOperation::ShiftDateTimeByMinutes { column, minutes } => {
            shift_datetime_by_minutes(table, column, *minutes)
        }
        PipelineOperation::SplitDateTimeParts { column, prefix } => {
            split_datetime_parts(table, column, prefix)
        }
        PipelineOperation::ExtractDateToColumn { column, target } => {
            extract_date_to_column(table, column, target)
        }
        PipelineOperation::ExtractYearToColumn { column, target } => {
            extract_year_to_column(table, column, target)
        }
        PipelineOperation::ExtractMonthToColumn { column, target } => {
            extract_month_to_column(table, column, target)
        }
        PipelineOperation::ExtractDayToColumn { column, target } => {
            extract_day_to_column(table, column, target)
        }
        PipelineOperation::ExtractHourToColumn { column, target } => {
            extract_hour_to_column(table, column, target)
        }
        PipelineOperation::FilterRowsByTimeWindow { column, start, end } => {
            filter_rows_by_time_window(table, column, start, end)
        }
        PipelineOperation::SortByDateTime { column, ascending } => {
            sort_by_datetime(table, column, *ascending)
        }
        PipelineOperation::GroupAggregate {
            group_columns,
            target_column,
            function,
        } => group_aggregate(table, group_columns, target_column, function),
        PipelineOperation::ApplyMappings { mappings } => apply_mappings(table, mappings),
    }
}

pub fn join_tables(
    left: &DataTable,
    right: &DataTable,
    left_keys: &[String],
    right_keys: &[String],
    join_kind: JoinKind,
) -> Result<DataTable> {
    if left_keys.is_empty() || right_keys.is_empty() || left_keys.len() != right_keys.len() {
        bail!("融合时需要成对指定左右主键");
    }

    let left_key_indexes = find_indexes(left, left_keys)?;
    let right_key_indexes = find_indexes(right, right_keys)?;

    let left_rows = to_rows(left);
    let right_rows = to_rows(right);

    let mut right_lookup = HashMap::<String, Vec<usize>>::new();
    for (index, row) in right_rows.iter().enumerate() {
        right_lookup
            .entry(compose_key(row, &right_key_indexes))
            .or_default()
            .push(index);
    }

    let mut output_headers = left.column_names();
    let mut output_types = left
        .columns
        .iter()
        .map(|column| column.logical_type.clone())
        .collect::<Vec<_>>();
    let right_key_names = right_keys.iter().collect::<BTreeSet<_>>();

    let right_extra_indexes = right
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            if right_key_names.contains(&column.name) {
                None
            } else {
                let mut output_name = column.name.clone();
                if output_headers.contains(&output_name) {
                    output_name.push_str("_right");
                }
                output_headers.push(output_name);
                output_types.push(column.logical_type.clone());
                Some(index)
            }
        })
        .collect::<Vec<_>>();

    let mut merged_rows = Vec::new();
    let mut matched_right = BTreeSet::new();

    for left_row in &left_rows {
        let key = compose_key(left_row, &left_key_indexes);
        if let Some(matches) = right_lookup.get(&key) {
            for right_index in matches {
                matched_right.insert(*right_index);
                let mut row = left_row.clone();
                for extra_index in &right_extra_indexes {
                    row.push(right_rows[*right_index][*extra_index].clone());
                }
                merged_rows.push(row);
            }
        } else if matches!(join_kind, JoinKind::Left | JoinKind::Outer) {
            let mut row = left_row.clone();
            row.extend((0..right_extra_indexes.len()).map(|_| None));
            merged_rows.push(row);
        }
    }

    if matches!(join_kind, JoinKind::Outer) {
        for (right_index, right_row) in right_rows.iter().enumerate() {
            if matched_right.contains(&right_index) {
                continue;
            }

            let mut row = vec![None; left.width()];
            for extra_index in &right_extra_indexes {
                row.push(right_row[*extra_index].clone());
            }
            merged_rows.push(row);
        }
    }

    Ok(build_table_from_rows(output_headers, output_types, merged_rows))
}

fn normalize_column_names(table: &DataTable) -> DataTable {
    let normalized = normalize_headers(&table.column_names());
    let mut columns = table.columns.clone();
    for (column, name) in columns.iter_mut().zip(normalized) {
        column.name = name;
    }
    DataTable { columns }
}

fn trim_text_values(table: &DataTable) -> DataTable {
    let mut columns = table.columns.clone();
    for column in &mut columns {
        if matches!(column.logical_type, LogicalType::Text | LogicalType::DateTime) {
            for value in &mut column.values {
                if let Some(text) = value {
                    let trimmed = text.trim().to_string();
                    *value = if trimmed.is_empty() { None } else { Some(trimmed) };
                }
            }
        }
    }
    DataTable { columns }
}

fn drop_empty_rows(table: &DataTable) -> DataTable {
    let keep_indexes = to_rows(table)
        .iter()
        .enumerate()
        .filter_map(|(index, row)| {
            let empty = row
                .iter()
                .all(|cell| cell.as_ref().map(|value| value.trim().is_empty()).unwrap_or(true));
            (!empty).then_some(index)
        })
        .collect::<Vec<_>>();
    select_rows(table, &keep_indexes)
}

fn deduplicate_rows(table: &DataTable) -> DataTable {
    let mut seen = BTreeSet::new();
    let keep_indexes = to_rows(table)
        .iter()
        .enumerate()
        .filter_map(|(index, row)| seen.insert(row_signature(row)).then_some(index))
        .collect::<Vec<_>>();
    select_rows(table, &keep_indexes)
}

fn filter_rows_contains(table: &DataTable, column: &str, keyword: &str) -> Result<DataTable> {
    if column.trim().is_empty() || keyword.trim().is_empty() {
        bail!("筛选时必须填写列名和关键字");
    }

    let index = find_index(table, column)?;
    let needle = keyword.trim().to_ascii_lowercase();
    let keep_indexes = table.columns[index]
        .values
        .iter()
        .enumerate()
        .filter_map(|(row_index, value)| {
            let matched = value
                .as_ref()
                .map(|cell| cell.to_ascii_lowercase().contains(&needle))
                .unwrap_or(false);
            matched.then_some(row_index)
        })
        .collect::<Vec<_>>();

    Ok(select_rows(table, &keep_indexes))
}

fn keep_row_range(table: &DataTable, start: usize, end: usize) -> Result<DataTable> {
    if start == 0 || end == 0 || end < start {
        bail!("行范围必须满足 1 <= 起始行 <= 结束行");
    }

    let keep_indexes = ((start - 1)..end.min(table.height()))
        .collect::<Vec<_>>();
    Ok(select_rows(table, &keep_indexes))
}

fn keep_top_rows(table: &DataTable, count: usize) -> Result<DataTable> {
    if count == 0 {
        bail!("保留前 N 行时，N 必须大于 0");
    }
    let keep_indexes = (0..count.min(table.height())).collect::<Vec<_>>();
    Ok(select_rows(table, &keep_indexes))
}

fn sample_rows(table: &DataTable, count: usize) -> Result<DataTable> {
    if count == 0 {
        bail!("抽样行数必须大于 0");
    }
    if count >= table.height() {
        return Ok(table.clone());
    }

    let max_index = table.height() - 1;
    let keep_indexes = (0..count)
        .map(|i| i * max_index / (count - 1).max(1))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    Ok(select_rows(table, &keep_indexes))
}

fn keep_rows_with_missing(table: &DataTable, columns: &[String]) -> Result<DataTable> {
    filter_rows_by_missing(table, columns, true)
}

fn drop_rows_with_missing(table: &DataTable, columns: &[String]) -> Result<DataTable> {
    filter_rows_by_missing(table, columns, false)
}

fn filter_rows_by_missing(table: &DataTable, columns: &[String], keep_missing: bool) -> Result<DataTable> {
    let target_indexes = if columns.is_empty() {
        (0..table.width()).collect::<Vec<_>>()
    } else {
        columns
            .iter()
            .map(|column| find_index(table, column))
            .collect::<Result<Vec<_>>>()?
    };

    let keep_indexes = (0..table.height())
        .filter(|row_index| {
            let has_missing = target_indexes.iter().any(|column_index| {
                table.columns[*column_index]
                    .values
                    .get(*row_index)
                    .and_then(|value| value.as_ref())
                    .map(|value| value.trim().is_empty())
                    .unwrap_or(true)
            });
            if keep_missing {
                has_missing
            } else {
                !has_missing
            }
        })
        .collect::<Vec<_>>();

    Ok(select_rows(table, &keep_indexes))
}

fn drop_rows_not_contains(table: &DataTable, column: &str, keyword: &str) -> Result<DataTable> {
    if column.trim().is_empty() || keyword.trim().is_empty() {
        bail!("删除匹配记录时必须填写列名和关键字");
    }

    let index = find_index(table, column)?;
    let needle = keyword.trim().to_ascii_lowercase();
    let keep_indexes = table.columns[index]
        .values
        .iter()
        .enumerate()
        .filter_map(|(row_index, value)| {
            let matched = value
                .as_ref()
                .map(|cell| cell.to_ascii_lowercase().contains(&needle))
                .unwrap_or(false);
            (!matched).then_some(row_index)
        })
        .collect::<Vec<_>>();

    Ok(select_rows(table, &keep_indexes))
}

fn drop_row_range(table: &DataTable, start: usize, end: usize) -> Result<DataTable> {
    if start == 0 || end == 0 || end < start {
        bail!("删除行范围必须满足 1 <= 起始行 <= 结束行");
    }

    let keep_indexes = (0..table.height())
        .filter(|row_index| {
            let line = row_index + 1;
            line < start || line > end
        })
        .collect::<Vec<_>>();

    Ok(select_rows(table, &keep_indexes))
}

fn deduplicate_by_columns(table: &DataTable, columns: &[String]) -> Result<DataTable> {
    if columns.is_empty() {
        bail!("按列去重时至少需要一个字段");
    }

    let indexes = find_indexes(table, columns)?;
    let mut seen = BTreeSet::new();
    let keep_indexes = to_rows(table)
        .iter()
        .enumerate()
        .filter_map(|(row_index, row)| {
            let key = compose_key(row, &indexes);
            seen.insert(key).then_some(row_index)
        })
        .collect::<Vec<_>>();

    Ok(select_rows(table, &keep_indexes))
}

fn rename_column(table: &DataTable, from: &str, to: &str) -> Result<DataTable> {
    if from.trim().is_empty() || to.trim().is_empty() {
        bail!("重命名时列名不能为空");
    }

    let mut columns = table.columns.clone();
    let Some(column) = columns.iter_mut().find(|column| column.name == from) else {
        bail!("未找到列: {from}");
    };
    column.name = to.trim().to_string();
    Ok(DataTable { columns })
}

fn keep_columns(table: &DataTable, columns: &[String]) -> Result<DataTable> {
    if columns.is_empty() {
        bail!("保留列列表不能为空");
    }
    let allowed = columns.iter().collect::<BTreeSet<_>>();
    let next = table
        .columns
        .iter()
        .filter(|column| allowed.contains(&column.name))
        .cloned()
        .collect::<Vec<_>>();

    if next.is_empty() {
        bail!("保留列后结果为空");
    }

    Ok(DataTable { columns: next })
}

fn drop_columns(table: &DataTable, columns: &[String]) -> Result<DataTable> {
    if columns.is_empty() {
        bail!("删除列列表不能为空");
    }
    let blocked = columns.iter().collect::<BTreeSet<_>>();
    let next = table
        .columns
        .iter()
        .filter(|column| !blocked.contains(&column.name))
        .cloned()
        .collect::<Vec<_>>();

    if next.is_empty() {
        bail!("删除列后结果为空");
    }

    Ok(DataTable { columns: next })
}

fn drop_empty_columns(table: &DataTable) -> Result<DataTable> {
    let next = table
        .columns
        .iter()
        .filter(|column| {
            column
                .values
                .iter()
                .any(|value| value.as_ref().map(|cell| !cell.trim().is_empty()).unwrap_or(false))
        })
        .cloned()
        .collect::<Vec<_>>();

    if next.is_empty() {
        bail!("删除空列后结果为空");
    }

    Ok(DataTable { columns: next })
}

fn reorder_columns(table: &DataTable, columns: &[String]) -> Result<DataTable> {
    if columns.is_empty() {
        bail!("调整列顺序时至少需要一列");
    }

    let mut ordered = Vec::with_capacity(table.width());
    let mut used = BTreeSet::new();
    for name in columns {
        let index = find_index(table, name)?;
        if used.insert(index) {
            ordered.push(table.columns[index].clone());
        }
    }

    for (index, column) in table.columns.iter().enumerate() {
        if used.insert(index) {
            ordered.push(column.clone());
        }
    }

    Ok(DataTable { columns: ordered })
}

fn add_column_name_affix(table: &DataTable, prefix: &str, suffix: &str) -> Result<DataTable> {
    if prefix.is_empty() && suffix.is_empty() {
        bail!("列名前后缀至少需要填写一个");
    }

    let mut columns = table.columns.clone();
    for column in &mut columns {
        column.name = format!("{prefix}{}{suffix}", column.name);
    }
    Ok(DataTable { columns })
}

fn duplicate_column(table: &DataTable, source: &str, target: &str) -> Result<DataTable> {
    let target = target.trim();
    if source.trim().is_empty() || target.is_empty() {
        bail!("复制列时必须填写来源列和新列名");
    }
    if table.columns.iter().any(|column| column.name == target) {
        bail!("目标列已存在: {target}");
    }

    let source_index = find_index(table, source)?;
    let mut columns = table.columns.clone();
    let mut duplicated = columns[source_index].clone();
    duplicated.name = target.to_string();
    columns.push(duplicated);
    Ok(DataTable { columns })
}

fn merge_columns(table: &DataTable, columns: &[String], target: &str, separator: &str) -> Result<DataTable> {
    let target = target.trim();
    if columns.len() < 2 {
        bail!("合并列时至少需要两个来源列");
    }
    if target.is_empty() {
        bail!("合并列时必须填写目标列名");
    }
    if table.columns.iter().any(|column| column.name == target) {
        bail!("目标列已存在: {target}");
    }

    let indexes = find_indexes(table, columns)?;
    let mut merged_values = Vec::with_capacity(table.height());
    for row_index in 0..table.height() {
        let parts = indexes
            .iter()
            .filter_map(|column_index| table.columns[*column_index].values.get(row_index))
            .filter_map(|value| value.clone())
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>();

        if parts.is_empty() {
            merged_values.push(None);
        } else {
            merged_values.push(Some(parts.join(separator)));
        }
    }

    let mut next = table.columns.clone();
    next.push(TableColumn {
        name: target.to_string(),
        logical_type: LogicalType::Text,
        values: merged_values,
    });
    Ok(DataTable { columns: next })
}

fn add_row_number_column(table: &DataTable, column: &str, start: usize) -> Result<DataTable> {
    let column = column.trim();
    if column.is_empty() {
        bail!("序号列名不能为空");
    }
    if table.columns.iter().any(|entry| entry.name == column) {
        bail!("序号列已存在: {column}");
    }

    let values = (0..table.height())
        .map(|index| Some((start + index).to_string()))
        .collect::<Vec<_>>();

    let mut next = table.columns.clone();
    next.push(TableColumn {
        name: column.to_string(),
        logical_type: LogicalType::Integer,
        values,
    });
    Ok(DataTable { columns: next })
}

fn sort_by_column(table: &DataTable, column: &str, ascending: bool) -> Result<DataTable> {
    let column_index = find_index(table, column)?;
    let mut indexes = (0..table.height()).collect::<Vec<_>>();
    let logical_type = table.columns[column_index].logical_type.clone();
    indexes.sort_by(|left, right| {
        let left_value = table.columns[column_index].values[*left].clone();
        let right_value = table.columns[column_index].values[*right].clone();
        let order = compare_values(&left_value, &right_value, &logical_type);
        if ascending {
            order
        } else {
            order.reverse()
        }
    });

    Ok(select_rows(table, &indexes))
}

fn fill_null_text(table: &DataTable, column: &str, value: &str) -> Result<DataTable> {
    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    for cell in &mut target.values {
        if cell
            .as_ref()
            .map(|value| value.trim().is_empty())
            .unwrap_or(true)
        {
            *cell = Some(value.to_string());
        }
    }

    Ok(DataTable { columns })
}

fn fill_null_forward(table: &DataTable, column: &str) -> Result<DataTable> {
    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    let mut last_value: Option<String> = None;
    for cell in &mut target.values {
        if let Some(current) = cell.as_ref().filter(|value| !value.trim().is_empty()) {
            last_value = Some(current.clone());
        } else if let Some(previous) = last_value.clone() {
            *cell = Some(previous);
        }
    }

    Ok(DataTable { columns })
}

fn fill_null_backward(table: &DataTable, column: &str) -> Result<DataTable> {
    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    let mut next_value: Option<String> = None;
    for cell in target.values.iter_mut().rev() {
        if let Some(current) = cell.as_ref().filter(|value| !value.trim().is_empty()) {
            next_value = Some(current.clone());
        } else if let Some(value) = next_value.clone() {
            *cell = Some(value);
        }
    }

    Ok(DataTable { columns })
}

fn fill_null_statistic(
    table: &DataTable,
    column: &str,
    strategy: &StatisticFillStrategy,
) -> Result<DataTable> {
    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    let fill_value = compute_statistic_fill_value(target, strategy)?;
    for cell in &mut target.values {
        if cell.as_ref().map(|value| value.trim().is_empty()).unwrap_or(true) {
            *cell = Some(fill_value.clone());
        }
    }

    Ok(DataTable { columns })
}

fn empty_string_to_null(table: &DataTable, column: &str) -> Result<DataTable> {
    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    for cell in &mut target.values {
        if cell.as_ref().map(|value| value.trim().is_empty()).unwrap_or(false) {
            *cell = None;
        }
    }

    Ok(DataTable { columns })
}

fn zero_to_null(table: &DataTable, column: &str) -> Result<DataTable> {
    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    for cell in &mut target.values {
        if cell
            .as_ref()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .map(|value| value == 0.0)
            .unwrap_or(false)
        {
            *cell = None;
        }
    }

    Ok(DataTable { columns })
}

fn replace_exact_value(table: &DataTable, column: &str, from: &str, to: &str) -> Result<DataTable> {
    if from.trim().is_empty() {
        bail!("原始值不能为空");
    }

    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };
    let replacement = (!to.trim().is_empty()).then_some(to.to_string());

    for cell in &mut target.values {
        if cell.as_ref().map(|value| value.trim() == from.trim()).unwrap_or(false) {
            *cell = replacement.clone();
        }
    }

    Ok(DataTable { columns })
}

fn convert_string_to_numeric(table: &DataTable, column: &str) -> Result<DataTable> {
    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    for cell in &mut target.values {
        if let Some(value) = cell {
            if let Some(parsed) = parse_numeric_text(value) {
                *value = parsed.to_string();
            }
        }
    }

    target.logical_type = LogicalType::Float;
    Ok(DataTable { columns })
}

fn convert_string_to_datetime(table: &DataTable, column: &str) -> Result<DataTable> {
    normalize_datetime_format(table, column)
}

fn convert_integer_to_float(table: &DataTable, column: &str) -> Result<DataTable> {
    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    for cell in &mut target.values {
        if let Some(value) = cell {
            if let Ok(parsed) = value.trim().parse::<i64>() {
                *value = format!("{parsed}.0");
            }
        }
    }

    target.logical_type = LogicalType::Float;
    Ok(DataTable { columns })
}

fn convert_to_boolean(table: &DataTable, column: &str) -> Result<DataTable> {
    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    for cell in &mut target.values {
        if let Some(value) = cell {
            if let Some(parsed) = parse_bool_text(value) {
                *value = parsed.to_string();
            }
        }
    }

    target.logical_type = LogicalType::Boolean;
    Ok(DataTable { columns })
}

fn cast_column(table: &DataTable, column: &str, target: &LogicalType) -> Result<DataTable> {
    let mut columns = table.columns.clone();
    let Some(current) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };
    current.logical_type = target.clone();
    Ok(DataTable { columns })
}

fn transform_text_case(table: &DataTable, column: &str, mode: &TextCaseMode) -> Result<DataTable> {
    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    for cell in &mut target.values {
        if let Some(value) = cell {
            *value = match mode {
                TextCaseMode::Upper => value.to_uppercase(),
                TextCaseMode::Lower => value.to_lowercase(),
            };
        }
    }

    Ok(DataTable { columns })
}

fn replace_text(table: &DataTable, column: &str, from: &str, to: &str) -> Result<DataTable> {
    if from.is_empty() {
        bail!("替换前文本不能为空");
    }

    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    for cell in &mut target.values {
        if let Some(value) = cell {
            *value = value.replace(from, to);
        }
    }

    Ok(DataTable { columns })
}

fn squeeze_text_whitespace(table: &DataTable, column: &str) -> Result<DataTable> {
    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    for cell in &mut target.values {
        if let Some(value) = cell {
            let squeezed = value.split_whitespace().collect::<Vec<_>>().join(" ");
            *cell = if squeezed.is_empty() { None } else { Some(squeezed) };
        }
    }

    Ok(DataTable { columns })
}

fn remove_text_pattern(table: &DataTable, column: &str, pattern: &str) -> Result<DataTable> {
    if pattern.is_empty() {
        bail!("移除字符不能为空");
    }

    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    for cell in &mut target.values {
        if let Some(value) = cell {
            *value = value.replace(pattern, "");
        }
    }

    Ok(DataTable { columns })
}

fn extract_text_before(table: &DataTable, column: &str, delimiter: &str) -> Result<DataTable> {
    extract_text_by_delimiter(table, column, delimiter, true)
}

fn extract_text_after(table: &DataTable, column: &str, delimiter: &str) -> Result<DataTable> {
    extract_text_by_delimiter(table, column, delimiter, false)
}

fn extract_text_by_delimiter(
    table: &DataTable,
    column: &str,
    delimiter: &str,
    take_before: bool,
) -> Result<DataTable> {
    if delimiter.is_empty() {
        bail!("分隔符不能为空");
    }

    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    for cell in &mut target.values {
        if let Some(value) = cell {
            let next = if let Some((left, right)) = value.split_once(delimiter) {
                if take_before { left.trim() } else { right.trim() }
            } else {
                value.trim()
            };
            *cell = if next.is_empty() { None } else { Some(next.to_string()) };
        }
    }

    Ok(DataTable { columns })
}

fn keep_digits_only(table: &DataTable, column: &str) -> Result<DataTable> {
    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    for cell in &mut target.values {
        if let Some(value) = cell {
            let digits = value.chars().filter(|ch| ch.is_ascii_digit()).collect::<String>();
            *cell = if digits.is_empty() { None } else { Some(digits) };
        }
    }

    Ok(DataTable { columns })
}

fn add_text_affix(table: &DataTable, column: &str, prefix: &str, suffix: &str) -> Result<DataTable> {
    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    for cell in &mut target.values {
        if let Some(value) = cell {
            *value = format!("{prefix}{value}{suffix}");
        }
    }

    Ok(DataTable { columns })
}

fn truncate_text(table: &DataTable, column: &str, max_chars: usize) -> Result<DataTable> {
    if max_chars == 0 {
        bail!("截断长度必须大于 0");
    }

    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    for cell in &mut target.values {
        if let Some(value) = cell {
            *value = value.chars().take(max_chars).collect();
        }
    }

    Ok(DataTable { columns })
}

fn round_numeric(table: &DataTable, column: &str, digits: usize) -> Result<DataTable> {
    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    for cell in &mut target.values {
        if let Some(value) = cell {
            if let Ok(number) = value.trim().parse::<f64>() {
                *value = format!("{number:.digits$}");
            }
        }
    }

    target.logical_type = LogicalType::Float;
    Ok(DataTable { columns })
}

fn scale_numeric(table: &DataTable, column: &str, factor: f64) -> Result<DataTable> {
    apply_numeric_transform(table, column, |value| value * factor)
}

fn shift_numeric(table: &DataTable, column: &str, offset: f64) -> Result<DataTable> {
    apply_numeric_transform(table, column, |value| value + offset)
}

fn clamp_numeric(table: &DataTable, column: &str, min: Option<f64>, max: Option<f64>) -> Result<DataTable> {
    if min.is_none() && max.is_none() {
        bail!("数值裁剪至少需要填写最小值或最大值");
    }
    if let (Some(min), Some(max)) = (min, max) && min > max {
        bail!("最小值不能大于最大值");
    }

    apply_numeric_transform(table, column, |value| {
        let lower = min.map(|limit| value.max(limit)).unwrap_or(value);
        max.map(|limit| lower.min(limit)).unwrap_or(lower)
    })
}

fn apply_numeric_transform<F>(table: &DataTable, column: &str, transform: F) -> Result<DataTable>
where
    F: Fn(f64) -> f64,
{
    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    for cell in &mut target.values {
        if let Some(value) = cell {
            if let Ok(number) = value.trim().parse::<f64>() {
                *value = format!("{}", transform(number));
            }
        }
    }

    target.logical_type = LogicalType::Float;
    Ok(DataTable { columns })
}

fn normalize_datetime_format(table: &DataTable, column: &str) -> Result<DataTable> {
    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    for cell in &mut target.values {
        if let Some(value) = cell {
            if let Some(parsed) = parse_datetime_or_timestamp(value.trim()) {
                *value = parsed.format("%Y-%m-%d %H:%M:%S").to_string();
            }
        }
    }

    target.logical_type = LogicalType::DateTime;
    Ok(DataTable { columns })
}

fn convert_timestamp_to_datetime(table: &DataTable, column: &str) -> Result<DataTable> {
    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    for cell in &mut target.values {
        if let Some(value) = cell {
            if let Some(parsed) = parse_timestamp_value(value.trim()) {
                *value = parsed.format("%Y-%m-%d %H:%M:%S").to_string();
            }
        }
    }

    target.logical_type = LogicalType::DateTime;
    Ok(DataTable { columns })
}

fn shift_datetime_by_minutes(table: &DataTable, column: &str, minutes: i64) -> Result<DataTable> {
    let mut columns = table.columns.clone();
    let Some(target) = columns.iter_mut().find(|entry| entry.name == column) else {
        bail!("未找到列: {column}");
    };

    for cell in &mut target.values {
        if let Some(value) = cell {
            if let Some(parsed) = parse_datetime_or_timestamp(value.trim()) {
                *value = (parsed + Duration::minutes(minutes))
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string();
            }
        }
    }

    target.logical_type = LogicalType::DateTime;
    Ok(DataTable { columns })
}

fn split_datetime_parts(table: &DataTable, column: &str, prefix: &str) -> Result<DataTable> {
    let prefix = if prefix.trim().is_empty() {
        column.trim().to_string()
    } else {
        prefix.trim().to_string()
    };
    ensure_time_targets_absent(table, &[format!("{prefix}_year"), format!("{prefix}_month"), format!("{prefix}_day"), format!("{prefix}_hour")])?;

    let source_index = find_index(table, column)?;
    let source_values = &table.columns[source_index].values;
    let year_values = source_values
        .iter()
        .map(|value| value.as_ref().and_then(|cell| parse_datetime_or_timestamp(cell.trim())).map(|parsed| parsed.year().to_string()))
        .collect::<Vec<_>>();
    let month_values = source_values
        .iter()
        .map(|value| value.as_ref().and_then(|cell| parse_datetime_or_timestamp(cell.trim())).map(|parsed| parsed.month().to_string()))
        .collect::<Vec<_>>();
    let day_values = source_values
        .iter()
        .map(|value| value.as_ref().and_then(|cell| parse_datetime_or_timestamp(cell.trim())).map(|parsed| parsed.day().to_string()))
        .collect::<Vec<_>>();
    let hour_values = source_values
        .iter()
        .map(|value| value.as_ref().and_then(|cell| parse_datetime_or_timestamp(cell.trim())).map(|parsed| parsed.hour().to_string()))
        .collect::<Vec<_>>();

    let mut columns = table.columns.clone();
    columns.push(TableColumn { name: format!("{prefix}_year"), logical_type: LogicalType::Integer, values: year_values });
    columns.push(TableColumn { name: format!("{prefix}_month"), logical_type: LogicalType::Integer, values: month_values });
    columns.push(TableColumn { name: format!("{prefix}_day"), logical_type: LogicalType::Integer, values: day_values });
    columns.push(TableColumn { name: format!("{prefix}_hour"), logical_type: LogicalType::Integer, values: hour_values });
    Ok(DataTable { columns })
}

fn extract_date_to_column(table: &DataTable, column: &str, target: &str) -> Result<DataTable> {
    let target = target.trim();
    if target.is_empty() {
        bail!("目标列名不能为空");
    }
    if table.columns.iter().any(|entry| entry.name == target) {
        bail!("目标列已存在: {target}");
    }
    let source_index = find_index(table, column)?;
    let values = table.columns[source_index]
        .values
        .iter()
        .map(|value| {
            value
                .as_ref()
                .and_then(|cell| parse_datetime_or_timestamp(cell.trim()))
                .map(|parsed| parsed.date().format("%Y-%m-%d").to_string())
        })
        .collect::<Vec<_>>();

    let mut columns = table.columns.clone();
    columns.push(TableColumn {
        name: target.to_string(),
        logical_type: LogicalType::DateTime,
        values,
    });
    Ok(DataTable { columns })
}

fn extract_year_to_column(table: &DataTable, column: &str, target: &str) -> Result<DataTable> {
    extract_datetime_part_to_column(table, column, target, |parsed| parsed.year().to_string(), LogicalType::Integer)
}

fn extract_month_to_column(table: &DataTable, column: &str, target: &str) -> Result<DataTable> {
    extract_datetime_part_to_column(table, column, target, |parsed| parsed.month().to_string(), LogicalType::Integer)
}

fn extract_day_to_column(table: &DataTable, column: &str, target: &str) -> Result<DataTable> {
    extract_datetime_part_to_column(table, column, target, |parsed| parsed.day().to_string(), LogicalType::Integer)
}

fn extract_hour_to_column(table: &DataTable, column: &str, target: &str) -> Result<DataTable> {
    extract_datetime_part_to_column(table, column, target, |parsed| parsed.hour().to_string(), LogicalType::Integer)
}

fn filter_rows_by_time_window(table: &DataTable, column: &str, start: &str, end: &str) -> Result<DataTable> {
    if start.trim().is_empty() || end.trim().is_empty() {
        bail!("时间窗口筛选必须填写开始时间和结束时间");
    }
    let start = parse_datetime_or_timestamp(start.trim()).ok_or_else(|| anyhow::anyhow!("无法解析开始时间: {start}"))?;
    let end = parse_datetime_or_timestamp(end.trim()).ok_or_else(|| anyhow::anyhow!("无法解析结束时间: {end}"))?;
    if start > end {
        bail!("开始时间不能晚于结束时间");
    }

    let source_index = find_index(table, column)?;
    let keep_indexes = table.columns[source_index]
        .values
        .iter()
        .enumerate()
        .filter_map(|(row_index, value)| {
            value.as_ref()
                .and_then(|cell| parse_datetime_or_timestamp(cell.trim()))
                .filter(|parsed| *parsed >= start && *parsed <= end)
                .map(|_| row_index)
        })
        .collect::<Vec<_>>();

    Ok(select_rows(table, &keep_indexes))
}

fn sort_by_datetime(table: &DataTable, column: &str, ascending: bool) -> Result<DataTable> {
    let column_index = find_index(table, column)?;
    let mut indexes = (0..table.height()).collect::<Vec<_>>();
    indexes.sort_by(|left, right| {
        let left_value = table.columns[column_index]
            .values
            .get(*left)
            .and_then(|value| value.as_ref())
            .and_then(|value| parse_datetime_or_timestamp(value.trim()));
        let right_value = table.columns[column_index]
            .values
            .get(*right)
            .and_then(|value| value.as_ref())
            .and_then(|value| parse_datetime_or_timestamp(value.trim()));
        let order = left_value.cmp(&right_value);
        if ascending { order } else { order.reverse() }
    });

    Ok(select_rows(table, &indexes))
}

fn group_aggregate(
    table: &DataTable,
    group_columns: &[String],
    target_column: &str,
    function: &AggregateFunction,
) -> Result<DataTable> {
    if group_columns.is_empty() {
        bail!("分组字段不能为空");
    }

    let group_indexes = find_indexes(table, group_columns)?;
    let target_index = if matches!(function, AggregateFunction::Count) && target_column.trim().is_empty() {
        None
    } else {
        Some(find_index(table, target_column.trim())?)
    };

    let rows = to_rows(table);
    let mut groups = BTreeMap::<String, Vec<Vec<Option<String>>>>::new();
    for row in rows {
        groups
            .entry(compose_key(&row, &group_indexes))
            .or_default()
            .push(row);
    }

    let mut output_rows = Vec::with_capacity(groups.len());
    for rows in groups.values() {
        let Some(first_row) = rows.first() else {
            continue;
        };

        let mut output_row = group_indexes
            .iter()
            .map(|index| first_row.get(*index).cloned().unwrap_or(None))
            .collect::<Vec<_>>();
        output_row.push(compute_aggregate_value(rows, target_index, function));
        output_rows.push(output_row);
    }

    let mut headers = group_columns.to_vec();
    headers.push(aggregate_column_name(target_column, function));

    let mut logical_types = group_indexes
        .iter()
        .map(|index| table.columns[*index].logical_type.clone())
        .collect::<Vec<_>>();
    logical_types.push(match function {
        AggregateFunction::Count | AggregateFunction::CountDistinct => LogicalType::Integer,
        AggregateFunction::Mean => LogicalType::Float,
        AggregateFunction::Sum => LogicalType::Float,
        AggregateFunction::Max | AggregateFunction::Min => target_index
            .and_then(|index| table.columns.get(index))
            .map(|column| column.logical_type.clone())
            .unwrap_or(LogicalType::Text),
    });

    Ok(build_table_from_rows(headers, logical_types, output_rows))
}

fn apply_mappings(table: &DataTable, mappings: &[(String, String)]) -> Result<DataTable> {
    let mut columns = table.columns.clone();
    for (source, target) in mappings {
        if let Some(column) = columns.iter_mut().find(|entry| entry.name == *source) {
            column.name = target.clone();
        }
    }
    Ok(DataTable { columns })
}

fn select_rows(table: &DataTable, indexes: &[usize]) -> DataTable {
    let columns = table
        .columns
        .iter()
        .map(|column| TableColumn {
            name: column.name.clone(),
            logical_type: column.logical_type.clone(),
            values: indexes
                .iter()
                .filter_map(|index| column.values.get(*index).cloned())
                .collect(),
        })
        .collect::<Vec<_>>();

    DataTable { columns }
}

fn to_rows(table: &DataTable) -> Vec<Vec<Option<String>>> {
    (0..table.height()).map(|index| table.row(index)).collect()
}

fn compare_values(
    left: &Option<String>,
    right: &Option<String>,
    logical_type: &LogicalType,
) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(left), Some(right)) => match logical_type {
            LogicalType::Integer => left.parse::<i64>().ok().cmp(&right.parse::<i64>().ok()),
            LogicalType::Float => left
                .parse::<f64>()
                .ok()
                .partial_cmp(&right.parse::<f64>().ok())
                .unwrap_or(Ordering::Equal),
            LogicalType::Boolean => left.cmp(right),
            LogicalType::DateTime | LogicalType::Text => left.cmp(right),
        },
    }
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

fn find_index(table: &DataTable, name: &str) -> Result<usize> {
    table
        .columns
        .iter()
        .position(|column| column.name == name)
        .ok_or_else(|| anyhow::anyhow!("未找到列: {name}"))
}

fn find_indexes(table: &DataTable, names: &[String]) -> Result<Vec<usize>> {
    names.iter().map(|name| find_index(table, name)).collect()
}

fn compose_key(row: &[Option<String>], indexes: &[usize]) -> String {
    indexes
        .iter()
        .map(|index| row.get(*index).cloned().unwrap_or(None).unwrap_or_default())
        .collect::<Vec<_>>()
        .join("|")
}

fn compute_aggregate_value(
    rows: &[Vec<Option<String>>],
    target_index: Option<usize>,
    function: &AggregateFunction,
) -> Option<String> {
    match function {
        AggregateFunction::Count => Some(rows.len().to_string()),
        AggregateFunction::CountDistinct => {
            let Some(target_index) = target_index else {
                return None;
            };
            let distinct = rows
                .iter()
                .filter_map(|row| row.get(target_index).cloned().unwrap_or(None))
                .filter(|value| !value.trim().is_empty())
                .collect::<BTreeSet<_>>();
            Some(distinct.len().to_string())
        }
        AggregateFunction::Sum | AggregateFunction::Mean => {
            let Some(target_index) = target_index else {
                return None;
            };
            let values = rows
                .iter()
                .filter_map(|row| row.get(target_index).cloned().unwrap_or(None))
                .filter_map(|value| value.trim().parse::<f64>().ok())
                .collect::<Vec<_>>();

            if values.is_empty() {
                return None;
            }

            let sum = values.iter().sum::<f64>();
            let result = if matches!(function, AggregateFunction::Mean) {
                sum / values.len() as f64
            } else {
                sum
            };
            Some(format!("{result:.4}"))
        }
        AggregateFunction::Max | AggregateFunction::Min => {
            let Some(target_index) = target_index else {
                return None;
            };
            let mut values = rows
                .iter()
                .filter_map(|row| row.get(target_index).cloned().unwrap_or(None))
                .filter(|value| !value.trim().is_empty())
                .collect::<Vec<_>>();

            if values.is_empty() {
                return None;
            }

            values.sort();
            if matches!(function, AggregateFunction::Max) {
                values.pop()
            } else {
                values.into_iter().next()
            }
        }
    }
}

fn aggregate_column_name(target_column: &str, function: &AggregateFunction) -> String {
    match function {
        AggregateFunction::Count => "count".to_string(),
        AggregateFunction::CountDistinct => format!("nunique_{}", target_column.trim()),
        AggregateFunction::Sum => format!("sum_{}", target_column.trim()),
        AggregateFunction::Mean => format!("mean_{}", target_column.trim()),
        AggregateFunction::Max => format!("max_{}", target_column.trim()),
        AggregateFunction::Min => format!("min_{}", target_column.trim()),
    }
}

fn compute_statistic_fill_value(
    column: &TableColumn,
    strategy: &StatisticFillStrategy,
) -> Result<String> {
    let non_empty = column
        .values
        .iter()
        .filter_map(|value| value.as_ref())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if non_empty.is_empty() {
        bail!("统计值填充前该列没有可用样本");
    }

    match strategy {
        StatisticFillStrategy::Mean => {
            let values = non_empty
                .iter()
                .filter_map(|value| value.parse::<f64>().ok())
                .collect::<Vec<_>>();
            if values.is_empty() {
                bail!("均值填充要求目标列至少包含可解析数值");
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            Ok(format!("{mean}"))
        }
        StatisticFillStrategy::Median => {
            let mut values = non_empty
                .iter()
                .filter_map(|value| value.parse::<f64>().ok())
                .collect::<Vec<_>>();
            if values.is_empty() {
                bail!("中位数填充要求目标列至少包含可解析数值");
            }
            values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
            let middle = values.len() / 2;
            let median = if values.len() % 2 == 0 {
                (values[middle - 1] + values[middle]) / 2.0
            } else {
                values[middle]
            };
            Ok(format!("{median}"))
        }
        StatisticFillStrategy::Mode => {
            let mut counts = BTreeMap::<String, usize>::new();
            for value in non_empty {
                *counts.entry(value).or_insert(0) += 1;
            }
            counts
                .into_iter()
                .max_by(|left, right| left.1.cmp(&right.1).then_with(|| right.0.cmp(&left.0)))
                .map(|entry| entry.0)
                .ok_or_else(|| anyhow::anyhow!("众数填充计算失败"))
        }
    }
}

fn ensure_time_targets_absent(table: &DataTable, targets: &[String]) -> Result<()> {
    for target in targets {
        if table.columns.iter().any(|entry| entry.name == *target) {
            bail!("目标列已存在: {target}");
        }
    }
    Ok(())
}

fn extract_datetime_part_to_column<F>(
    table: &DataTable,
    column: &str,
    target: &str,
    mapper: F,
    logical_type: LogicalType,
) -> Result<DataTable>
where
    F: Fn(NaiveDateTime) -> String,
{
    let target = target.trim();
    if target.is_empty() {
        bail!("目标列名不能为空");
    }
    if table.columns.iter().any(|entry| entry.name == target) {
        bail!("目标列已存在: {target}");
    }
    let source_index = find_index(table, column)?;
    let values = table.columns[source_index]
        .values
        .iter()
        .map(|value| {
            value
                .as_ref()
                .and_then(|cell| parse_datetime_or_timestamp(cell.trim()))
                .map(&mapper)
        })
        .collect::<Vec<_>>();

    let mut columns = table.columns.clone();
    columns.push(TableColumn {
        name: target.to_string(),
        logical_type,
        values,
    });
    Ok(DataTable { columns })
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

    for pattern in DATETIME_PATTERNS {
        if let Ok(value) = NaiveDateTime::parse_from_str(value, pattern) {
            return Some(value);
        }
        if let Ok(value) = NaiveDate::parse_from_str(value, pattern) {
            return value.and_hms_opt(0, 0, 0);
        }
    }

    None
}

fn parse_datetime_or_timestamp(value: &str) -> Option<NaiveDateTime> {
    parse_datetime_value(value).or_else(|| parse_timestamp_value(value))
}

fn parse_timestamp_value(value: &str) -> Option<NaiveDateTime> {
    let trimmed = value.trim();
    if trimmed.is_empty() || !trimmed.chars().all(|ch| ch == '-' || ch.is_ascii_digit()) {
        return None;
    }

    let timestamp = trimmed.parse::<i64>().ok()?;
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

fn parse_numeric_text(value: &str) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let normalized = trimmed.replace([',', '，', ' '], "");
    normalized.parse::<f64>().ok()
}

fn parse_bool_text(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "y" | "t" | "on" | "是" | "开" => Some(true),
        "false" | "0" | "no" | "n" | "f" | "off" | "否" | "关" => Some(false),
        _ => None,
    }
}
