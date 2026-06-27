use crate::model::{
    AggregateFunction, DataTable, JoinKind, LogicalType, PipelineOperation, TableColumn, TextCaseMode,
    infer_logical_type, normalize_headers, row_signature,
};
use anyhow::{Result, bail};
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
        PipelineOperation::DropRowsWithMissing { columns } => drop_rows_with_missing(table, columns),
        PipelineOperation::RenameColumn { from, to } => rename_column(table, from, to),
        PipelineOperation::KeepColumns { columns } => keep_columns(table, columns),
        PipelineOperation::DropColumns { columns } => drop_columns(table, columns),
        PipelineOperation::SortBy { column, ascending } => sort_by_column(table, column, *ascending),
        PipelineOperation::FillNullText { column, value } => fill_null_text(table, column, value),
        PipelineOperation::FillNullForward { column } => fill_null_forward(table, column),
        PipelineOperation::CastColumn { column, target } => cast_column(table, column, target),
        PipelineOperation::TransformTextCase { column, mode } => {
            transform_text_case(table, column, mode)
        }
        PipelineOperation::ReplaceText { column, from, to } => replace_text(table, column, from, to),
        PipelineOperation::RoundNumeric { column, digits } => round_numeric(table, column, *digits),
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

fn drop_rows_with_missing(table: &DataTable, columns: &[String]) -> Result<DataTable> {
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
            target_indexes.iter().all(|column_index| {
                table.columns[*column_index]
                    .values
                    .get(*row_index)
                    .and_then(|value| value.as_ref())
                    .map(|value| !value.trim().is_empty())
                    .unwrap_or(false)
            })
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
