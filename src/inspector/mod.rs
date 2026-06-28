use crate::model::{
    ColumnProfile, DataTable, DatasetProfile, JoinSuggestion, LogicalType, MappingSuggestion,
    QualityIssue, QualityOverview, QualityRules, TableColumn, looks_like_datetime, parse_bool,
    row_signature,
};
use chrono::{NaiveDate, NaiveDateTime};
use std::collections::{BTreeSet, HashMap};

const MAPPING_DICTIONARY: &[(&str, &[&str])] = &[
    ("timestamp", &["time", "时间", "日期", "datetime", "ts"]),
    ("device_id", &["device", "dev_id", "设备编号", "设备id", "id"]),
    ("temperature", &["temp", "温度", "temp_c", "temperature_c"]),
    ("pressure", &["press", "压力"]),
    ("status", &["state", "状态", "result_status"]),
    ("batch_id", &["batch", "批次", "batch_no"]),
];

pub fn build_profile(table: &DataTable, rules: &QualityRules) -> DatasetProfile {
    let row_count = table.height();
    let column_count = table.width();
    let mut columns = Vec::new();
    let mut issues = Vec::new();
    let mut overview = QualityOverview::default();
    let mut key_candidates = Vec::new();
    let mut time_candidates = Vec::new();
    let mut numeric_columns = Vec::new();
    let high_missing_threshold = rules.normalized_threshold();

    let resolved_primary_key = resolve_primary_key(table, rules);
    let resolved_composite_keys = resolve_composite_keys(table, rules);
    let resolved_time_column = resolve_time_column(table, rules);

    for column in &table.columns {
        let present_values = normalized_values(column);
        let non_null_count = present_values.len();
        let missing_count = row_count.saturating_sub(non_null_count);
        let unique_count = present_values.iter().cloned().collect::<BTreeSet<_>>().len();
        let sample_value = present_values
            .iter()
            .find(|value| !value.trim().is_empty())
            .cloned()
            .unwrap_or_else(|| "无示例".to_string());
        let missing_rate = if row_count == 0 {
            0.0
        } else {
            missing_count as f32 / row_count as f32
        };

        let is_key_candidate = row_count > 0 && unique_count == row_count && missing_count == 0;
        let is_time_candidate = column.logical_type == LogicalType::DateTime
            || column.name.contains("time")
            || column.name.contains("date")
            || column
                .values
                .iter()
                .flatten()
                .filter(|value| looks_like_datetime(value))
                .count()
                * 100
                / non_null_count.max(1)
                >= 70;
        let is_numeric = matches!(column.logical_type, LogicalType::Integer | LogicalType::Float);

        let role_hint = if column.name == resolved_primary_key {
            "主键"
        } else if resolved_composite_keys.contains(&column.name) {
            "组合键"
        } else if column.name == resolved_time_column {
            "时间序列"
        } else if is_key_candidate {
            "主键候选"
        } else if is_time_candidate {
            "时间字段"
        } else if is_numeric {
            "数值字段"
        } else {
            "普通字段"
        };

        if is_key_candidate {
            key_candidates.push(column.name.clone());
        }
        if is_time_candidate {
            time_candidates.push(column.name.clone());
        }
        if is_numeric {
            numeric_columns.push(column.name.clone());
        }

        if missing_rate >= high_missing_threshold {
            overview.high_missing_field_count += 1;
            issues.push(QualityIssue {
                category: "缺失字段".to_string(),
                severity: if missing_rate >= 0.6 { "高" } else { "中" }.to_string(),
                field: column.name.clone(),
                detail: format!(
                    "缺失 {} 行，占比 {:.1}%，已达到高缺失预警阈值 {:.0}%",
                    missing_count,
                    missing_rate * 100.0,
                    high_missing_threshold * 100.0
                ),
            });
        }

        if let Some(issue) = detect_numeric_invalid(column) {
            overview.numeric_invalid_column_count += 1;
            issues.push(issue);
        }

        if let Some(issue) = detect_type_mixing(column) {
            overview.mixed_type_column_count += 1;
            issues.push(issue);
        }

        columns.push(ColumnProfile {
            name: column.name.clone(),
            logical_type: column.logical_type.as_str().to_string(),
            non_null_count,
            missing_count,
            missing_rate,
            unique_count,
            sample_value,
            role_hint: role_hint.to_string(),
        });
    }

    let fully_empty_rows = detect_fully_empty_rows(table);
    if fully_empty_rows > 0 {
        overview.fully_empty_row_count = fully_empty_rows;
        issues.push(QualityIssue {
            category: "空记录".to_string(),
            severity: "中".to_string(),
            field: "整表".to_string(),
            detail: format!("检测到 {} 行整行缺失记录", fully_empty_rows),
        });
    }

    let duplicate_rows = detect_duplicate_rows(table);
    if duplicate_rows > 0 {
        overview.duplicate_row_count = duplicate_rows;
        issues.push(QualityIssue {
            category: "重复记录".to_string(),
            severity: "中".to_string(),
            field: "整表".to_string(),
            detail: format!("检测到 {} 行完全重复记录", duplicate_rows),
        });
    }

    if !resolved_primary_key.is_empty() {
        let primary_key_empty_count = count_missing_in_column(table, &resolved_primary_key);
        if primary_key_empty_count > 0 {
            overview.primary_key_empty_count = primary_key_empty_count;
            issues.push(QualityIssue {
                category: "主键规则".to_string(),
                severity: "高".to_string(),
                field: resolved_primary_key.clone(),
                detail: format!("主键列存在 {} 个空值", primary_key_empty_count),
            });
        }

        let duplicate_count = detect_key_duplicates(table, &[resolved_primary_key.clone()]);
        if duplicate_count > 0 {
            overview.primary_key_duplicate_count = duplicate_count;
            issues.push(QualityIssue {
                category: "主键重复".to_string(),
                severity: "高".to_string(),
                field: resolved_primary_key.clone(),
                detail: format!("按指定主键检测到 {} 行重复记录", duplicate_count),
            });
        }
    }

    if resolved_composite_keys.len() >= 2 {
        let duplicate_count = detect_key_duplicates(table, &resolved_composite_keys);
        if duplicate_count > 0 {
            overview.composite_duplicate_count = duplicate_count;
            issues.push(QualityIssue {
                category: "组合重复".to_string(),
                severity: "中".to_string(),
                field: resolved_composite_keys.join(", "),
                detail: format!("按组合字段检测到 {} 行重复记录", duplicate_count),
            });
        }
    }

    if !resolved_time_column.is_empty() {
        let order_issue_count = detect_time_order_issues(table, &resolved_time_column);
        if order_issue_count > 0 {
            overview.time_order_issue_count = order_issue_count;
            issues.push(QualityIssue {
                category: "时间规则".to_string(),
                severity: "中".to_string(),
                field: resolved_time_column.clone(),
                detail: format!("时间序列中检测到 {} 处逆序或异常跳变", order_issue_count),
            });
        }
    }

    DatasetProfile {
        row_count,
        column_count,
        key_candidates,
        time_candidates,
        numeric_columns,
        resolved_primary_key,
        resolved_composite_keys,
        resolved_time_column,
        preview_header: table.preview_header(table.width()),
        preview_rows: table.preview_rows(18, table.width()),
        columns,
        quality_overview: overview,
        quality_issues: issues,
        mapping_suggestions: suggest_mappings(table),
    }
}

pub fn build_join_suggestions(
    current_name: &str,
    current_table: &DataTable,
    others: &[(&str, &DataTable)],
) -> Vec<JoinSuggestion> {
    let current_names = current_table.column_names();
    let current_normalized = canonicalized_names(&current_names);
    let current_keys = current_table
        .columns
        .iter()
        .filter(|column| column.name.ends_with("id") || column.name.contains("code"))
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();

    let mut suggestions = Vec::new();
    for (dataset_name, table) in others {
        if *dataset_name == current_name {
            continue;
        }

        let right_names = table.column_names();
        for left_name in &current_names {
            for right_name in &right_names {
                let left_canonical = current_normalized.get(left_name).cloned().unwrap_or_default();
                let right_canonical = canonical_name(right_name);

                if left_name == right_name
                    || (!left_canonical.is_empty() && left_canonical == right_canonical)
                {
                    suggestions.push(JoinSuggestion {
                        target_dataset: (*dataset_name).to_string(),
                        left_key: left_name.clone(),
                        right_key: right_name.clone(),
                        join_type: if current_keys.contains(left_name) {
                            "左连接"
                        } else {
                            "内连接"
                        }
                        .to_string(),
                        reason: if left_name == right_name {
                            "字段同名，可直接按主键尝试融合".to_string()
                        } else {
                            format!("字段语义接近：{} ≈ {}", left_name, right_name)
                        },
                    });
                }
            }
        }
    }

    suggestions.truncate(8);
    suggestions
}

pub fn suggest_mappings(table: &DataTable) -> Vec<MappingSuggestion> {
    table.columns
        .iter()
        .filter_map(|column| {
            let target = canonical_name(&column.name);
            if target.is_empty() || target == column.name {
                None
            } else {
                Some(MappingSuggestion {
                    source_name: column.name.clone(),
                    target_name: target,
                    confidence: "推荐".to_string(),
                    status: "待应用".to_string(),
                })
            }
        })
        .collect()
}

fn resolve_primary_key(table: &DataTable, rules: &QualityRules) -> String {
    let configured = rules.primary_key.trim();
    if !configured.is_empty() && table.columns.iter().any(|column| column.name == configured) {
        return configured.to_string();
    }

    table.columns
        .iter()
        .find(|column| {
            let values = normalized_values(column);
            !values.is_empty()
                && values.len() == table.height()
                && values.iter().cloned().collect::<BTreeSet<_>>().len() == table.height()
        })
        .map(|column| column.name.clone())
        .unwrap_or_default()
}

fn resolve_composite_keys(table: &DataTable, rules: &QualityRules) -> Vec<String> {
    let configured = rules
        .composite_keys
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .filter(|value| table.columns.iter().any(|column| column.name == *value))
        .map(str::to_string)
        .collect::<Vec<_>>();

    if configured.len() >= 2 {
        configured
    } else {
        Vec::new()
    }
}

fn resolve_time_column(table: &DataTable, rules: &QualityRules) -> String {
    let configured = rules.time_column.trim();
    if !configured.is_empty() && table.columns.iter().any(|column| column.name == configured) {
        return configured.to_string();
    }

    table.columns
        .iter()
        .find(|column| {
            column.logical_type == LogicalType::DateTime
                || column
                    .values
                    .iter()
                    .flatten()
                    .filter(|value| looks_like_datetime(value))
                    .count()
                    * 100
                    / normalized_values(column).len().max(1)
                    >= 70
        })
        .map(|column| column.name.clone())
        .unwrap_or_default()
}

fn canonicalized_names(names: &[String]) -> HashMap<String, String> {
    names.iter()
        .map(|name| (name.clone(), canonical_name(name)))
        .collect()
}

fn canonical_name(name: &str) -> String {
    let normalized = name.trim().to_ascii_lowercase().replace([' ', '-'], "_");
    for (target, aliases) in MAPPING_DICTIONARY {
        if normalized == *target || aliases.iter().any(|alias| normalized == *alias) {
            return (*target).to_string();
        }
    }
    String::new()
}

fn normalized_values(column: &TableColumn) -> Vec<String> {
    column
        .values
        .iter()
        .filter_map(|value| normalize_cell(value.as_ref()))
        .collect()
}

fn normalize_cell(value: Option<&String>) -> Option<String> {
    value
        .map(|entry| entry.trim())
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
}

fn detect_fully_empty_rows(table: &DataTable) -> usize {
    (0..table.height())
        .filter(|row_index| {
            table.row(*row_index)
                .iter()
                .all(|value| normalize_cell(value.as_ref()).is_none())
        })
        .count()
}

fn detect_duplicate_rows(table: &DataTable) -> usize {
    let mut signatures = HashMap::<String, usize>::new();
    let mut duplicates = 0usize;

    for row_index in 0..table.height() {
        let normalized_row = table
            .row(row_index)
            .into_iter()
            .map(|value| normalize_cell(value.as_ref()).unwrap_or_default())
            .map(Some)
            .collect::<Vec<_>>();
        let signature = row_signature(&normalized_row);
        let count = signatures.entry(signature).or_insert(0);
        *count += 1;
        if *count > 1 {
            duplicates += 1;
        }
    }

    duplicates
}

fn detect_key_duplicates(table: &DataTable, columns: &[String]) -> usize {
    let target_indexes = columns
        .iter()
        .filter_map(|name| table.columns.iter().position(|column| column.name == *name))
        .collect::<Vec<_>>();
    if target_indexes.len() != columns.len() {
        return 0;
    }

    let mut signatures = HashMap::<String, usize>::new();
    let mut duplicates = 0usize;

    for row_index in 0..table.height() {
        let row = table.row(row_index);
        let values = target_indexes
            .iter()
            .map(|index| row.get(*index).cloned().unwrap_or(None))
            .collect::<Vec<_>>();

        if values
            .iter()
            .any(|value| normalize_cell(value.as_ref()).is_none())
        {
            continue;
        }

        let signature = values
            .iter()
            .map(|value| normalize_cell(value.as_ref()).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("|");

        let count = signatures.entry(signature).or_insert(0);
        *count += 1;
        if *count > 1 {
            duplicates += 1;
        }
    }

    duplicates
}

fn count_missing_in_column(table: &DataTable, column_name: &str) -> usize {
    table.columns
        .iter()
        .find(|column| column.name == column_name)
        .map(|column| {
            column
                .values
                .iter()
                .filter(|value| normalize_cell(value.as_ref()).is_none())
                .count()
        })
        .unwrap_or(0)
}

fn detect_numeric_invalid(column: &TableColumn) -> Option<QualityIssue> {
    if !matches!(column.logical_type, LogicalType::Integer | LogicalType::Float) {
        return None;
    }

    let invalid_count = column
        .values
        .iter()
        .filter_map(|value| normalize_cell(value.as_ref()))
        .filter(|value| match column.logical_type {
            LogicalType::Integer => value.parse::<i64>().is_err(),
            LogicalType::Float => value.parse::<f64>().is_err(),
            _ => false,
        })
        .count();

    (invalid_count > 0).then(|| QualityIssue {
        category: "类型异常".to_string(),
        severity: "高".to_string(),
        field: column.name.clone(),
        detail: format!("数值列中检测到 {} 个非法字符或不可解析值", invalid_count),
    })
}

fn detect_type_mixing(column: &TableColumn) -> Option<QualityIssue> {
    let mut kinds = BTreeSet::new();
    let mut value_count = 0usize;

    for value in column.values.iter().filter_map(|value| normalize_cell(value.as_ref())) {
        value_count += 1;
        kinds.insert(classify_value_kind(&value));
    }

    if value_count == 0 || kinds.len() <= 1 {
        return None;
    }

    Some(QualityIssue {
        category: "类型混杂".to_string(),
        severity: if matches!(column.logical_type, LogicalType::Integer | LogicalType::Float) {
            "高"
        } else {
            "中"
        }
        .to_string(),
        field: column.name.clone(),
        detail: format!(
            "同列出现多种语义类型：{}",
            kinds.into_iter().collect::<Vec<_>>().join(" / ")
        ),
    })
}

fn detect_time_order_issues(table: &DataTable, column_name: &str) -> usize {
    let Some(column) = table.columns.iter().find(|column| column.name == column_name) else {
        return 0;
    };

    let mut previous: Option<NaiveDateTime> = None;
    let mut issues = 0usize;
    for value in column.values.iter().filter_map(|value| normalize_cell(value.as_ref())) {
        let Some(current) = parse_datetime_value(&value) else {
            continue;
        };

        if let Some(previous_value) = previous {
            if current < previous_value {
                issues += 1;
            }
        }
        previous = Some(current);
    }

    issues
}

fn classify_value_kind(value: &str) -> String {
    if value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok() {
        "数值".to_string()
    } else if parse_bool(value).is_some() {
        "布尔".to_string()
    } else if looks_like_datetime(value) {
        "时间".to_string()
    } else {
        "文本".to_string()
    }
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
