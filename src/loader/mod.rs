use crate::model::{
    DataTable, FileFormat, LoadedDataset, TableColumn, infer_logical_type, normalize_headers,
    null_marked,
};
use anyhow::{Context, Result, bail};
use calamine::{Data, Reader, Xlsx, open_workbook};
use chrono::Local;
use encoding_rs::GBK;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::time::Instant;

pub fn load_paths(paths: &[PathBuf]) -> Result<Vec<LoadedDataset>> {
    let mut datasets = Vec::new();

    for path in paths {
        let format = FileFormat::from_path(path)?;
        let metadata = fs::metadata(path)
            .with_context(|| format!("无法读取文件元信息: {}", path.display()))?;
        let started_at = Instant::now();

        let mut loaded = match format {
            FileFormat::Csv => vec![load_csv(path, metadata.len())?],
            FileFormat::Json => vec![load_json(path, metadata.len())?],
            FileFormat::Xlsx => load_xlsx(path, metadata.len())?,
        };
        let elapsed_ms = started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;
        for dataset in &mut loaded {
            dataset.import_duration_ms = Some(elapsed_ms);
        }
        datasets.extend(loaded);
    }

    if datasets.is_empty() {
        bail!("没有可导入的数据集");
    }

    Ok(datasets)
}

fn load_csv(path: &Path, size_bytes: u64) -> Result<LoadedDataset> {
    let bytes = fs::read(path).with_context(|| format!("无法读取 CSV 文件: {}", path.display()))?;
    let content = decode_text_bytes(&bytes);
    let delimiter = guess_delimiter(&content);

    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .flexible(true)
        .from_reader(Cursor::new(content.into_bytes()));

    let headers = reader
        .headers()
        .context("CSV 表头读取失败")?
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>();

    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record.context("CSV 记录读取失败")?;
        rows.push(
            record
                .iter()
                .map(|value| {
                    if null_marked(value) {
                        None
                    } else {
                        Some(value.trim().to_string())
                    }
                })
                .collect::<Vec<_>>(),
        );
    }

    let table = build_table(headers, rows);

    Ok(LoadedDataset {
        dataset_name: path_stem_or_default(path, "csv_dataset"),
        source_path: path.to_path_buf(),
        format: FileFormat::Csv,
        size_bytes,
        imported_at: Local::now(),
        import_duration_ms: None,
        sheet_name: None,
        table,
    })
}

fn load_json(path: &Path, size_bytes: u64) -> Result<LoadedDataset> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("无法读取 JSON 文件: {}", path.display()))?;
    let json: Value = serde_json::from_str(&content).context("JSON 解析失败")?;

    let records = match json {
        Value::Array(items) => items,
        Value::Object(_) => vec![json],
        _ => bail!("JSON 根节点必须是对象或对象数组"),
    };

    let mut all_keys = BTreeSet::new();
    let mut flattened_rows = Vec::new();

    for item in records {
        let mut flattened = BTreeMap::new();
        flatten_json("", &item, &mut flattened);
        all_keys.extend(flattened.keys().cloned());
        flattened_rows.push(flattened);
    }

    let headers = all_keys.into_iter().collect::<Vec<_>>();
    let rows = flattened_rows
        .into_iter()
        .map(|row| {
            headers
                .iter()
                .map(|header| row.get(header).cloned().unwrap_or(None))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    Ok(LoadedDataset {
        dataset_name: path_stem_or_default(path, "json_dataset"),
        source_path: path.to_path_buf(),
        format: FileFormat::Json,
        size_bytes,
        imported_at: Local::now(),
        import_duration_ms: None,
        sheet_name: None,
        table: build_table(headers, rows),
    })
}

fn load_xlsx(path: &Path, size_bytes: u64) -> Result<Vec<LoadedDataset>> {
    let mut workbook: Xlsx<_> =
        open_workbook(path).with_context(|| format!("无法打开 Excel: {}", path.display()))?;

    let mut datasets = Vec::new();
    for sheet_name in workbook.sheet_names().to_owned() {
        let range = workbook
            .worksheet_range(&sheet_name)
            .with_context(|| format!("sheet 读取失败: {sheet_name}"))?;

        let mut rows = range.rows();
        let Some(header_row) = rows.next() else {
            continue;
        };

        let headers = header_row
            .iter()
            .map(cell_to_string)
            .collect::<Vec<_>>();

        let body = rows
            .map(|row| {
                row.iter()
                    .map(|cell| {
                        let value = cell_to_string(cell);
                        if null_marked(&value) {
                            None
                        } else {
                            Some(value)
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        let table = build_table(headers, body);
        if table.width() == 0 {
            continue;
        }

        datasets.push(LoadedDataset {
            dataset_name: xlsx_dataset_name(path, &sheet_name),
            source_path: path.to_path_buf(),
            format: FileFormat::Xlsx,
            size_bytes,
            imported_at: Local::now(),
            import_duration_ms: None,
            sheet_name: Some(sheet_name),
            table,
        });
    }

    if datasets.is_empty() {
        bail!("Excel 中没有可用 sheet");
    }

    Ok(datasets)
}

fn path_stem_or_default(path: &Path, fallback: &str) -> String {
    path.file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or(fallback)
        .to_string()
}

fn xlsx_dataset_name(path: &Path, sheet_name: &str) -> String {
    let stem = path_stem_or_default(path, "xlsx_dataset");
    if stem.eq_ignore_ascii_case(sheet_name) {
        stem
    } else {
        format!("{stem}[{sheet_name}]")
    }
}

fn build_table(headers: Vec<String>, rows: Vec<Vec<Option<String>>>) -> DataTable {
    let normalized_headers = normalize_headers(&headers);
    let width = normalized_headers.len().max(rows.iter().map(Vec::len).max().unwrap_or(0));

    let mut column_buffers = vec![Vec::<Option<String>>::new(); width];
    for row in rows {
        for column_index in 0..width {
            column_buffers[column_index].push(row.get(column_index).cloned().unwrap_or(None));
        }
    }

    let columns = (0..width)
        .map(|index| {
            let name = normalized_headers
                .get(index)
                .cloned()
                .unwrap_or_else(|| format!("column_{}", index + 1));
            let values = column_buffers.get(index).cloned().unwrap_or_default();
            let logical_type = infer_logical_type(&values);

            TableColumn {
                name,
                logical_type,
                values,
            }
        })
        .collect::<Vec<_>>();

    DataTable { columns }
}

fn decode_text_bytes(bytes: &[u8]) -> String {
    match String::from_utf8(bytes.to_vec()) {
        Ok(text) => text,
        Err(_) => {
            let (decoded, _, _) = GBK.decode(bytes);
            decoded.into_owned()
        }
    }
}

fn guess_delimiter(content: &str) -> u8 {
    let candidates = [b',', b';', b'\t', b'|'];
    let lines = content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .take(12)
        .collect::<Vec<_>>();

    candidates
        .iter()
        .copied()
        .max_by_key(|delimiter| {
            lines
                .iter()
                .map(|line| line.matches(char::from(*delimiter)).count())
                .sum::<usize>()
        })
        .unwrap_or(b',')
}

fn flatten_json(prefix: &str, value: &Value, out: &mut BTreeMap<String, Option<String>>) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                let next = if prefix.is_empty() {
                    key.to_string()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten_json(&next, value, out);
            }
        }
        Value::Array(values) => {
            if values.iter().all(|item| !item.is_object() && !item.is_array()) {
                let joined = values
                    .iter()
                    .filter_map(simple_json_value)
                    .collect::<Vec<_>>()
                    .join("; ");
                out.insert(prefix.to_string(), Some(joined));
            } else {
                out.insert(prefix.to_string(), Some(value.to_string()));
            }
        }
        _ => {
            out.insert(prefix.to_string(), simple_json_value(value));
        }
    }
}

fn simple_json_value(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Bool(flag) => Some(flag.to_string()),
        Value::Number(number) => Some(number.to_string()),
        Value::String(text) => {
            if null_marked(text) {
                None
            } else {
                Some(text.trim().to_string())
            }
        }
        _ => Some(value.to_string()),
    }
}

fn cell_to_string(cell: &Data) -> String {
    match cell {
        Data::Empty => String::new(),
        Data::String(value) => value.trim().to_string(),
        Data::Float(value) => value.to_string(),
        Data::Int(value) => value.to_string(),
        Data::Bool(value) => value.to_string(),
        Data::DateTime(value) => value.to_string(),
        Data::DateTimeIso(value) => value.clone(),
        Data::DurationIso(value) => value.clone(),
        Data::Error(_) => "ERROR".to_string(),
    }
}
