use crate::model::{ColumnProfile, DatasetRecord, PipelineTemplate, QualityIssue, QualityOverview, QualityRules};
use anyhow::{Context, Result, bail};
use serde::Serialize;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

pub fn export_dataset_csv(record: &DatasetRecord, path: &Path) -> Result<()> {
    let file = fs::File::create(path)
        .with_context(|| format!("无法创建 CSV: {}", path.display()))?;
    let mut writer = BufWriter::new(file);

    writer.write_all(b"\xEF\xBB\xBF")?;

    let mut csv_writer = csv::WriterBuilder::new().from_writer(writer);
    csv_writer.write_record(record.working_table.column_names())?;

    for row_index in 0..record.working_table.height() {
        let row = record
            .working_table
            .row(row_index)
            .into_iter()
            .map(|cell| cell.unwrap_or_default())
            .collect::<Vec<_>>();
        csv_writer.write_record(row)?;
    }

    csv_writer.flush()?;
    Ok(())
}

pub fn export_dataset_json(record: &DatasetRecord, path: &Path) -> Result<()> {
    let headers = record.working_table.column_names();
    let rows = (0..record.working_table.height())
        .map(|row_index| {
            headers
                .iter()
                .cloned()
                .zip(record.working_table.row(row_index))
                .map(|(key, value)| {
                    (
                        key,
                        value
                            .map(serde_json::Value::String)
                            .unwrap_or(serde_json::Value::Null),
                    )
                })
                .collect::<serde_json::Map<String, serde_json::Value>>()
        })
        .map(serde_json::Value::Object)
        .collect::<Vec<_>>();

    let content = serde_json::to_string_pretty(&rows)?;
    fs::write(path, content).with_context(|| format!("无法写入 JSON: {}", path.display()))?;
    Ok(())
}

pub fn export_quality_report(record: &DatasetRecord, path: &Path) -> Result<()> {
    #[derive(Serialize)]
    struct QualityReport<'a> {
        dataset_name: &'a str,
        source_path: String,
        imported_at: String,
        row_count: usize,
        column_count: usize,
        key_candidates: &'a [String],
        time_candidates: &'a [String],
        resolved_primary_key: &'a str,
        resolved_composite_keys: &'a [String],
        resolved_time_column: &'a str,
        rules: &'a QualityRules,
        overview: &'a QualityOverview,
        issues: &'a [QualityIssue],
        columns: &'a [ColumnProfile],
    }

    let report = QualityReport {
        dataset_name: &record.dataset_name,
        source_path: record.source_path.display().to_string(),
        imported_at: record.imported_at.format("%Y-%m-%d %H:%M:%S").to_string(),
        row_count: record.profile.row_count,
        column_count: record.profile.column_count,
        key_candidates: &record.profile.key_candidates,
        time_candidates: &record.profile.time_candidates,
        resolved_primary_key: &record.profile.resolved_primary_key,
        resolved_composite_keys: &record.profile.resolved_composite_keys,
        resolved_time_column: &record.profile.resolved_time_column,
        rules: &record.quality_rules,
        overview: &record.profile.quality_overview,
        issues: &record.profile.quality_issues,
        columns: &record.profile.columns,
    };

    let temp_path = std::env::temp_dir().join(format!(
        "data_processing_quality_{}_{}.json",
        record.id,
        std::process::id()
    ));
    write_pretty_json(&temp_path, &report)?;

    let script_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("export_quality_pdf.py");
    if !script_path.exists() {
        bail!("未找到 PDF 导出脚本: {}", script_path.display());
    }

    let (python, prefix_args) = find_python_runner()?;
    let output = Command::new(&python)
        .args(prefix_args)
        .arg(&script_path)
        .arg(&temp_path)
        .arg(path)
        .output()
        .with_context(|| format!("无法调用 PDF 导出程序: {}", python.display()))?;

    let _ = fs::remove_file(&temp_path);

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        bail!(
            "PDF 导出失败: {}{}{}",
            stdout.trim(),
            if !stdout.trim().is_empty() && !stderr.trim().is_empty() {
                " | "
            } else {
                ""
            },
            stderr.trim()
        );
    }

    Ok(())
}

pub fn export_pipeline_template(template: &PipelineTemplate, path: &Path) -> Result<()> {
    write_pretty_json(path, template)
}

pub fn import_pipeline_template(path: &Path) -> Result<PipelineTemplate> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("无法读取流程模板: {}", path.display()))?;
    serde_json::from_str(&content).context("流程模板 JSON 解析失败")
}

fn write_pretty_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let content = serde_json::to_string_pretty(value)?;
    fs::write(path, content).with_context(|| format!("无法写入文件: {}", path.display()))?;
    Ok(())
}

fn find_python_runner() -> Result<(PathBuf, Vec<&'static str>)> {
    let candidates = vec![
        (
            PathBuf::from(
                r"C:\Users\49482\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe",
            ),
            Vec::new(),
        ),
        (PathBuf::from("python"), Vec::new()),
        (PathBuf::from("py"), vec!["-3"]),
    ];

    for (candidate, args) in candidates {
        let status = Command::new(&candidate)
            .args(&args)
            .arg("-c")
            .arg("import reportlab")
            .output();

        if let Ok(output) = status && output.status.success() {
            return Ok((candidate, args));
        }
    }

    bail!("未找到可用的 Python/reportlab 运行环境")
}
