use crate::model::{DataTable, LogicalType};
use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use kuva::plot::ViolinPlot;
use kuva::plot::funnel::FunnelPlot;
use kuva::plot::lollipop::LollipopPlot;
use kuva::plot::scatter::MarkerShape;
use kuva::prelude::*;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

const PREVIEW_FILE_NAME: &str = "visualization_preview.png";
const HEATMAP_MAX_ROWS: usize = 80;
const EMPTY_OPTION: &str = "未选择";
const CJK_FONT_STACK: &str = "Microsoft YaHei, SimHei, SimSun, DejaVu Sans, Liberation Sans, Arial, sans-serif";

#[derive(Clone, Debug)]
pub enum VisualizationChartType {
    Line,
    Area,
    Scatter,
    Bar,
    Histogram,
    Pie,
    Box,
    Violin,
    Lollipop,
    Funnel,
    Heatmap,
}

impl VisualizationChartType {
    pub fn from_text(value: &str) -> Self {
        match value.trim() {
            "面积图" => Self::Area,
            "散点图" => Self::Scatter,
            "柱状图" => Self::Bar,
            "直方图" => Self::Histogram,
            "饼图" => Self::Pie,
            "箱线图" => Self::Box,
            "小提琴图" => Self::Violin,
            "棒棒糖图" => Self::Lollipop,
            "漏斗图" => Self::Funnel,
            "热力图" => Self::Heatmap,
            _ => Self::Line,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Line => "折线图",
            Self::Area => "面积图",
            Self::Scatter => "散点图",
            Self::Bar => "柱状图",
            Self::Histogram => "直方图",
            Self::Pie => "饼图",
            Self::Box => "箱线图",
            Self::Violin => "小提琴图",
            Self::Lollipop => "棒棒糖图",
            Self::Funnel => "漏斗图",
            Self::Heatmap => "热力图",
        }
    }
}

#[derive(Clone, Debug)]
pub enum VisualizationOutputFormat {
    Svg,
    Png,
    Pdf,
}

impl VisualizationOutputFormat {
    pub fn from_text(value: &str) -> Self {
        match value.trim().to_ascii_uppercase().as_str() {
            "PNG" => Self::Png,
            "PDF" => Self::Pdf,
            _ => Self::Svg,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Svg => "SVG",
            Self::Png => "PNG",
            Self::Pdf => "PDF",
        }
    }

    pub fn extension(&self) -> &'static str {
        match self {
            Self::Svg => "svg",
            Self::Png => "png",
            Self::Pdf => "pdf",
        }
    }
}

#[derive(Clone, Debug)]
pub enum VisualizationColorTheme {
    EngineeringBlue,
    SignalRed,
    ProcessGreen,
    Amber,
    Violet,
}

impl VisualizationColorTheme {
    pub fn from_text(value: &str) -> Self {
        match value.trim() {
            "信号红" => Self::SignalRed,
            "过程绿" => Self::ProcessGreen,
            "琥珀橙" => Self::Amber,
            "紫罗兰" => Self::Violet,
            _ => Self::EngineeringBlue,
        }
    }

    pub fn primary(&self) -> &'static str {
        match self {
            Self::EngineeringBlue => "#2563eb",
            Self::SignalRed => "#dc2626",
            Self::ProcessGreen => "#0f766e",
            Self::Amber => "#d97706",
            Self::Violet => "#7c3aed",
        }
    }
}

#[derive(Clone, Debug)]
pub enum VisualizationMarkerShape {
    Circle,
    Square,
    Triangle,
    Diamond,
    Cross,
    Plus,
}

impl VisualizationMarkerShape {
    pub fn from_text(value: &str) -> Self {
        match value.trim() {
            "方块" => Self::Square,
            "三角" => Self::Triangle,
            "菱形" => Self::Diamond,
            "十字" => Self::Cross,
            "加号" => Self::Plus,
            _ => Self::Circle,
        }
    }

    fn as_kuva(&self) -> MarkerShape {
        match self {
            Self::Circle => MarkerShape::Circle,
            Self::Square => MarkerShape::Square,
            Self::Triangle => MarkerShape::Triangle,
            Self::Diamond => MarkerShape::Diamond,
            Self::Cross => MarkerShape::Cross,
            Self::Plus => MarkerShape::Plus,
        }
    }
}

#[derive(Clone, Debug)]
pub struct VisualizationRequest {
    pub chart_type: VisualizationChartType,
    pub output_format: VisualizationOutputFormat,
    pub title: String,
    pub x_label: String,
    pub y_label: String,
    pub color_theme: VisualizationColorTheme,
    pub marker_shape: VisualizationMarkerShape,
    pub line_width: f64,
    pub point_size: f64,
    pub histogram_bins: usize,
    pub filled: bool,
    pub x_column: String,
    pub y_column: String,
    pub category_column: String,
    pub value_column: String,
    pub group_column: String,
    pub matrix_columns: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct VisualizationFieldSuggestion {
    pub title: String,
    pub x_label: String,
    pub y_label: String,
    pub x_column: String,
    pub y_column: String,
    pub category_column: String,
    pub value_column: String,
    pub group_column: String,
    pub matrix_columns: Vec<String>,
    pub summary: String,
}

#[derive(Clone, Debug)]
pub struct VisualizationReport {
    pub chart_name: String,
    pub summary: String,
    pub output_path: String,
    pub output_format: String,
}

enum VisualizationPayload {
    Points(Vec<(f64, f64)>),
    CategoryValues(Vec<(String, f64)>),
    Histogram(Vec<f64>),
    BoxGroups(Vec<(String, Vec<f64>)>),
    Heatmap(Vec<Vec<f64>>),
}

pub fn preview_image_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("visualization_preview")
        .join(PREVIEW_FILE_NAME)
}

pub fn suggest_fields(
    dataset_name: &str,
    table: &DataTable,
    chart_type: &VisualizationChartType,
) -> VisualizationFieldSuggestion {
    let all_columns = all_columns(table);
    let numeric_columns = numeric_columns(table);
    let time_columns = time_columns(table);
    let category_columns = category_columns(table);

    let first_numeric = numeric_columns.first().cloned().unwrap_or_default();
    let second_numeric = numeric_columns
        .get(1)
        .cloned()
        .or_else(|| numeric_columns.first().cloned())
        .unwrap_or_default();
    let best_dimension = time_columns
        .first()
        .cloned()
        .or_else(|| category_columns.first().cloned())
        .or_else(|| all_columns.first().cloned())
        .unwrap_or_default();
    let best_dimension_name = best_dimension.as_str();
    let best_group = category_columns
        .iter()
        .find(|name| name.as_str() != best_dimension_name)
        .cloned()
        .or_else(|| {
            all_columns
                .iter()
                .find(|name| name.as_str() != best_dimension_name)
                .cloned()
        })
        .unwrap_or_default();
    let matrix_columns = numeric_columns.iter().take(6).cloned().collect::<Vec<_>>();

    let title = format!("{dataset_name} - {}", chart_type.as_str());

    match chart_type {
        VisualizationChartType::Line
        | VisualizationChartType::Area
        | VisualizationChartType::Scatter
        | VisualizationChartType::Lollipop => VisualizationFieldSuggestion {
            title,
            x_label: choose_label(&best_dimension, "X 轴"),
            y_label: choose_label(&second_numeric, "Y 轴"),
            x_column: best_dimension.clone(),
            y_column: second_numeric.clone(),
            category_column: String::new(),
            value_column: first_numeric.clone(),
            group_column: String::new(),
            matrix_columns: Vec::new(),
            summary: format!(
                "建议字段：X = {}，Y = {}",
                choose_label_optional(&best_dimension),
                choose_label_optional(&second_numeric)
            ),
        },
        VisualizationChartType::Bar
        | VisualizationChartType::Pie
        | VisualizationChartType::Funnel => VisualizationFieldSuggestion {
            title,
            x_label: choose_label(&best_dimension, "分类"),
            y_label: choose_label(&first_numeric, "数值"),
            x_column: String::new(),
            y_column: String::new(),
            category_column: best_dimension.clone(),
            value_column: first_numeric.clone(),
            group_column: String::new(),
            matrix_columns: Vec::new(),
            summary: format!(
                "建议字段：分类 = {}，数值 = {}",
                choose_label_optional(&best_dimension),
                choose_label_optional(&first_numeric)
            ),
        },
        VisualizationChartType::Histogram => VisualizationFieldSuggestion {
            title,
            x_label: choose_label(&first_numeric, "取值区间"),
            y_label: "频数".to_string(),
            x_column: String::new(),
            y_column: String::new(),
            category_column: String::new(),
            value_column: first_numeric.clone(),
            group_column: String::new(),
            matrix_columns: Vec::new(),
            summary: format!("建议字段：数值 = {}", choose_label_optional(&first_numeric)),
        },
        VisualizationChartType::Box | VisualizationChartType::Violin => VisualizationFieldSuggestion {
            title,
            x_label: choose_label(&best_group, "分组"),
            y_label: choose_label(&first_numeric, "数值"),
            x_column: String::new(),
            y_column: String::new(),
            category_column: String::new(),
            value_column: first_numeric.clone(),
            group_column: best_group.clone(),
            matrix_columns: Vec::new(),
            summary: format!(
                "建议字段：分组 = {}，数值 = {}",
                choose_label_optional(&best_group),
                choose_label_optional(&first_numeric)
            ),
        },
        VisualizationChartType::Heatmap => VisualizationFieldSuggestion {
            title,
            x_label: "字段".to_string(),
            y_label: "记录".to_string(),
            x_column: String::new(),
            y_column: String::new(),
            category_column: String::new(),
            value_column: String::new(),
            group_column: String::new(),
            matrix_columns: matrix_columns.clone(),
            summary: if matrix_columns.is_empty() {
                "建议字段：当前数据集中没有可用于热力图的数值列".to_string()
            } else {
                format!("建议字段：热力矩阵 = {}", matrix_columns.join(", "))
            },
        },
    }
}

pub fn render_preview(dataset: &DataTable, request: &VisualizationRequest) -> Result<VisualizationReport> {
    let preview_path = preview_image_path();
    if let Some(parent) = preview_path.parent() {
        fs::create_dir_all(parent)?;
    }
    render(dataset, request, &preview_path, Some(1.8))
}

pub fn export_chart(
    dataset: &DataTable,
    request: &VisualizationRequest,
    output_path: &Path,
) -> Result<VisualizationReport> {
    render(dataset, request, output_path, None)
}

fn render(
    dataset: &DataTable,
    request: &VisualizationRequest,
    output_path: &Path,
    preview_scale: Option<f64>,
) -> Result<VisualizationReport> {
    let payload = build_payload(dataset, request)?;
    let plots = build_plots(&payload, request)?;
    let layout = build_layout(&plots, request);

    match preview_scale {
        Some(scale) => {
            let bytes = render_to_png(plots, layout, scale as f32)
                .map_err(|error| anyhow!("生成 PNG 预览失败: {error}"))?;
            fs::write(output_path, bytes)?;
        }
        None => match request.output_format {
            VisualizationOutputFormat::Svg => {
                let svg = render_to_svg(plots, layout);
                fs::write(output_path, svg)?;
            }
            VisualizationOutputFormat::Png => {
                let bytes = render_to_png(plots, layout, 2.0)
                    .map_err(|error| anyhow!("生成 PNG 失败: {error}"))?;
                fs::write(output_path, bytes)?;
            }
            VisualizationOutputFormat::Pdf => {
                let bytes = render_to_pdf(plots, layout)
                    .map_err(|error| anyhow!("生成 PDF 失败: {error}"))?;
                fs::write(output_path, bytes)?;
            }
        },
    }

    Ok(VisualizationReport {
        chart_name: request.chart_type.as_str().to_string(),
        summary: build_summary(&payload, request),
        output_path: output_path.display().to_string(),
        output_format: preview_scale
            .map(|_| "PNG".to_string())
            .unwrap_or_else(|| request.output_format.as_str().to_string()),
    })
}

fn build_plots(payload: &VisualizationPayload, request: &VisualizationRequest) -> Result<Vec<Plot>> {
    let primary = request.color_theme.primary();
    let plot = match (&request.chart_type, payload) {
        (VisualizationChartType::Line, VisualizationPayload::Points(points)) => Plot::Line(
            LinePlot::new()
                .with_data(points.clone())
                .with_color(primary)
                .with_stroke_width(request.line_width),
        ),
        (VisualizationChartType::Area, VisualizationPayload::Points(points)) => {
            let mut line = LinePlot::new()
                .with_data(points.clone())
                .with_color(primary)
                .with_stroke_width(request.line_width);
            if request.filled {
                line = line.with_fill().with_fill_opacity(0.30);
            }
            Plot::Line(line)
        }
        (VisualizationChartType::Scatter, VisualizationPayload::Points(points)) => Plot::Scatter(
            ScatterPlot::new()
                .with_data(points.clone())
                .with_color(primary)
                .with_size(request.point_size)
                .with_marker(request.marker_shape.as_kuva()),
        ),
        (VisualizationChartType::Bar, VisualizationPayload::CategoryValues(items)) => {
            let mut plot = BarPlot::new().with_color(primary);
            for (label, value) in items {
                plot = plot.with_bar(label.as_str(), *value);
            }
            Plot::Bar(plot)
        }
        (VisualizationChartType::Histogram, VisualizationPayload::Histogram(values)) => {
            let (min, max) = padded_range(values)?;
            Plot::Histogram(
                Histogram::new()
                    .with_data(values.clone())
                    .with_bins(request.histogram_bins.max(1))
                    .with_range((min, max))
                    .with_color(primary),
            )
        }
        (VisualizationChartType::Pie, VisualizationPayload::CategoryValues(items)) => {
            let mut plot = PiePlot::new().with_percent();
            for (index, (label, value)) in items.iter().enumerate() {
                plot = plot.with_slice(label.as_str(), *value, palette_color(&request.color_theme, index));
            }
            Plot::Pie(plot)
        }
        (VisualizationChartType::Box, VisualizationPayload::BoxGroups(groups)) => {
            let mut plot = BoxPlot::new().with_color(primary);
            for (label, values) in groups {
                plot = plot.with_group(label.as_str(), values.clone());
            }
            Plot::Box(plot)
        }
        (VisualizationChartType::Violin, VisualizationPayload::BoxGroups(groups)) => {
            let mut plot = ViolinPlot::new().with_color(primary).with_width(30.0);
            for (label, values) in groups {
                plot = plot.with_group(label.as_str(), values.clone());
            }
            Plot::Violin(plot)
        }
        (VisualizationChartType::Lollipop, VisualizationPayload::Points(points)) => {
            let mut plot = LollipopPlot::new().with_baseline(0.0);
            for (x, y) in points {
                plot = plot.with_point(*x, *y);
            }
            Plot::Lollipop(plot)
        }
        (VisualizationChartType::Funnel, VisualizationPayload::CategoryValues(items)) => {
            let mut plot = FunnelPlot::new();
            for (label, value) in items {
                plot = plot.with_stage(label.as_str(), value.max(0.0));
            }
            Plot::Funnel(plot)
        }
        (VisualizationChartType::Heatmap, VisualizationPayload::Heatmap(matrix)) => {
            Plot::Heatmap(Heatmap::new().with_data(matrix.clone()))
        }
        _ => bail!("当前图表类型与字段映射不匹配，请先重新识别字段"),
    };

    Ok(vec![plot])
}

fn build_layout(plots: &[Plot], request: &VisualizationRequest) -> Layout {
    let mut layout = Layout::auto_from_plots(plots)
        .with_font_family(CJK_FONT_STACK)
        .with_title_size(18)
        .with_label_size(14)
        .with_tick_size(12)
        .with_body_size(12);
    if !request.title.trim().is_empty() {
        layout = layout.with_title(request.title.trim());
    }
    if !request.x_label.trim().is_empty() {
        layout = layout.with_x_label(request.x_label.trim());
    }
    if !request.y_label.trim().is_empty() {
        layout = layout.with_y_label(request.y_label.trim());
    }
    layout
}

fn build_payload(dataset: &DataTable, request: &VisualizationRequest) -> Result<VisualizationPayload> {
    match request.chart_type {
        VisualizationChartType::Line
        | VisualizationChartType::Area
        | VisualizationChartType::Scatter
        | VisualizationChartType::Lollipop => {
            let x_column = required_name(&request.x_column, "X 字段")?;
            let y_column = required_name(&request.y_column, "Y 字段")?;
            Ok(VisualizationPayload::Points(xy_from_table(dataset, x_column, y_column)?))
        }
        VisualizationChartType::Bar
        | VisualizationChartType::Pie
        | VisualizationChartType::Funnel => {
            let category_column = required_name(&request.category_column, "分类字段")?;
            let value_column = required_name(&request.value_column, "数值字段")?;
            Ok(VisualizationPayload::CategoryValues(category_values_from_table(
                dataset,
                category_column,
                value_column,
            )?))
        }
        VisualizationChartType::Histogram => {
            let value_column = required_name(&request.value_column, "数值字段")?;
            Ok(VisualizationPayload::Histogram(numeric_values_from_table(
                dataset,
                value_column,
            )?))
        }
        VisualizationChartType::Box | VisualizationChartType::Violin => {
            let value_column = required_name(&request.value_column, "数值字段")?;
            Ok(VisualizationPayload::BoxGroups(box_groups_from_table(
                dataset,
                cleaned_choice(&request.group_column),
                value_column,
            )?))
        }
        VisualizationChartType::Heatmap => {
            let columns = if request.matrix_columns.is_empty() {
                numeric_columns(dataset).into_iter().take(6).collect::<Vec<_>>()
            } else {
                request.matrix_columns.clone()
            };
            if columns.is_empty() {
                bail!("热力图至少需要一个数值字段");
            }
            Ok(VisualizationPayload::Heatmap(heatmap_from_table(dataset, &columns)?))
        }
    }
}

fn xy_from_table(table: &DataTable, x_name: &str, y_name: &str) -> Result<Vec<(f64, f64)>> {
    let x_values = column_values(table, x_name)?;
    let y_values = column_values(table, y_name)?;
    let mut points = Vec::new();

    for (index, (x, y)) in x_values.iter().zip(y_values.iter()).enumerate() {
        let Some(y) = parse_numeric_cell(y.as_ref()) else {
            continue;
        };
        let x = parse_axis_cell(x.as_ref()).unwrap_or((index + 1) as f64);
        points.push((x, y));
    }

    if points.is_empty() {
        bail!("字段 {x_name} 和 {y_name} 中没有可用于绘图的数值记录");
    }
    Ok(points)
}

fn category_values_from_table(
    table: &DataTable,
    category_name: &str,
    value_name: &str,
) -> Result<Vec<(String, f64)>> {
    let category_values = column_values(table, category_name)?;
    let numeric_values = column_values(table, value_name)?;
    let mut totals = BTreeMap::<String, f64>::new();

    for (category, value) in category_values.iter().zip(numeric_values.iter()) {
        let Some(value) = parse_numeric_cell(value.as_ref()) else {
            continue;
        };
        let label = category
            .as_ref()
            .map(|item| item.trim())
            .filter(|item| !item.is_empty())
            .unwrap_or("空值")
            .to_string();
        *totals.entry(label).or_insert(0.0) += value;
    }

    let items = totals.into_iter().collect::<Vec<_>>();
    if items.is_empty() {
        bail!("字段 {category_name} 和 {value_name} 中没有可用于绘图的数据");
    }
    Ok(items)
}

fn numeric_values_from_table(table: &DataTable, column_name: &str) -> Result<Vec<f64>> {
    let values = column_values(table, column_name)?
        .iter()
        .filter_map(|value| parse_numeric_cell(value.as_ref()))
        .collect::<Vec<_>>();
    if values.is_empty() {
        bail!("字段 {column_name} 中没有可用于绘图的数值数据");
    }
    Ok(values)
}

fn box_groups_from_table(
    table: &DataTable,
    group_name: Option<&str>,
    value_name: &str,
) -> Result<Vec<(String, Vec<f64>)>> {
    let numeric_values = column_values(table, value_name)?;
    if let Some(group_name) = group_name {
        let group_values = column_values(table, group_name)?;
        let mut groups = BTreeMap::<String, Vec<f64>>::new();
        for (group, value) in group_values.iter().zip(numeric_values.iter()) {
            let Some(value) = parse_numeric_cell(value.as_ref()) else {
                continue;
            };
            let name = group
                .as_ref()
                .map(|item| item.trim())
                .filter(|item| !item.is_empty())
                .unwrap_or("空值")
                .to_string();
            groups.entry(name).or_default().push(value);
        }
        let result = groups.into_iter().collect::<Vec<_>>();
        if result.is_empty() {
            bail!("字段 {group_name} 和 {value_name} 中没有可用于绘图的数据");
        }
        Ok(result)
    } else {
        Ok(vec![("样本组".to_string(), numeric_values_from_table(table, value_name)?)])
    }
}

fn heatmap_from_table(table: &DataTable, columns: &[String]) -> Result<Vec<Vec<f64>>> {
    let mut rows = Vec::new();
    for row_index in 0..table.height().min(HEATMAP_MAX_ROWS) {
        let mut row = Vec::new();
        for column_name in columns {
            let column = column_values(table, column_name)?;
            let value = column
                .get(row_index)
                .and_then(|entry| parse_numeric_cell(entry.as_ref()))
                .unwrap_or(0.0);
            row.push(value);
        }
        rows.push(row);
    }
    if rows.is_empty() {
        bail!("热力图没有可用于绘图的数据");
    }
    Ok(rows)
}

fn all_columns(table: &DataTable) -> Vec<String> {
    table.columns.iter().map(|column| column.name.clone()).collect()
}

fn numeric_columns(table: &DataTable) -> Vec<String> {
    table
        .columns
        .iter()
        .filter(|column| matches!(column.logical_type, LogicalType::Integer | LogicalType::Float))
        .map(|column| column.name.clone())
        .collect()
}

fn time_columns(table: &DataTable) -> Vec<String> {
    table
        .columns
        .iter()
        .filter(|column| column.logical_type == LogicalType::DateTime)
        .map(|column| column.name.clone())
        .collect()
}

fn category_columns(table: &DataTable) -> Vec<String> {
    let mut columns = table
        .columns
        .iter()
        .filter(|column| !matches!(column.logical_type, LogicalType::Integer | LogicalType::Float))
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    if columns.is_empty() {
        columns = all_columns(table);
    }
    columns
}

fn column_values<'a>(table: &'a DataTable, column_name: &str) -> Result<&'a [Option<String>]> {
    table.columns
        .iter()
        .find(|column| column.name == column_name)
        .map(|column| column.values.as_slice())
        .with_context(|| format!("未找到字段 {column_name}"))
}

fn build_summary(payload: &VisualizationPayload, request: &VisualizationRequest) -> String {
    match payload {
        VisualizationPayload::Points(points) => format!(
            "{} | X = {} | Y = {} | {} 个点",
            request.chart_type.as_str(),
            fallback_label(&request.x_column, "X"),
            fallback_label(&request.y_column, "Y"),
            points.len()
        ),
        VisualizationPayload::CategoryValues(items) => format!(
            "{} | 分类 = {} | 数值 = {} | {} 个分组",
            request.chart_type.as_str(),
            fallback_label(&request.category_column, "分类"),
            fallback_label(&request.value_column, "数值"),
            items.len()
        ),
        VisualizationPayload::Histogram(values) => format!(
            "直方图 | 数值字段 = {} | {} 个样本 | {} 个分箱",
            fallback_label(&request.value_column, "数值"),
            values.len(),
            request.histogram_bins.max(1)
        ),
        VisualizationPayload::BoxGroups(groups) => format!(
            "{} | 分组 = {} | 数值 = {} | {} 个组",
            request.chart_type.as_str(),
            fallback_label(&request.group_column, "默认单组"),
            fallback_label(&request.value_column, "数值"),
            groups.len()
        ),
        VisualizationPayload::Heatmap(matrix) => format!(
            "热力图 | {} 行 x {} 列",
            matrix.len(),
            matrix.first().map(|row| row.len()).unwrap_or(0)
        ),
    }
}

fn parse_numeric_cell(value: Option<&String>) -> Option<f64> {
    value.and_then(|entry| parse_numeric(entry))
}

fn parse_numeric(value: &str) -> Option<f64> {
    value.trim().replace(',', "").parse::<f64>().ok()
}

fn parse_axis_cell(value: Option<&String>) -> Option<f64> {
    value.and_then(|entry| parse_axis_value(entry))
}

fn parse_axis_value(value: &str) -> Option<f64> {
    parse_numeric(value).or_else(|| parse_datetime_value(value))
}

fn parse_datetime_value(value: &str) -> Option<f64> {
    const DATETIME_PATTERNS: [&str; 7] = [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y%m%d",
    ];

    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Some(timestamp.timestamp() as f64);
    }

    for pattern in DATETIME_PATTERNS {
        if let Ok(value) = NaiveDateTime::parse_from_str(value, pattern) {
            return Some(value.and_utc().timestamp() as f64);
        }
        if let Ok(value) = NaiveDate::parse_from_str(value, pattern) {
            return value
                .and_hms_opt(0, 0, 0)
                .map(|value| value.and_utc().timestamp() as f64);
        }
    }

    None
}

fn required_name<'a>(value: &'a str, label: &str) -> Result<&'a str> {
    cleaned_choice(value).with_context(|| format!("请选择{label}"))
}

fn cleaned_choice(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed == EMPTY_OPTION {
        None
    } else {
        Some(trimmed)
    }
}

fn fallback_label(value: &str, fallback: &str) -> String {
    cleaned_choice(value).unwrap_or(fallback).to_string()
}

fn choose_label(value: &str, fallback: &str) -> String {
    if value.trim().is_empty() {
        fallback.to_string()
    } else {
        value.to_string()
    }
}

fn choose_label_optional(value: &str) -> String {
    if value.trim().is_empty() {
        EMPTY_OPTION.to_string()
    } else {
        value.to_string()
    }
}

fn padded_range(values: &[f64]) -> Result<(f64, f64)> {
    let min = values
        .iter()
        .copied()
        .reduce(f64::min)
        .context("直方图缺少有效数值")?;
    let max = values
        .iter()
        .copied()
        .reduce(f64::max)
        .context("直方图缺少有效数值")?;
    if (max - min).abs() < f64::EPSILON {
        Ok((min - 0.5, max + 0.5))
    } else {
        Ok((min, max))
    }
}

fn palette_color(theme: &VisualizationColorTheme, index: usize) -> &'static str {
    let engineering = [
        "#2563eb", "#60a5fa", "#93c5fd", "#1d4ed8", "#3b82f6", "#1e40af", "#0ea5e9", "#0284c7",
    ];
    let signal = [
        "#dc2626", "#f87171", "#fca5a5", "#b91c1c", "#ef4444", "#991b1b", "#fb7185", "#e11d48",
    ];
    let process = [
        "#0f766e", "#2dd4bf", "#99f6e4", "#115e59", "#14b8a6", "#134e4a", "#22c55e", "#16a34a",
    ];
    let amber = [
        "#d97706", "#f59e0b", "#fcd34d", "#b45309", "#fbbf24", "#92400e", "#fb923c", "#ea580c",
    ];
    let violet = [
        "#7c3aed", "#8b5cf6", "#c4b5fd", "#6d28d9", "#a78bfa", "#5b21b6", "#9333ea", "#c026d3",
    ];

    let palette = match theme {
        VisualizationColorTheme::EngineeringBlue => &engineering,
        VisualizationColorTheme::SignalRed => &signal,
        VisualizationColorTheme::ProcessGreen => &process,
        VisualizationColorTheme::Amber => &amber,
        VisualizationColorTheme::Violet => &violet,
    };
    palette[index % palette.len()]
}
