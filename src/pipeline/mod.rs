use crate::model::{PipelineOperation, PipelineStep, PipelineTemplate};
use chrono::Local;

pub fn build_step(operation: &PipelineOperation, detail: String, outcome: String) -> PipelineStep {
    PipelineStep {
        timestamp: Local::now(),
        action: operation.to_string(),
        detail,
        outcome,
        operation: Some(operation.clone()),
    }
}

pub fn template_from_operations(
    dataset_name: String,
    operations: Vec<PipelineOperation>,
) -> PipelineTemplate {
    PipelineTemplate {
        dataset_name,
        created_at: Local::now(),
        operations,
    }
}
