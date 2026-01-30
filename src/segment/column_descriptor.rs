use serde::Deserialize;

/// Mirrors Druid's ValueType enum.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum ValueType {
    String,
    Long,
    Float,
    Double,
    Complex,
}

/// Mirrors Druid's ColumnDescriptor, serialized as JSON at the start
/// of each column's data within the smoosh archive.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnDescriptor {
    pub value_type: ValueType,
    #[serde(default)]
    pub has_multiple_values: bool,
    pub parts: Vec<ColumnPartSerde>,
}

/// One entry in the ColumnDescriptor's `parts` array.
/// The `type` field identifies the serialization class.
/// Additional fields vary by type and are parsed separately from binary data.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnPartSerde {
    #[serde(rename = "type")]
    pub serde_type: String,
    /// Capture remaining fields as a generic JSON value for forward compatibility.
    #[serde(flatten)]
    pub extra: serde_json::Value,
}
