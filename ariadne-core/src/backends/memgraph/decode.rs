//! Loss-aware conversion from Memgraph Bolt values into JSON query results.
//!
//! Temporal, spatial, graph, and numeric values must either preserve their
//! information in JSON or return an error; conversions must never panic.

use super::MemgraphError;
use crate::prelude::*;
use rsmgclient::Record;
use serde::Serialize;
use serde_json::{Number, Value};
use std::collections::HashMap;

pub(super) fn record_to_json(columns: &[String], record: &Record) -> Result<Value> {
    if columns.len() != record.values.len() {
        return Err(MemgraphError::ValueConversionError(format!(
            "record contains {} values for {} columns",
            record.values.len(),
            columns.len()
        ))
        .into());
    }
    let mut map = serde_json::Map::new();
    for (column, value) in columns.iter().zip(&record.values) {
        map.insert(column.clone(), value_to_json(value)?);
    }
    Ok(Value::Object(map))
}

pub(super) fn value_to_json(value: &rsmgclient::Value) -> Result<Value> {
    let value = match value {
        rsmgclient::Value::Null => Value::Null,
        rsmgclient::Value::Bool(value) => Value::Bool(*value),
        rsmgclient::Value::Int(value) => Value::Number(Number::from(*value)),
        rsmgclient::Value::Float(value) => Value::Number(json_number(*value, "float")?),
        rsmgclient::Value::String(value) => Value::String(value.clone()),
        rsmgclient::Value::List(values) => Value::Array(
            values
                .iter()
                .map(value_to_json)
                .collect::<Result<Vec<_>>>()?,
        ),
        rsmgclient::Value::Date(date) => Value::String(date.to_string()),
        rsmgclient::Value::LocalTime(time) => Value::String(format_local_time(time)),
        rsmgclient::Value::LocalDateTime(date_time) => {
            Value::String(format_local_date_time(date_time))
        }
        rsmgclient::Value::DateTime(date_time) => date_time_to_json(date_time),
        rsmgclient::Value::Duration(duration) => Value::String(duration.to_string()),
        rsmgclient::Value::Map(values) => {
            let mut map = serde_json::Map::new();
            for (key, value) in values {
                map.insert(key.clone(), value_to_json(value)?);
            }
            Value::Object(map)
        }
        rsmgclient::Value::Node(node) => serde_json::to_value(Node::try_new(node)?)?,
        rsmgclient::Value::Relationship(relationship) => {
            serde_json::to_value(Relationship::try_new(relationship)?)?
        }
        rsmgclient::Value::UnboundRelationship(relationship) => {
            serde_json::to_value(UnboundRelationship::try_new(relationship)?)?
        }
        rsmgclient::Value::Path(path) => serde_json::to_value(Path::try_new(path)?)?,
        rsmgclient::Value::Point2D(point) => point_2d_to_json(point)?,
        rsmgclient::Value::Point3D(point) => point_3d_to_json(point)?,
    };
    Ok(value)
}

fn json_number(value: f64, field: &str) -> Result<Number> {
    Number::from_f64(value).ok_or_else(|| {
        MemgraphError::ValueConversionError(format!("{field} is not a finite JSON number")).into()
    })
}

fn push_fraction(value: &mut String, nanosecond: u32) {
    if nanosecond != 0 {
        let fraction = format!("{nanosecond:09}");
        value.push('.');
        value.push_str(fraction.trim_end_matches('0'));
    }
}

fn format_year(year: i32) -> String {
    if year < 0 {
        format!("-{:04}", year.unsigned_abs())
    } else if year <= 9999 {
        format!("{year:04}")
    } else {
        format!("+{year}")
    }
}

fn format_local_time(time: &rsmgclient::LocalTime) -> String {
    let mut value = format!(
        "{:02}:{:02}:{:02}",
        time.hour(),
        time.minute(),
        time.second()
    );
    push_fraction(&mut value, time.nanosecond() as u32);
    value
}

fn format_local_date_time(date_time: &rsmgclient::LocalDateTime) -> String {
    let mut value = format!(
        "{}-{:02}-{:02}T{:02}:{:02}:{:02}",
        format_year(i32::from(date_time.year())),
        date_time.month(),
        date_time.day(),
        date_time.hour(),
        date_time.minute(),
        date_time.second()
    );
    push_fraction(&mut value, date_time.nanosecond() as u32);
    value
}

fn format_offset(offset_seconds: i32) -> String {
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let offset = offset_seconds.unsigned_abs();
    let hours = offset / 3_600;
    let minutes = (offset % 3_600) / 60;
    let seconds = offset % 60;
    if seconds == 0 {
        format!("{sign}{hours:02}:{minutes:02}")
    } else {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
    }
}

fn date_time_to_json(date_time: &rsmgclient::DateTime) -> Value {
    let mut value = format!(
        "{}-{:02}-{:02}T{:02}:{:02}:{:02}",
        format_year(date_time.year),
        date_time.month,
        date_time.day,
        date_time.hour,
        date_time.minute,
        date_time.second
    );
    push_fraction(&mut value, date_time.nanosecond);
    value.push_str(&format_offset(date_time.time_zone_offset_seconds));

    let mut result = serde_json::Map::new();
    result.insert("type".to_string(), Value::String("datetime".to_string()));
    result.insert("value".to_string(), Value::String(value));
    if let Some(time_zone_id) = &date_time.time_zone_id {
        result.insert(
            "timezone_id".to_string(),
            Value::String(time_zone_id.clone()),
        );
    }
    Value::Object(result)
}

fn point_2d_to_json(point: &rsmgclient::Point2D) -> Result<Value> {
    Ok(Value::Object(point_json_fields(
        point.srid,
        point.x_longitude,
        point.y_latitude,
    )?))
}

fn point_3d_to_json(point: &rsmgclient::Point3D) -> Result<Value> {
    let mut result = point_json_fields(point.srid, point.x_longitude, point.y_latitude)?;
    result.insert(
        "z".to_string(),
        Value::Number(json_number(point.z_height, "point.z")?),
    );
    Ok(Value::Object(result))
}

fn point_json_fields(srid: u16, x: f64, y: f64) -> Result<serde_json::Map<String, Value>> {
    let mut result = serde_json::Map::new();
    result.insert("type".to_string(), Value::String("point".to_string()));
    result.insert("srid".to_string(), Value::Number(srid.into()));
    result.insert("x".to_string(), Value::Number(json_number(x, "point.x")?));
    result.insert("y".to_string(), Value::Number(json_number(y, "point.y")?));
    Ok(result)
}

#[derive(Debug, PartialEq, Clone, Serialize)]
struct Node {
    id: i64,
    label_count: u32,
    labels: Vec<String>,
    properties: HashMap<String, Value>,
    #[serde(rename = "type")]
    type_: String,
}

impl Node {
    fn try_new(node: &rsmgclient::Node) -> Result<Self> {
        let properties = node
            .properties
            .iter()
            .map(|(key, value)| Ok((key.clone(), value_to_json(value)?)))
            .collect::<Result<_>>()?;
        Ok(Self {
            id: node.id,
            label_count: node.label_count,
            labels: node.labels.clone(),
            properties,
            type_: "node".to_string(),
        })
    }
}

#[derive(Debug, PartialEq, Clone, Serialize)]
struct Relationship {
    id: i64,
    start_id: i64,
    end_id: i64,
    label: String,
    #[serde(rename = "type")]
    type_: String,
    properties: HashMap<String, Value>,
}

impl Relationship {
    fn try_new(relationship: &rsmgclient::Relationship) -> Result<Self> {
        let properties = relationship
            .properties
            .iter()
            .map(|(key, value)| Ok((key.clone(), value_to_json(value)?)))
            .collect::<Result<_>>()?;
        Ok(Self {
            id: relationship.id,
            start_id: relationship.start_id,
            end_id: relationship.end_id,
            label: relationship.type_.clone(),
            type_: "relationship".to_string(),
            properties,
        })
    }
}

#[derive(Debug, PartialEq, Clone, Serialize)]
struct UnboundRelationship {
    id: i64,
    label: String,
    #[serde(rename = "type")]
    type_: String,
    properties: HashMap<String, Value>,
}

impl UnboundRelationship {
    fn try_new(relationship: &rsmgclient::UnboundRelationship) -> Result<Self> {
        let properties = relationship
            .properties
            .iter()
            .map(|(key, value)| Ok((key.clone(), value_to_json(value)?)))
            .collect::<Result<_>>()?;
        Ok(Self {
            id: relationship.id,
            label: relationship.type_.clone(),
            type_: "unbound_relationship".to_string(),
            properties,
        })
    }
}

#[derive(Debug, PartialEq, Clone, Serialize)]
struct Path {
    node_count: u32,
    relationship_count: u32,
    nodes: Vec<Node>,
    relationships: Vec<UnboundRelationship>,
}

impl Path {
    fn try_new(path: &rsmgclient::Path) -> Result<Self> {
        Ok(Self {
            node_count: path.node_count,
            relationship_count: path.relationship_count,
            nodes: path
                .nodes
                .iter()
                .map(Node::try_new)
                .collect::<Result<_>>()?,
            relationships: path
                .relationships
                .iter()
                .map(UnboundRelationship::try_new)
                .collect::<Result<_>>()?,
        })
    }
}
