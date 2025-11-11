use schemars::Schema;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::convert::TryFrom;

pub struct Property {
    pub name: String,
    pub data_type: String,
}

impl Property {
    pub fn new(name: String, data_type: String) -> Self {
        Self { name, data_type }
    }
}

pub struct Type {
    pub name: String,
    pub properties: Vec<Property>,
}

impl Type {
    pub fn new(name: String, properties: Vec<Property>) -> Self {
        Self { name, properties }
    }
}

pub struct SchemaInfo {
    pub root_type: Type,
    pub definitions: BTreeMap<String, Type>,
}

impl SchemaInfo {
    fn new(root_type: Type, definitions: BTreeMap<String, Type>) -> Self {
        Self {
            root_type,
            definitions,
        }
    }
}

pub fn get_schema(schema: &Schema) -> SchemaInfo {
    let object = schema
        .as_object()
        .expect("Root schema should be represented as an object");

    let mut definitions: BTreeMap<String, Type> = BTreeMap::new();

    if let Some(defs_value) = object.get("$defs").or_else(|| object.get("definitions")) {
        if let Some(defs) = defs_value.as_object() {
            for (full_name, value) in defs {
                if let Ok(def_schema) = Schema::try_from(value.clone()) {
                    if let Some(info) = process_schema(full_name, &def_schema) {
                        definitions.insert(full_name.clone(), info);
                    }
                }
            }
        }
    }

    let root_title = object
        .get("title")
        .and_then(Value::as_str)
        .map(short_type_name)
        .unwrap_or("Unknown")
        .to_string();

    let mut root_type = process_schema(&root_title, schema).unwrap_or_else(|| {
        Type::new(
            root_title.clone(),
            object
                .get("properties")
                .and_then(Value::as_object)
                .map(|props| {
                    props
                        .keys()
                        .map(|name| Property::new(name.clone(), "ANY".to_string()))
                        .collect()
                })
                .unwrap_or_default(),
        )
    });

    customize_root_type(&mut root_type);

    SchemaInfo::new(root_type, definitions)
}

pub fn write_schema_prompt(schema_list: Vec<SchemaInfo>) -> String {
    let mut prompt = String::from("Node properties:\n");
    let mut all_defs: BTreeMap<String, Type> = BTreeMap::new();

    for mut schema in schema_list {
        let type_expr = to_type_expression(&schema.root_type);
        prompt += &type_expr;
        all_defs.append(&mut schema.definitions);
    }

    prompt += "Referenced types (used via `#/$defs/`):\n";
    for (_, definition) in all_defs {
        let type_expr = to_type_expression(&definition);
        prompt += &type_expr;
    }
    prompt.push('\n');
    prompt
}

fn to_type_expression(root_type: &Type) -> String {
    let name = root_type.name.as_str();
    let properties = root_type.properties.as_slice();
    let props_with_type = properties
        .iter()
        .map(|property| format!("{}: {}", property.name, property.data_type))
        .collect::<Vec<String>>()
        .join(", ");
    let prop_message = if properties.len() > 1 {
        "properties"
    } else {
        "property"
    };
    format!(
        "  {name}: {} {prop_message} ({props_with_type})\n",
        properties.len()
    )
}

fn process_schema(type_name: &str, schema: &Schema) -> Option<Type> {
    let object = schema.as_object()?;
    let properties = object.get("properties")?.as_object()?;
    if properties.is_empty() {
        return None;
    }

    let mut props_info: Vec<Property> = Vec::with_capacity(properties.len());
    for (prop_name, prop_schema_value) in properties {
        let schema = match Schema::try_from(prop_schema_value.clone()) {
            Ok(schema) => schema,
            Err(_) => continue,
        };
        let mut type_name = get_type_name(&schema);
        normalize_container_type(prop_name, &mut type_name);
        props_info.push(Property::new(prop_name.clone(), type_name));
    }

    Some(Type::new(type_name.to_string(), props_info))
}

fn get_type_name(schema: &Schema) -> String {
    if let Some(value) = schema.as_bool() {
        return if value {
            "ANY".to_string()
        } else {
            "NEVER".to_string()
        };
    }

    let object = match schema.as_object() {
        Some(object) => object,
        None => return "ANY".to_string(),
    };

    if let Some(reference) = extract_reference(object) {
        return map_reference(reference);
    }

    if let Some(types) = get_from_discriminator(object, "anyOf") {
        return types;
    }

    if let Some(types) = get_from_discriminator(object, "oneOf") {
        return types;
    }

    if let Some(type_value) = object.get("type") {
        if let Some(type_str) = type_value.as_str() {
            return match type_str {
                "string" => "STRING".to_string(),
                "number" => "FLOAT".to_string(),
                "integer" => "INTEGER".to_string(),
                "boolean" => "BOOLEAN".to_string(),
                "object" => "MAP".to_string(),
                "null" => "NULL".to_string(),
                "array" => {
                    let item_type = object
                        .get("items")
                        .and_then(|items| match items {
                            Value::Object(_) | Value::Bool(_) => Schema::try_from(items.clone())
                                .ok()
                                .map(|schema| get_type_name(&schema)),
                            Value::Array(array) if array.len() == 1 => Schema::try_from(
                                array
                                    .first()
                                    .cloned()
                                    .expect("array should have at least one element"),
                            )
                            .ok()
                            .map(|schema| get_type_name(&schema)),
                            Value::Array(_) => Some("TUPLE".to_string()),
                            _ => None,
                        })
                        .unwrap_or_else(|| "ANY".to_string());
                    format!("[{item_type}]")
                }
                other => other.to_uppercase(),
            };
        } else if let Some(types) = type_value.as_array() {
            let mut resolved: Vec<String> = types
                .iter()
                .filter_map(Value::as_str)
                .filter(|ty| *ty != "null")
                .map(|ty| match ty {
                    "string" => "STRING".to_string(),
                    "number" => "FLOAT".to_string(),
                    "integer" => "INTEGER".to_string(),
                    "boolean" => "BOOLEAN".to_string(),
                    "object" => "MAP".to_string(),
                    "array" => "[ANY]".to_string(),
                    other => other.to_uppercase(),
                })
                .collect();
            resolved.sort();
            resolved.dedup();
            if resolved.len() == 1 {
                return resolved.pop().unwrap();
            } else if resolved.is_empty() {
                return "NULL".to_string();
            } else {
                return "ANY".to_string();
            }
        }
    }

    if let Some(all_of) = object.get("allOf").and_then(Value::as_array) {
        for schema in all_of {
            if let Some(reference) = schema
                .as_object()
                .and_then(|inner| extract_reference(inner))
            {
                return map_reference(reference);
            }
        }
    }

    if let Some(const_value) = object.get("const") {
        if const_value.is_string() {
            return "STRING".to_string();
        } else if const_value.is_number() {
            return "FLOAT".to_string();
        } else if const_value.is_boolean() {
            return "BOOLEAN".to_string();
        }
    }

    "ANY".to_string()
}

fn extract_reference(object: &Map<String, Value>) -> Option<&str> {
    object.get("$ref").and_then(Value::as_str)
}

fn map_reference(reference: &str) -> String {
    match reference {
        "#/$defs/io.k8s.apimachinery.pkg.apis.meta.v1.Time"
        | "#/$defs/io.k8s.apimachinery.pkg.apis.meta.v1.MicroTime"
        | "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.Time"
        | "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.MicroTime" => "DATETIME_UTC".into(),
        _ => reference.to_string(),
    }
}

fn short_type_name(full_name: &str) -> &str {
    full_name.rsplit('.').next().unwrap_or(full_name)
}

fn get_from_discriminator(object: &Map<String, Value>, key: &str) -> Option<String> {
    let entries = object.get(key)?.as_array()?;
    let mut resolved: Vec<String> = entries
        .iter()
        .filter_map(|value| Schema::try_from(value.clone()).ok())
        .map(|schema| get_type_name(&schema))
        .filter(|ty| ty != "NULL")
        .collect();
    if resolved.is_empty() {
        return None;
    }
    resolved.sort();
    resolved.dedup();
    if resolved.len() == 1 {
        return Some(resolved.pop().unwrap());
    }
    Some("ANY".to_string())
}

fn customize_root_type(root_type: &mut Type) {
    if root_type.name == "Cluster"
        && !root_type
            .properties
            .iter()
            .any(|p| p.name == "retrieved_at")
    {
        root_type.properties.push(Property::new(
            "retrieved_at".to_string(),
            "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.Time".to_string(),
        ));
    }
    if root_type.name == "Container" {
        for property in &mut root_type.properties {
            normalize_container_type(&property.name, &mut property.data_type);
        }
    }
    if root_type.name == "Endpoint" {
        rename_property(root_type, "node_name", "nodeName");
        rename_property(root_type, "target_ref", "targetRef");
        if !root_type
            .properties
            .iter()
            .any(|p| p.name == "deprecatedTopology")
        {
            root_type.properties.push(Property::new(
                "deprecatedTopology".to_string(),
                "MAP".to_string(),
            ));
        }
    }
}

fn normalize_container_type(property_name: &str, data_type: &mut String) {
    if property_name == "container_type" && data_type == "#/definitions/ContainerType" {
        *data_type = "STRING".to_string();
    }
}

fn rename_property(ty: &mut Type, from: &str, to: &str) {
    if ty.properties.iter().any(|p| p.name == to) {
        return;
    }
    if let Some(property) = ty.properties.iter_mut().find(|p| p.name == from) {
        property.name = to.to_string();
    }
}
