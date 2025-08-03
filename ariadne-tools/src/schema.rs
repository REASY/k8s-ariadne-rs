use k8s_openapi::schemars::schema::{InstanceType, RootSchema, Schema, SingleOrVec};
use std::collections::BTreeMap;

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

pub fn get_schema(schema: &RootSchema) -> SchemaInfo {
    let mut results: BTreeMap<String, Type> = BTreeMap::new();

    // The `definitions` map in the RootSchema contains the schema for every complex type
    // referenced in the root object's schema. We iterate through them to process each one.
    for (full_name, schema) in &schema.definitions {
        // The full name is like "io.k8s.api.core.v1.ContainerStateTerminated".
        if let Some(info) = process_schema(full_name.as_str(), schema) {
            results.insert(full_name.clone(), info);
        }
    }

    // After processing the definitions, we process the root schema object itself.
    // This corresponds to the `ContainerState` type.
    let root_schema_obj = &schema.schema;
    let root_name = root_schema_obj
        .metadata
        .as_ref()
        .expect("Root schema should have metadata")
        .title
        .as_deref()
        .and_then(|t| Some(t.split('.').last().unwrap_or(t)))
        .expect("Root schema metadata should have a title");

    let root_type: Type =
        process_schema(root_name, &Schema::Object(root_schema_obj.clone())).unwrap();
    SchemaInfo::new(root_type, results)
}

pub fn write_schema_prompt(schema_list: Vec<SchemaInfo>) -> String {
    let mut prompt = String::from("Node properties:\n");
    let mut all_defs: BTreeMap<String, Type> = BTreeMap::new();
    // Print all the collected and sorted results.
    for mut s in schema_list {
        let type_expr = to_type_expression(&s.root_type);
        prompt += &type_expr;
        all_defs.append(&mut s.definitions);
    }
    prompt += "Referenced types (used via `#/definitions/`):\n";
    for (_, def) in all_defs {
        let type_expr = to_type_expression(&def);
        prompt += &type_expr;
    }
    prompt += "\n";
    prompt
}

fn to_type_expression(root_type: &Type) -> String {
    let name = root_type.name.as_str();
    let props = root_type.properties.as_slice();
    let props_with_type = props
        .iter()
        .map(|p| format!("{}: {}", p.name, p.data_type))
        .collect::<Vec<String>>()
        .join(", ");
    let prop_message = if props.len() > 1 {
        "properties"
    } else {
        "property"
    };
    let line = format!(
        "  {name}: {} {prop_message} ({props_with_type})\n",
        props.len()
    );
    line
}

/// Processes a single schema object to extract its properties.
/// Returns a tuple containing the property count and a formatted string of property names and types.
fn process_schema(type_name: &str, schema: &Schema) -> Option<Type> {
    if let Schema::Object(obj) = schema {
        // We are interested in schemas that describe objects with properties.
        if let Some(object_validation) = &obj.object {
            let properties = &object_validation.properties;
            assert_ne!(properties.len(), 0);

            // Collect information about each property.
            let props_info: Vec<Property> = properties
                .iter()
                .map(|(prop_name, prop_schema)| {
                    Property::new(prop_name.clone(), get_type_name(prop_schema))
                })
                .collect();
            let type_info = Type::new(type_name.to_string(), props_info);
            return Some(type_info);
        }
    }
    None
}

/// A helper function to determine the type name of a property from its schema.
/// This function handles direct types (like String, Number) and references to other types.
fn get_type_name(schema: &Schema) -> String {
    let obj = match schema {
        Schema::Object(o) => o,
        Schema::Bool(true) => return "ANY".to_string(),
        Schema::Bool(false) => return "NEVER".to_string(),
    };

    // If the property is a reference to another definition (e.g., "$ref": "#/definitions/..."),
    // we extract and return the referenced type's short name.
    if let Some(ref_path) = &obj.reference {
        return ref_path.to_string();
    }

    // Handle primitive types like string, number, boolean, etc.
    if let Some(instance_type) = &obj.instance_type {
        return match instance_type {
            SingleOrVec::Single(ty) => match **ty {
                InstanceType::String => "STRING".to_string(),
                InstanceType::Number => "FLOAT".to_string(),
                InstanceType::Integer => "INTEGER".to_string(),
                InstanceType::Boolean => "BOOLEAN".to_string(),
                InstanceType::Object => "MAP".to_string(),
                InstanceType::Null => "NULL".to_string(),
                // If the type is an array, recursively find the type of its items.
                InstanceType::Array => {
                    let item_type = obj
                        .array
                        .as_ref()
                        .and_then(|av| av.items.as_ref())
                        .map(|items| match items {
                            SingleOrVec::Single(item_schema) => get_type_name(item_schema),
                            SingleOrVec::Vec(_) => "TUPLE".to_string(),
                        })
                        .unwrap_or_else(|| "ANY".to_string());
                    format!("[{}]", item_type)
                }
            },
            // The property can be one of several types.
            SingleOrVec::Vec(_) => "ANY".to_string(),
        };
    }

    if let Some(sub) = &obj.subschemas {
        assert!(sub.any_of.is_none());
        assert!(sub.one_of.is_none());

        let v = sub.all_of.as_ref().expect("all_of should not be None");
        assert_eq!(v.len(), 1);
        match v.first().unwrap() {
            Schema::Bool(_) => {
                panic!("all_of should not contain bools");
            }
            Schema::Object(r) => {
                let ref_type = r.reference.as_ref().unwrap();
                if ref_type == "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.Time" {
                    "DATETIME_UTC".to_string()
                } else {
                    ref_type.to_string()
                }
            }
        }
    } else {
        panic!("Schema should have subschemas");
    }
}
