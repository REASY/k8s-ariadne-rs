use super::{Memgraph, decode};
use rsmgclient::Record;
use serde_json::json;

#[test]
fn converts_memgraph_temporal_values_to_json_strings() {
    let date = rsmgclient::Date::new(2026, 7, 23).unwrap();
    let time = rsmgclient::LocalTime::new(14, 5, 9, 123_000_000).unwrap();
    let date_time = rsmgclient::LocalDateTime::new(2026, 7, 23, 14, 5, 9, 123_000_000).unwrap();
    let offset_date_time = rsmgclient::DateTime {
        year: 2026,
        month: 7,
        day: 23,
        hour: 14,
        minute: 5,
        second: 9,
        nanosecond: 123_000_000,
        time_zone_offset_seconds: 19_800,
        time_zone_id: Some("Asia/Kolkata".to_string()),
    };

    assert_eq!(
        decode::value_to_json(&rsmgclient::Value::Date(date)).unwrap(),
        json!("2026-07-23")
    );
    assert_eq!(
        decode::value_to_json(&rsmgclient::Value::LocalTime(time)).unwrap(),
        json!("14:05:09.123")
    );
    assert_eq!(
        decode::value_to_json(&rsmgclient::Value::LocalDateTime(date_time)).unwrap(),
        json!("2026-07-23T14:05:09.123")
    );
    assert_eq!(
        decode::value_to_json(&rsmgclient::Value::DateTime(offset_date_time)).unwrap(),
        json!({
            "type": "datetime",
            "value": "2026-07-23T14:05:09.123+05:30",
            "timezone_id": "Asia/Kolkata"
        })
    );
}

#[test]
fn converts_memgraph_points_to_json_objects() {
    let point_2d = rsmgclient::Point2D {
        srid: 4_326,
        x_longitude: 103.851_959,
        y_latitude: 1.290_27,
    };
    let point_3d = rsmgclient::Point3D {
        srid: 4_979,
        x_longitude: 103.851_959,
        y_latitude: 1.290_27,
        z_height: 15.5,
    };

    assert_eq!(
        decode::value_to_json(&rsmgclient::Value::Point2D(point_2d)).unwrap(),
        json!({
            "type": "point",
            "srid": 4326,
            "x": 103.851959,
            "y": 1.29027
        })
    );
    assert_eq!(
        decode::value_to_json(&rsmgclient::Value::Point3D(point_3d)).unwrap(),
        json!({
            "type": "point",
            "srid": 4979,
            "x": 103.851959,
            "y": 1.29027,
            "z": 15.5
        })
    );
}

#[test]
fn rejects_non_finite_numbers_and_mismatched_records() {
    assert!(decode::value_to_json(&rsmgclient::Value::Float(f64::NAN)).is_err());
    assert!(
        decode::value_to_json(&rsmgclient::Value::Point2D(rsmgclient::Point2D {
            srid: 4_326,
            x_longitude: f64::INFINITY,
            y_latitude: 1.0,
        }))
        .is_err()
    );

    let record = Record {
        values: vec![rsmgclient::Value::Int(1)],
    };
    assert!(decode::record_to_json(&["first".to_string(), "second".to_string()], &record).is_err());
}

#[test]
fn rejects_malformed_memgraph_urls_without_panicking() {
    assert!(Memgraph::try_new_from_url("not-a-url").is_err());
    assert!(Memgraph::try_new_from_url("bolt://:7687").is_err());
    assert!(Memgraph::try_new_from_url("bolt://localhost:not-a-port").is_err());
}

#[test]
fn formats_zero_fraction_and_offset_seconds_without_losing_information() {
    let local_time = rsmgclient::LocalTime::new(0, 0, 0, 0).unwrap();
    let offset_date_time = rsmgclient::DateTime {
        year: -1,
        month: 1,
        day: 2,
        hour: 3,
        minute: 4,
        second: 5,
        nanosecond: 1,
        time_zone_offset_seconds: -3_661,
        time_zone_id: None,
    };

    assert_eq!(
        decode::value_to_json(&rsmgclient::Value::LocalTime(local_time)).unwrap(),
        json!("00:00:00")
    );
    assert_eq!(
        decode::value_to_json(&rsmgclient::Value::DateTime(offset_date_time)).unwrap(),
        json!({
            "type": "datetime",
            "value": "-0001-01-02T03:04:05.000000001-01:01:01"
        })
    );
}
