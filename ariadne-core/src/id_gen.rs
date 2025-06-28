use std::collections::HashMap;

pub enum GetNextIdResult {
    Existing(u32),
    New(u32),
}

#[derive(Debug, Default)]
pub struct IdGen {
    next_id: u32,
    id_to_str: HashMap<u32, String>,
    str_to_id: HashMap<String, u32>,
}

impl IdGen {
    pub fn new() -> IdGen {
        IdGen {
            next_id: 0,
            id_to_str: HashMap::new(),
            str_to_id: HashMap::new(),
        }
    }

    pub fn get_next_id(&mut self, str: &str) -> GetNextIdResult {
        let r = match self.str_to_id.get(str) {
            None => {
                let id = self.next_id;
                assert_ne!(id, u32::MAX, "Reached u32::MAX");

                self.str_to_id.insert(str.to_string(), id);
                self.id_to_str.insert(id, str.to_string());
                self.next_id += 1;
                GetNextIdResult::New(id)
            }
            Some(id) => GetNextIdResult::Existing(*id),
        };
        r
    }

    pub fn get_by_id(&self, id: u32) -> Option<String> {
        self.id_to_str.get(&id).map(|r| r.clone())
    }

    pub fn get_id(&self, str: &str) -> Option<u32> {
        self.str_to_id.get(str).map(|r| *r)
    }
}
