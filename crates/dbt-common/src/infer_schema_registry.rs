use crate::relation_registry::RelationRegistry;

#[derive(Clone, Debug)]
pub struct InferredSchema {
    pub columns: Vec<String>,
    pub closed: bool,
}

static REGISTRY: RelationRegistry<InferredSchema> = RelationRegistry::new();

pub fn register(relation: &str, columns: &[String], closed: bool) {
    REGISTRY.register(
        relation,
        InferredSchema {
            columns: columns.to_vec(),
            closed,
        },
    );
}

pub fn lookup(relation: &str) -> Option<InferredSchema> {
    REGISTRY.lookup(relation)
}
