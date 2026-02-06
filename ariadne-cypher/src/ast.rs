#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub clauses: Vec<Clause>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum Clause {
    Match(MatchClause),
    Unwind(UnwindClause),
    With(WithClause),
    Return(ReturnClause),
    Call(CallClause),
    Updating(UpdatingClause),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchClause {
    pub optional: bool,
    pub pattern: Pattern,
    pub where_clause: Option<Expr>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnwindClause {
    pub expression: Expr,
    pub variable: String,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    pub distinct: bool,
    pub items: Vec<ProjectionItem>,
    pub order: Option<OrderBy>,
    pub skip: Option<Expr>,
    pub limit: Option<Expr>,
    pub where_clause: Option<Expr>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReturnClause {
    pub distinct: bool,
    pub items: Vec<ProjectionItem>,
    pub order: Option<OrderBy>,
    pub skip: Option<Expr>,
    pub limit: Option<Expr>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CallClause {
    pub name: String,
    pub args: Vec<Expr>,
    pub yields: Option<Vec<YieldItem>>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct YieldItem {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdatingClause {
    pub kind: UpdatingClauseKind,
    pub span: Span,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UpdatingClauseKind {
    Create,
    Merge,
    Delete,
    Set,
    Remove,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    pub items: Vec<OrderItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderItem {
    pub expr: Expr,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Pattern {
    Node(NodePattern),
    Relationship(RelationshipPattern),
    Path(PathPattern),
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodePattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelationshipPattern {
    pub left: NodePattern,
    pub rel: RelationshipDetail,
    pub right: NodePattern,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PathPattern {
    pub start: NodePattern,
    pub segments: Vec<PathSegment>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PathSegment {
    pub rel: RelationshipDetail,
    pub node: NodePattern,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelationshipDetail {
    pub variable: Option<String>,
    pub types: Vec<String>,
    pub direction: RelationshipDirection,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RelationshipDirection {
    LeftToRight,
    RightToLeft,
    Undirected,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(Literal),
    Variable(String),
    Star,
    PropertyAccess {
        expr: Box<Expr>,
        key: String,
    },
    IndexAccess {
        expr: Box<Expr>,
        index: Box<Expr>,
    },
    ListSlice {
        expr: Box<Expr>,
        start: Option<Box<Expr>>,
        end: Option<Box<Expr>>,
    },
    FunctionCall {
        name: String,
        args: Vec<Expr>,
    },
    CountStar,
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    BinaryOp {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    In {
        expr: Box<Expr>,
        list: Box<Expr>,
    },
    HasLabel {
        expr: Box<Expr>,
        labels: Vec<String>,
    },
    Case {
        base: Option<Box<Expr>>,
        alternatives: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    Exists {
        pattern: Pattern,
        where_clause: Option<Box<Expr>>,
    },
    ListComprehension {
        variable: String,
        list: Box<Expr>,
        where_clause: Option<Box<Expr>>,
        map: Box<Expr>,
    },
    Quantifier {
        kind: QuantifierKind,
        variable: String,
        list: Box<Expr>,
        where_clause: Option<Box<Expr>>,
    },
    Parameter(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum QuantifierKind {
    Any,
    All,
    None,
    Single,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Not,
    Neg,
    Pos,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOp {
    Or,
    Xor,
    And,
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    StartsWith,
    EndsWith,
    Contains,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
    List(Vec<Expr>),
    Map(Vec<(String, Expr)>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start_byte: usize,
    pub end_byte: usize,
    pub start_row: usize,
    pub start_col: usize,
    pub end_row: usize,
    pub end_col: usize,
}

impl Span {
    pub fn display(&self) -> String {
        format!(
            "{}:{}-{}:{}",
            self.start_row + 1,
            self.start_col + 1,
            self.end_row + 1,
            self.end_col + 1
        )
    }
}
