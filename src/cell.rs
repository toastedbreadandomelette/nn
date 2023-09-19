#[derive(Debug, Clone)]
pub enum Cell {
    /// Null
    Null,
    /// String
    String(String),
    /// Number
    Number(i64),
    /// Decimal value
    Decimal(f64),
}

#[derive(Clone, Copy, Debug)]
pub enum CellType {
    /// String
    String,
    /// Signed Integer
    I64,
    /// Floating Number
    F64,
    /// Empty or null
    Null,
}
