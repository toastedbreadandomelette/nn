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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CellType {
    /// String data type
    String,
    /// Signed Integer
    I64,
    /// Floating Number
    F64,
    /// Empty or null
    Null,
}

impl CellType {
    #[inline(always)]
    pub(crate) fn infer_type(&self, current_type: Self) -> Self {
        match self {
            Self::Null => current_type,
            Self::F64 => current_type.infer_from_f64(),
            Self::I64 => current_type.infer_from_i64(),
            _ => Self::String,
        }
    }

    #[inline(always)]
    fn infer_from_f64(self) -> Self {
        if self == Self::String {
            return Self::String;
        }

        self
    }
    #[inline(always)]
    pub fn infer_from_i64(self) -> Self {
        if let Self::I64 | Self::Null = self {
            Self::I64
        } else {
            self
        }
    }
}
