use vector::Vector;

use crate::cell::{Cell, CellType};
use crate::iter::*;

/// A generic cell for storing parsed data.
pub struct DataFrame {
    /// Column data
    column_data: Vector<Cell>,
    /// Header name for each string
    header: Vec<String>,
    /// Data Type of each header type
    dtype: Vec<CellType>,
}

impl DataFrame {
    /// Collects data from the parser only, should not be accessible to user
    pub(super) fn new(column_data: Vector<Cell>, header: Vec<String>) -> Self {
        let header_len = header.len();
        Self {
            column_data,
            header,
            dtype: vec![CellType::I64; header_len],
        }
    }

    /// Custom iterator that returns the chunk of data to the user.
    ///
    /// To test: performance
    #[inline(always)]
    pub fn iter(&self) -> DataFrameIterator {
        DataFrameIterator::new(&self.column_data, self.header.len())
    }

    /// Length of the column data
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.column_data.len() / self.hlen()
    }

    /// Header length of the Data Frame
    #[inline(always)]
    pub fn hlen(&self) -> usize {
        self.header.len()
    }

    /// Column iterator for the array.
    /// Returns the iterator if column exists
    pub fn iter_col(&self, col: &str) -> Option<DataFrameColumnIterator> {
        let index = self.header.iter().position(|c| c == col)?;

        Some(DataFrameColumnIterator::new(
            &self.column_data,
            self.header.len(),
            index,
        ))
    }
}
