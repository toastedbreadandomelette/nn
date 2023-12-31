use crate::cell::Cell;

/// Custom iterator for data type [`DataFrame`]:
/// `DataFrameIterator`
pub struct DataFrameIterator<'a> {
    /// Actual data frame
    data_frame: &'a [Cell],
    /// Column size for iterating through each cell
    col_size: usize,
    /// Index that points to current cell
    index: usize,
}

impl<'a> DataFrameIterator<'a> {
    #[inline(always)]
    pub fn new(data_frame: &'a [Cell], col_size: usize) -> Self {
        Self {
            data_frame,
            col_size,
            index: 0,
        }
    }
}

impl<'a> Iterator for DataFrameIterator<'a> {
    type Item = &'a [Cell];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.data_frame.len() {
            None
        } else {
            let cell_slice =
                &self.data_frame[self.index..self.index + self.col_size];
            self.index += self.col_size;
            Some(cell_slice)
        }
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remainder_size =
            (self.data_frame.len() - self.index) / self.col_size;
        (remainder_size, Some(remainder_size))
    }

    #[inline(always)]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if n >= self.data_frame.len() / self.col_size {
            None
        } else {
            let start = n * self.col_size;
            let cell_slice = &self.data_frame[start..start + self.col_size];
            Some(cell_slice)
        }
    }
}
