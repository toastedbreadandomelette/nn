use crate::parse_state::ParseState;
use std::thread::Scope;

use crate::cell::{Cell, CellType};
use crate::dframe::DataFrame;
use vector::Vector;

pub struct CsvParser<'a> {
    /// Buffer to parse from
    byte_buffer: &'a [u8],
    /// Current offset
    offset: usize,
    /// Current scan state
    state: ParseState,
    /// Headers
    header_scanned: Vec<String>,
}

impl<'a> CsvParser<'a> {
    /// Create a naive parser
    #[inline]
    pub fn new(byte_buffer: &'a [u8]) -> Self {
        Self {
            byte_buffer,
            offset: 0,
            header_scanned: Vec::new(),
            state: ParseState::Start,
        }
    }

    #[inline(always)]
    pub const fn get_curr_byte(&self) -> Option<u8> {
        if self.offset >= self.byte_buffer.len() {
            None
        } else {
            Some(self.byte_buffer[self.offset])
        }
    }

    /// Move to next byte
    #[inline(always)]
    fn move_next(&mut self) {
        self.offset += 1;
    }

    /// Skip all the whitespaces
    #[inline]
    fn skip_whitespace(&mut self) {
        while let Some(b' ' | b'\n' | 9..=13) = self.get_curr_byte() {
            self.move_next()
        }
    }

    #[inline(always)]
    const fn scan_start(&self) -> ParseState {
        match self.get_curr_byte() {
            Some(b'"') => ParseState::HeaderQuoteStart,
            Some(b',') => ParseState::HeaderSep,
            Some(b'\n') | None => ParseState::NewLine,
            _ => ParseState::HeaderString,
        }
    }

    #[inline]
    pub fn scan_header_quote(&mut self) -> String {
        self.move_next();
        let starting_point = self.offset;

        loop {
            match self.get_curr_byte() {
                Some(b'"') => {
                    self.state = ParseState::HeaderQuoteEnd;
                    break;
                }
                _ => self.move_next(),
            }
        }

        unsafe {
            core::str::from_utf8_unchecked(
                &self.byte_buffer[starting_point..self.offset],
            )
            .to_owned()
        }
    }

    #[inline]
    pub fn scan_header_string(&mut self) -> String {
        let starting_point = self.offset;
        self.move_next();

        loop {
            match self.get_curr_byte() {
                Some(b',') => {
                    self.state = ParseState::HeaderSep;
                    break;
                }
                Some(b'\r' | b'\n') => {
                    self.state = ParseState::NewLine;
                    break;
                }
                _ => self.move_next(),
            }
        }

        unsafe {
            core::str::from_utf8_unchecked(
                &self.byte_buffer[starting_point..self.offset],
            )
            .to_owned()
        }
    }

    /// Scan header
    pub fn scan_header(&mut self) -> (Vec<String>, usize) {
        assert_eq!(self.offset, 0);

        self.skip_whitespace();

        'out: loop {
            match self.state {
                // Scan start, get the current state based on the
                // current byte and move accordingly
                ParseState::Start => self.state = self.scan_start(),

                // Scan start of quoted header string,
                // read till the end of quote.
                ParseState::HeaderQuoteStart => {
                    let scanned_header = self.scan_header_quote();
                    self.header_scanned.push(scanned_header);
                }

                // Scan start of header string,
                // read till the separator or end line
                ParseState::HeaderString => {
                    let scanned_string = self.scan_header_string();
                    self.header_scanned.push(scanned_string);
                }

                // Scan new line: break the line
                ParseState::NewLine => break 'out,

                // End quote or Separator character (usually comma),
                // read quote and decide the current state
                ParseState::HeaderQuoteEnd | ParseState::HeaderSep => {
                    self.move_next();
                    self.state = self.scan_start()
                }

                // End quote or Separator character (usually comma),
                // read quote and decide the current state
                _ => self.state = self.scan_start(),
            }
        }

        (self.header_scanned.to_owned(), self.offset)
    }

    #[inline]
    fn convert_from_slice(slice: &str, state: ParseState) -> (Cell, CellType) {
        match state {
            ParseState::CellNumberStart
            | ParseState::CellNumberCurrent
            | ParseState::CellNumberEnd
            | ParseState::CellQuoteNumberEnd => {
                (Cell::Number(slice.parse::<i64>().unwrap()), CellType::I64)
            }

            ParseState::CellDecimalEnd
            | ParseState::CellDecimalEndWithPointRead
            | ParseState::CellQuoteDecimalEnd
            | ParseState::CellQuoteDecimalEndWithPointRead => {
                (Cell::Decimal(slice.parse::<f64>().unwrap()), CellType::F64)
            }

            _ => (Cell::String(slice.to_owned()), CellType::String),
        }
    }

    /// Split slices of length `total_len` (i.e., `Cell`) each row contains
    /// `multiplier` elements and operated by `split` thread.
    ///
    /// # Safety
    ///
    /// Unsafe, slices `slice` into `split` slices, and returns references or starting
    /// point of each values.
    fn split_slices<'b, T>(
        slice: &'b mut [T],
        // total_len: usize,
        slice_info: &[(usize, usize, usize)],
        // split: usize,
        multiplier: usize,
    ) -> Vec<&'b mut [T]> {
        // Total length of the slice with multiplied .
        // Size of each slice: divided evenly
        // with remainder slice

        // Create array of slices for new array
        // with tracking start pointer
        let (mut mut_slices, mut curr_start) = (Vec::new(), 0);

        // Get pointer
        let ptr = slice.as_mut_ptr();
        slice_info.iter().for_each(|(c, _, _)| {
            // Remainder are added to each starting cell
            // let row_size = *c;

            // Get the reference with the size and push to vector
            let sliced_value = unsafe {
                core::ptr::slice_from_raw_parts_mut(
                    ptr.add(curr_start),
                    c * multiplier,
                )
            };

            curr_start += c * multiplier;
            mut_slices.push(unsafe { &mut *sliced_value });
        });

        mut_slices
    }

    #[inline(always)]
    pub fn aggregate_from_null(curr: CellType) -> CellType {
        curr
    }

    #[inline(always)]
    pub fn aggregate_from_f64(curr: CellType) -> CellType {
        match curr {
            CellType::String => CellType::String,
            _ => CellType::F64,
        }
    }

    #[inline(always)]
    pub fn aggregate_from_i64(curr: CellType) -> CellType {
        match curr {
            CellType::F64 => CellType::F64,
            CellType::String => CellType::String,
            _ => CellType::I64,
        }
    }

    #[inline(always)]
    pub fn agg_type(aggregate_type: CellType, curr: CellType) -> CellType {
        match aggregate_type {
            CellType::Null => Self::aggregate_from_null(curr),
            CellType::F64 => Self::aggregate_from_f64(curr),
            CellType::I64 => Self::aggregate_from_i64(curr),
            _ => CellType::String,
        }
    }

    /// Get total lines from the file
    /// Cannot work properly with csv files handling \r\n new line
    /// (CRLF).
    #[allow(unused_assignments)]
    fn parse_content_on_buffer(
        &mut self,
        column_data: &mut [Cell],
        res_type: &mut [CellType],
    ) {
        // Column data
        let (mut start, mut end): (Option<usize>, Option<usize>) = (None, None);
        let (mut save_state, mut arr_index) = (None, 0);

        self.byte_buffer.iter().enumerate().for_each(|(index, c)| {
            let prev_state = self.state;
            self.state = ParseState::get_scan_state_from_data(self.state, *c);
            // println!("{:?} {:?} {:?} {:?} -> {:?}", start, *c as char, save_state, prev_state, self.state);
            match self.state {
                // Scan start, get the current state based on the
                // current byte and iterator takes care of
                // rest accordingly
                ParseState::Start
                | ParseState::CellString
                | ParseState::CellDecimalStartWithPointRead
                | ParseState::CellNumberStart => {
                    start = Some(index);
                }

                // Starting quoted values,
                ParseState::CellQuoteStart
                | ParseState::CellQuoteNumberStart
                | ParseState::CellQuoteDecimalStart
                | ParseState::CellQuoteDecimalStartWithPointRead => {
                    start = Some(index + 1);
                }

                // Scan start of quoted header string,
                // read till the end of quote.
                ParseState::CellNumberEnd
                | ParseState::CellDecimalEnd
                | ParseState::CellDecimalEndWithPointRead
                | ParseState::CellSep
                | ParseState::NewLine => {
                    let (push_value, result_type) = if end.is_none()
                        && start.is_none()
                    {
                        (Cell::Null, CellType::Null)
                    } else {
                        let ep = end.unwrap_or(index);
                        let save_state_as = save_state.unwrap_or(self.state);
                        let sp = start.unwrap_or(index);
                        unsafe {
                            if sp != ep {
                                let slice =
                                    Self::trim_ascii(&self.byte_buffer[sp..ep]);
                                let str_slice =
                                    core::str::from_utf8_unchecked(slice);

                                Self::convert_from_slice(
                                    str_slice,
                                    save_state_as,
                                )
                            } else {
                                (Cell::Null, CellType::Null)
                            }
                        }
                    };
                    let col = arr_index % res_type.len();
                    (start, end, save_state) = (None, None, None);

                    if arr_index < column_data.len() {
                        column_data[arr_index] = push_value;

                        let prev_type = res_type[col];
                        let val = Self::agg_type(prev_type, result_type);

                        res_type[col] = val;
                    }

                    arr_index += 1;
                }

                // Scan start of quoted header string,
                // read till the end of quote.
                ParseState::CellQuoteEnd
                | ParseState::CellQuoteNumberEnd
                | ParseState::CellQuoteDecimalEnd
                | ParseState::CellQuoteDecimalEndWithPointRead => {
                    end = Some(index);
                    save_state = Some(self.state);
                }

                ParseState::CarriageRet => {
                    if end.is_none() {
                        end = Some(index);
                        save_state = Some(prev_state);
                    }
                }

                // Scan as it is
                _ => {}
            }
        });
    }

    /// Trim ascii having whitespaces, and returns a new `slice`
    #[inline]
    fn trim_ascii(slice: &[u8]) -> &[u8] {
        let start = slice.iter().position(|c| !c.is_ascii_whitespace());
        let end = slice.iter().rev().position(|c| !c.is_ascii_whitespace());

        match (start, end) {
            (Some(st), Some(ed)) => &slice[st..slice.len() - ed],
            (None, Some(ed)) => &slice[..slice.len() - ed],
            (Some(st), None) => &slice[st..],
            (None, None) => slice,
        }
    }

    /// Returns total lines with starting point and ending point
    /// of the buffer to be read.
    ///
    /// ## Note
    /// Not accurate, should also work for multi-lined cell.
    fn get_total_lines_in_a_file<'c>(
        mmaped_buffer: &'c [u8],
        scope: &'c Scope<'c, '_>,
        thread_number: usize,
    ) -> Vec<(usize, usize, usize)> {
        // Thread should be processing sub-array of elements.
        let slots_division = mmaped_buffer.len() / thread_number;

        // Parsing and finding the end point of the line.
        let mut end_prefix = mmaped_buffer[slots_division..]
            .iter()
            .position(|c| *c == b'\n')
            .unwrap_or(0)
            + slots_division;

        let mut slices: Vec<(&'c [u8], usize, usize)> =
            Vec::with_capacity(thread_number);

        slices.push((&mmaped_buffer[..end_prefix], 0, end_prefix));

        slices.extend((1..thread_number - 1).map(|multiplier| {
            let spos = end_prefix + 1;
            let end_pos = (multiplier + 1) * slots_division;

            // Seek the start position to start from position next to \n
            let epos = mmaped_buffer[end_pos..]
                .iter()
                .position(|c| *c == b'\n')
                .unwrap_or(0)
                + end_pos;

            end_prefix = epos;
            (&mmaped_buffer[spos..epos], spos, epos)
        }));

        slices.push((
            &mmaped_buffer[end_prefix..],
            end_prefix + 1,
            mmaped_buffer.len(),
        ));

        slices
            .into_iter()
            .map(|(slice, start, end)| {
                (
                    scope.spawn(move || slice.split(|c| *c == b'\n').count()),
                    start,
                    end,
                )
            })
            .map(|(c, st, ed)| (c.join().unwrap(), st, ed))
            .collect()
    }

    /// Parsing CSV file `file_name` using multiple threads
    ///
    /// Opens the file in memory mapped IO (read-only) and collects the data
    /// on the memory, to be used later via `DataFrame` struct
    ///
    /// To do: Selecting different strategies for parsing: Do either
    /// 1. Read alternate lines
    /// 2. Read batch lines
    ///     - (challenge: seeking starting point to valid new line,
    ///        so this part is incomplete)
    pub fn parse_multi_threaded(
        file_name: &'a str,
        total_threads: usize,
    ) -> DataFrame {
        let fd = std::fs::OpenOptions::new()
            .read(true)
            .open(file_name)
            .unwrap();

        let mmaped = unsafe {
            memmap2::MmapOptions::new()
                .populate()
                .stack()
                .map(&fd)
                .unwrap()
        };

        let mut p = CsvParser::new(&mmaped);
        let (scanned_header, offset_from_scanner) = p.scan_header();
        let next_pos = offset_from_scanner
            + match mmaped[offset_from_scanner..]
                .iter()
                .position(|c| *c == b'\n')
            {
                Some(val) => val + 1,
                None => 0,
            };

        let mmaped_slice = Self::trim_ascii(&mmaped[next_pos..]);
        // Calculate total lines read
        let length = std::thread::scope(|scope| {
            Self::get_total_lines_in_a_file(mmaped_slice, scope, total_threads)
        });
        let c = length.iter().fold(0, |prev, curr| prev + curr.0) - 1;

        // Initialized result with zero value.
        let mut result: Vector<Cell> = Vector::zeroed(c * scanned_header.len());
        let mut result_types: Vec<Vec<CellType>> =
            vec![vec![CellType::Null; scanned_header.len()]; total_threads];

        // UNSAFE CALL: Creates multiple slices of vector `result` into smaller pieces,
        // since reallocating multiple vector or flattening is slower.
        let mut sliced_buffer = Self::split_slices(
            &mut result,
            &length,
            // total_threads,
            scanned_header.len(),
        );

        std::thread::scope(|scope| {
            // Trim whitespaces
            // To do: for each thread, start from offset just next to new line
            let mmaped2 = &mmaped_slice;

            sliced_buffer
                .iter_mut()
                .zip(result_types.iter_mut())
                .zip(length)
                .enumerate()
                .for_each(|(_, ((res, res_types), (len, start, end)))| {
                    // Each thread is alloted a specific `non-overlapping` region of the
                    // slice in `result`, which is ensured by function `split_slices`
                    // The values are recorded in res.
                    debug_assert_eq!(res.len(), len * scanned_header.len());
                    // let type_slice = &mut result_types[index][..];
                    scope.spawn(move || {
                        CsvParser::new(&mmaped2[start..end])
                            .parse_content_on_buffer(res, &mut res_types[..]);
                    });
                });
        });

        let res = result_types.iter_mut().fold(
            vec![CellType::Null; scanned_header.len()],
            |mut prev, arr| {
                prev.iter_mut()
                    .zip(arr)
                    .for_each(|(p, c)| *p = Self::agg_type(*p, *c));
                prev
            },
        );

        DataFrame::new(result, scanned_header, res)
    }

    /// Parsing CSV file `file_name` using single thread
    ///
    /// Opens the file in memory mapped IO (read-only) and
    /// collects the data from file
    #[inline]
    #[allow(unused)]
    pub fn parse(file_name: &'a str) -> DataFrame {
        Self::parse_multi_threaded(file_name, 1)
    }
}
