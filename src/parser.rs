use std::thread::ScopedJoinHandle;

use crate::cell::Cell;
use crate::dframe::DataFrame;
use vector::Vector;

pub struct CsvParser<'a> {
    /// Buffer to parse from
    byte_buffer: &'a [u8],
    /// Id: (Might be useful for multithreading: [`std::thread`])
    id: usize,
    /// Current offset
    offset: usize,
    /// Batch size
    batch_size: usize,
    /// Current scan state
    state: ParseState,
    /// Headers
    header_scanned: Vec<String>,
}

/// State evaluator that tells the current data type and
/// nature of parsing data based of previous state and the current byte
/// the buffer returns.
#[allow(unused)]
#[derive(Copy, Clone, Debug, PartialEq, PartialOrd)]
pub enum ParseState {
    /// Start of parsing section
    Start,
    /// Reading Header String
    HeaderString,
    /// Reading Quote Start
    HeaderQuoteStart,
    /// Reading Closing Quote
    HeaderQuoteEnd,
    /// Reading Separator in header
    HeaderSep,

    /// Cell value start, which is a string
    CellString,
    /// Cell value, which is a string
    CellCurrent,

    /// Cell quote start, which is a string
    CellQuoteStart,
    /// Cell quote body, which is a string
    CellQuoteCurrent,
    /// Cell quote end, which is a string
    CellQuoteEnd,

    /// Cell quote start, which is a number
    CellQuoteNumberStart,
    /// Cell quote body, which is a number
    CellQuoteNumberCurrent,
    /// Cell quote end, which is a number
    CellQuoteNumberEnd,

    /// Cell quote start, which is a number
    CellQuoteDecimalStart,
    /// Cell quote body, which is a number
    CellQuoteDecimalCurrent,
    /// Cell quote end, which is a number
    CellQuoteDecimalEnd,

    /// Start reading number
    CellNumberStart,
    /// Reading number
    CellNumberCurrent,
    /// End reading number
    CellNumberEnd,

    /// Start reading decimal number
    CellDecimalStart,
    /// Read decimal number
    CellDecimalCurrent,
    /// End reading decimal number
    CellDecimalEnd,

    /// Read decimal number with decimal point read
    CellDecimalStartWithPointRead,
    /// Read decimal number with decimal point read
    CellDecimalCurrentWithPointRead,
    /// Read decimal number with decimal point read
    CellDecimalEndWithPointRead,

    /// Read decimal number with decimal point read
    CellQuoteDecimalStartWithPointRead,
    /// Read decimal number with decimal point read
    CellQuoteDecimalCurrentWithPointRead,
    /// Read decimal number with decimal point read
    CellQuoteDecimalEndWithPointRead,

    /// Read separator
    CellSep,
    /// Skip character
    SkipChar,
    /// Skip whitespace within cell
    SkippedStartWhitespace,
    /// Skip whitespace but assume might be the end
    SkippedAssumeEndWhitespace,
    /// Reading new line character
    NewLine,
    /// File end
    EndFile,
}

impl<'a> CsvParser<'a> {
    /// Create a naive parser
    #[inline]
    pub fn new(byte_buffer: &'a [u8]) -> Self {
        Self {
            byte_buffer,
            id: 0,
            offset: 0,
            batch_size: 2048,
            header_scanned: Vec::new(),
            state: ParseState::Start,
        }
    }

    /// Create a parser with assigned ID
    #[inline]
    pub fn with_id(byte_buffer: &'a [u8], id: usize) -> Self {
        Self {
            byte_buffer,
            id,
            offset: 0,
            batch_size: 2048,
            state: ParseState::Start,
            header_scanned: Vec::new(),
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

    /// Start scanning this buffer from certain offset
    #[inline]
    pub fn with_id_from_offset(
        byte_buffer: &'a [u8],
        id: usize,
        offset: usize,
    ) -> Self {
        Self {
            byte_buffer,
            id,
            offset,
            batch_size: 2048,
            state: ParseState::Start,
            header_scanned: Vec::new(),
        }
    }

    #[inline(always)]
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    #[inline(always)]
    pub fn set_batch_size(&mut self, batch_size: usize) {
        self.batch_size = batch_size;
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

    /// Handle transition to states when current state is
    /// reading a decimal value
    #[inline(always)]
    const fn handle_decimal_state(initial_state: ParseState) -> ParseState {
        match initial_state {
            ParseState::CellNumberCurrent | ParseState::CellNumberStart => {
                ParseState::CellDecimalCurrentWithPointRead
            }

            ParseState::CellQuoteStart => {
                ParseState::CellQuoteDecimalStartWithPointRead
            }

            ParseState::CellQuoteCurrent => ParseState::CellQuoteCurrent,

            // While reading potential number, switch to
            // potential quoted decimal number
            ParseState::CellQuoteNumberStart
            | ParseState::CellQuoteNumberCurrent => {
                ParseState::CellQuoteDecimalCurrentWithPointRead
            }

            // If quoted decimal is already acknowledged, then it's not
            // a decimal value, but a quoted string
            ParseState::CellQuoteDecimalStartWithPointRead
            | ParseState::CellQuoteDecimalCurrentWithPointRead => {
                ParseState::CellQuoteCurrent
            }

            // If decimal is already acknowledged, then it's not
            // a decimal value, but a string
            ParseState::CellString
            | ParseState::CellDecimalStartWithPointRead
            | ParseState::CellDecimalCurrentWithPointRead
            | ParseState::CellCurrent => ParseState::CellCurrent,

            _ => ParseState::CellDecimalStartWithPointRead,
        }
    }

    /// Handle transition to states when current state is
    /// reading certain value
    #[inline(always)]
    const fn handle_default(initial_state: ParseState) -> ParseState {
        match initial_state {
            // If quoted, continue reading.
            ParseState::CellQuoteStart | ParseState::CellQuoteCurrent => {
                ParseState::CellQuoteCurrent
            }

            // Does matter iff any special number, switch to
            // normal non-quoted string.
            ParseState::CellString
            | ParseState::CellCurrent
            | ParseState::CellNumberCurrent
            | ParseState::CellDecimalCurrent
            | ParseState::CellDecimalStartWithPointRead
            | ParseState::CellDecimalCurrentWithPointRead
            | ParseState::CellQuoteDecimalStartWithPointRead
            | ParseState::CellQuoteDecimalCurrentWithPointRead => {
                ParseState::CellCurrent
            }

            _ => ParseState::CellString,
        }
    }

    /// Handle transition to states when current state is
    /// reading separator
    #[inline(always)]
    const fn handle_separator(initial_state: ParseState) -> ParseState {
        match initial_state {
            // Any quoted values defaults to quoted string, knowing
            // that separator is read.
            ParseState::CellQuoteCurrent
            | ParseState::CellQuoteStart
            | ParseState::CellQuoteDecimalCurrent
            | ParseState::CellQuoteDecimalStart
            | ParseState::CellQuoteDecimalStartWithPointRead
            | ParseState::CellQuoteDecimalCurrentWithPointRead => {
                ParseState::CellQuoteCurrent
            }

            ParseState::CellNumberCurrent | ParseState::CellNumberStart => {
                ParseState::CellNumberEnd
            }

            ParseState::CellDecimalCurrent
            | ParseState::CellDecimalStart
            | ParseState::CellDecimalStartWithPointRead
            | ParseState::CellDecimalCurrentWithPointRead => {
                ParseState::CellDecimalEnd
            }

            _ => ParseState::CellSep,
        }
    }

    #[inline(always)]
    const fn handle_number(initial_state: ParseState) -> ParseState {
        match initial_state {
            ParseState::CellQuoteDecimalStartWithPointRead
            | ParseState::CellQuoteDecimalCurrentWithPointRead => {
                ParseState::CellQuoteDecimalCurrentWithPointRead
            }

            ParseState::CellQuoteStart
            | ParseState::CellQuoteNumberStart
            | ParseState::CellQuoteNumberCurrent => {
                ParseState::CellQuoteNumberCurrent
            }

            ParseState::CellQuoteCurrent => ParseState::CellQuoteCurrent,

            ParseState::CellNumberCurrent | ParseState::CellNumberStart => {
                ParseState::CellNumberCurrent
            }

            ParseState::CellDecimalStartWithPointRead
            | ParseState::CellDecimalCurrentWithPointRead => {
                ParseState::CellDecimalCurrentWithPointRead
            }

            ParseState::CellDecimalCurrent | ParseState::CellDecimalStart => {
                ParseState::CellDecimalCurrent
            }

            ParseState::CellString | ParseState::CellCurrent => {
                ParseState::CellCurrent
            }

            _ => ParseState::CellNumberStart,
        }
    }

    /// Evaluate next state `ParseState` given the `initial_state`
    /// and knowing that character is end line.
    ///
    /// Jump from
    /// ```
    /// CellQuoteCurrent | CellQuoteStart => CellQuoteCurrent, /// (While reading quotes, we're still reading cell),
    /// CellNumberCurrent | CellNumberStart => CellNumberEnd,
    /// CellDecimalStart | CellDecimalCurrent => CellDecimalEnd,
    /// ```
    ///
    #[inline(always)]
    const fn handle_lf(initial_state: ParseState, c: u8) -> ParseState {
        match initial_state {
            // Starting with quote and running into new-line characters
            // should default to normal quoted string.
            ParseState::CellQuoteCurrent
            | ParseState::CellQuoteStart
            | ParseState::CellQuoteDecimalCurrent
            | ParseState::CellQuoteDecimalStart
            | ParseState::CellQuoteDecimalStartWithPointRead
            | ParseState::CellQuoteDecimalCurrentWithPointRead => {
                ParseState::CellQuoteCurrent
            }

            ParseState::CellNumberCurrent | ParseState::CellNumberStart => {
                ParseState::CellNumberEnd
            }

            ParseState::CellDecimalCurrent
            | ParseState::CellDecimalStart
            | ParseState::CellDecimalStartWithPointRead
            | ParseState::CellDecimalCurrentWithPointRead => {
                ParseState::CellDecimalEnd
            }

            _ => {
                if c == b'\r' {
                    ParseState::CellSep
                } else {
                    ParseState::NewLine
                }
            }
        }
    }

    /// Evaluate next state `ParseState` given the `initial_state`
    /// and the `byte`.
    ///
    /// ## To Do
    /// - Handle for generic separator
    /// - Maybe move from byte to char or byte sequence
    #[inline]
    const fn get_scan_state_from_data(
        initial_state: ParseState,
        c: u8,
    ) -> ParseState {
        match c {
            // If quote is started, end it else start the quote
            b'"' => match initial_state {
                // If previous started or running, end the values
                ParseState::CellQuoteStart | ParseState::CellQuoteCurrent => {
                    ParseState::CellQuoteEnd
                }
                ParseState::CellQuoteDecimalStart
                | ParseState::CellQuoteDecimalCurrent => {
                    ParseState::CellQuoteDecimalEnd
                }
                ParseState::CellQuoteDecimalStartWithPointRead
                | ParseState::CellQuoteDecimalCurrentWithPointRead => {
                    ParseState::CellQuoteDecimalEndWithPointRead
                }
                _ => ParseState::CellQuoteStart,
            },

            // Handle when a single point is read by the parser
            b'.' => Self::handle_decimal_state(initial_state),

            // Handle when a single point is read by the parser
            b'0'..=b'9' => Self::handle_number(initial_state),

            // To-do Handle generic separator
            b',' => Self::handle_separator(initial_state),

            b'\r' | b'\n' => Self::handle_lf(initial_state, c),
            // b' ' => ParseState::SkippedStartWhitespace,
            _ => Self::handle_default(initial_state),
        }
    }

    #[inline]
    fn convert_from_slice(slice: &str, state: ParseState) -> Cell {
        match state {
            ParseState::CellNumberEnd | ParseState::CellQuoteNumberEnd => {
                Cell::Number(slice.parse::<i64>().unwrap())
            }

            ParseState::CellDecimalEnd
            | ParseState::CellDecimalEndWithPointRead
            | ParseState::CellQuoteDecimalEnd
            | ParseState::CellQuoteDecimalEndWithPointRead => {
                Cell::Decimal(slice.parse::<f64>().unwrap())
            }

            _ => Cell::String(slice.to_owned()),
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
        // let (size_of_each_slice, size_of_rem_slice) =
        //     (total_len / split, total_len % split);

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

    /// Parse content total lines from the file
    #[allow(unused_assignments)]
    fn parse_content(&mut self, offset_from_scanner: usize) -> Vector<Cell> {
        // Column data
        let mut column_data: Vector<Cell> = Vector::with_capacity(8);

        let mut x = self
            .byte_buffer
            .iter()
            .enumerate()
            .skip(offset_from_scanner + 1)
            .skip_while(|(_, c)| **c == b'\r' || **c == b'\n');

        let (next, _) = x.next().unwrap();
        let mut skip_new_line = 0;

        let (mut start, mut start_used, chunk_size) =
            (0, true, self.batch_size);

        self.byte_buffer[next..]
            .chunks(chunk_size)
            .enumerate()
            .for_each(|(chunk_index, buff)| {
                let curr = chunk_index * chunk_size + next;

                buff.iter().enumerate().for_each(|(new_idx, c)| {
                    let index = curr + new_idx;

                    self.state = Self::get_scan_state_from_data(self.state, *c);

                    match self.state {
                        // Scan start, get the current state based on the
                        // current byte and iterator takes care of
                        // rest accordingly
                        ParseState::Start
                        | ParseState::CellString
                        | ParseState::CellDecimalStartWithPointRead
                        | ParseState::CellNumberStart => {
                            start = index;
                            start_used = false;
                        }

                        ParseState::CellQuoteStart
                        | ParseState::CellQuoteNumberStart
                        | ParseState::CellQuoteDecimalStart
                        | ParseState::CellQuoteDecimalCurrentWithPointRead => {
                            start = index + 1;
                            start_used = false;
                        }

                        // Scan start of quoted header string,
                        // read till the end of quote.
                        ParseState::CellNumberEnd
                        | ParseState::CellDecimalEnd
                        | ParseState::CellDecimalEndWithPointRead
                        | ParseState::CellSep
                        | ParseState::NewLine => {
                            let push_value = unsafe {
                                if !start_used {
                                    let str_slice =
                                        core::str::from_utf8_unchecked(
                                            &self.byte_buffer[start..index],
                                        );

                                    Self::convert_from_slice(
                                        str_slice, self.state,
                                    )
                                } else {
                                    Cell::Null
                                }
                            };

                            if self.state != ParseState::NewLine {
                                start_used = true;
                                column_data.push(push_value);
                            } else {
                                skip_new_line += 1;
                            }
                        }

                        ParseState::CellQuoteEnd
                        | ParseState::CellQuoteNumberEnd
                        | ParseState::CellQuoteDecimalEnd
                        | ParseState::CellQuoteDecimalEndWithPointRead => {
                            let push_value = unsafe {
                                if !start_used {
                                    let str_slice =
                                        core::str::from_utf8_unchecked(
                                            &self.byte_buffer[start..index - 1],
                                        );

                                    Self::convert_from_slice(
                                        str_slice, self.state,
                                    )
                                } else {
                                    Cell::Null
                                }
                            };

                            if self.state != ParseState::NewLine {
                                start_used = true;
                                column_data.push(push_value);
                            } else {
                                skip_new_line += 1;
                            }
                        }
                        // Scan as it is
                        _ => {}
                    }
                });
            });

        column_data
    }

    /// Get total lines from the file
    #[allow(unused_assignments)]
    fn parse_content_on_buffer(
        &mut self,
        total_threads: usize,
        column_data: &mut [Cell],
    ) {
        // Column data
        let (mut start, mut start_used, chunk_size) =
            (0, true, self.batch_size);

        let (mut arr_index, mut skip_new_line) = (0, 0);

        self.byte_buffer.chunks(chunk_size).enumerate().for_each(
            |(idx, buff)| {
                let curr = idx * chunk_size;

                buff.iter().enumerate().for_each(|(new_idx, c)| {
                    // if skip_new_line == self.id {
                    let index = curr + new_idx;

                    self.state = Self::get_scan_state_from_data(self.state, *c);

                    match self.state {
                        // Scan start, get the current state based on the
                        // current byte and iterator takes care of
                        // rest accordingly
                        ParseState::Start
                        | ParseState::CellString
                        | ParseState::CellDecimalStartWithPointRead
                        | ParseState::CellNumberStart => {
                            start = index;
                            start_used = false;
                        }

                        ParseState::CellQuoteStart
                        | ParseState::CellQuoteNumberStart
                        | ParseState::CellQuoteDecimalStart
                        | ParseState::CellQuoteDecimalStartWithPointRead => {
                            start = index + 1;
                            start_used = false;
                        }

                        // Scan start of quoted header string,
                        // read till the end of quote.
                        ParseState::CellNumberEnd
                        | ParseState::CellDecimalEnd
                        | ParseState::CellSep
                        | ParseState::NewLine => {
                            let push_value = unsafe {
                                if !start_used {
                                    let str_slice =
                                        core::str::from_utf8_unchecked(
                                            &self.byte_buffer[start..index],
                                        );

                                    Self::convert_from_slice(
                                        str_slice, self.state,
                                    )
                                } else {
                                    Cell::Null
                                }
                            };

                            if self.state != ParseState::NewLine {
                                start_used = true;
                                if arr_index < column_data.len() {
                                    column_data[arr_index] = push_value;
                                }
                                arr_index += 1;
                            } else {
                                skip_new_line += 1;
                                skip_new_line %= total_threads;
                            }
                        }
                        // Scan start of quoted header string,
                        // read till the end of quote.
                        ParseState::CellQuoteEnd
                        | ParseState::CellQuoteNumberEnd
                        | ParseState::CellQuoteDecimalEnd
                        | ParseState::CellQuoteDecimalEndWithPointRead => {
                            let push_value = unsafe {
                                if !start_used {
                                    let str_slice =
                                        core::str::from_utf8_unchecked(
                                            &self.byte_buffer[start..index],
                                        );

                                    Self::convert_from_slice(
                                        str_slice, self.state,
                                    )
                                } else {
                                    Cell::Null
                                }
                            };

                            if self.state != ParseState::NewLine {
                                start_used = true;
                                if arr_index < column_data.len() {
                                    column_data[arr_index] = push_value;
                                }
                                arr_index += 1;
                            } else {
                                skip_new_line += 1;
                                skip_new_line %= total_threads;
                            }
                        }

                        // Scan as it is
                        _ => {}
                    }
                    // } else {
                    //     match *c {
                    //         b'\n' => {
                    //             skip_new_line += 1;
                    //             skip_new_line %= total_threads;
                    //             if skip_new_line == self.id {
                    //                 self.state = ParseState::Start;
                    //             }
                    //         }
                    //         _ => {}
                    //     }
                    // }
                });
            },
        );
    }

    /// Trim ascii having whitespaces.
    #[inline]
    fn trim_ascii<'b>(slice: &'b [u8]) -> &'b [u8] {
        let start = slice.iter().position(|c| !c.is_ascii_whitespace());
        let end = slice.iter().rev().position(|c| !c.is_ascii_whitespace());

        match (start, end) {
            (Some(st), Some(ed)) => &slice[st..slice.len() - ed],
            (None, Some(ed)) => &slice[..slice.len() - ed],
            (Some(st), None) => &slice[st..],
            (None, None) => &slice[..],
        }
    }

    /// Returns total lines with starting point and ending point
    /// of the buffer to be read.
    fn get_total_lines_in_a_file<'c>(
        mmaped_buffer: &'c [u8],
        scope: &'c std::thread::Scope<'c, '_>,
        thread_number: usize,
    ) -> Vec<(usize, usize, usize)> {
        let slots_division = mmaped_buffer.len() / thread_number;

        let mut end_prefix = match mmaped_buffer[slots_division..]
            .iter()
            .position(|c| *c == b'\n')
        {
            Some(index) => index,
            None => 0,
        } + slots_division;

        let mut slices: Vec<(&'c [u8], usize, usize)> =
            Vec::with_capacity(thread_number);

        slices.push((&mmaped_buffer[..end_prefix], 0, end_prefix));

        slices.extend((1..thread_number - 1).map(|multiplier| {
            let start_pos = end_prefix + 1;
            let spos = start_pos;

            let end_pos = (multiplier + 1) * slots_division;
            // Seek the start position to start from position next to \n
            let epos =
                match mmaped_buffer[end_pos..].iter().position(|c| *c == b'\n')
                {
                    Some(index) => index,
                    None => 0,
                } + end_pos;

            end_prefix = epos;
            (&mmaped_buffer[spos..epos], spos, epos)
        }));

        slices.push((
            &mmaped_buffer[end_prefix..],
            end_prefix,
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
            .into_iter()
            .map(|(c, st, ed)| (c.join().unwrap(), st, ed))
            .collect()
    }

    /// Parsing CSV file `file_name` using multiple threads
    ///
    /// Opens the file in memory mapped IO (read-only) and collects the data
    /// on the memory, to be used later via [`DataFrame`] struct
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
            Self::get_total_lines_in_a_file(
                &mmaped_slice[..],
                scope,
                total_threads,
            )
        });
        let c = length.iter().fold(0, |prev, curr| prev + curr.0) - 1;

        // unsafe { println!("{length:?}") }
        // // To-do: Make a convenient way to parse header as well as return apporpriate
        // // slice that is suitable for parsing and storing values.

        // // Initialized result with zero value.
        let mut result: Vector<Cell> = Vector::zeroed(c * scanned_header.len());

        // // // UNSAFE CALL: Creates multiple slices of vector `result` into smaller pieces,
        // // // since reallocating multiple vector or flattening is slower.
        let mut sliced_buffer = Self::split_slices(
            &mut result,
            &length,
            // total_threads,
            scanned_header.len(),
        );

        std::thread::scope(|scope| {
            // Trim whitespaces
            // To do: for each thread, start from offset just next to new line
            let mmaped2 = &mmaped_slice[..];

            sliced_buffer.iter_mut().zip(length).enumerate().for_each(
                |(_, (res, (len, start, end)))| {
                    // Each thread is alloted a specific `non-overlapping` region of the
                    // slice in `result`, which is ensured by function `split_slices`
                    // The values are recorded in res.
                    debug_assert_eq!(res.len(), len * scanned_header.len());
                    scope.spawn(move || {
                        CsvParser::new(&mmaped2[start..end])
                            .parse_content_on_buffer(total_threads, res);
                    });
                },
            );
        });

        DataFrame::new(result, scanned_header)
    }

    /// Parsing CSV file `file_name` using single thread
    ///
    /// Opens the file in memory mapped IO (read-only) and
    /// collects the data from file
    pub fn parse(file_name: &'a str) -> DataFrame {
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

        let sliced = Self::trim_ascii(&mmaped);
        let mut p = CsvParser::new(&sliced);
        p.skip_whitespace();

        // Scan header
        // This can be left optional
        let (scan_header, offset_from_scanner) = p.scan_header();
        p.state = ParseState::Start;

        let parsed_data =
            CsvParser::new(&sliced).parse_content(offset_from_scanner);

        DataFrame::new(parsed_data, scan_header)
    }
}