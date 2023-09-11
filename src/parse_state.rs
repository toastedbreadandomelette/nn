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

impl ParseState {
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

    #[inline(always)]
    const fn handle_quotes(initial_state: ParseState) -> ParseState {
        match initial_state {
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
        }
    }

    /// Evaluate next state `ParseState` given the `initial_state`
    /// and the `byte`.
    ///
    /// ## To Do
    /// - Handle for generic separator
    /// - Maybe move from byte to char or byte sequence
    #[inline]
    pub const fn get_scan_state_from_data(
        initial_state: ParseState,
        c: u8,
    ) -> ParseState {
        match c {
            // If quote is started, end it else start the quote
            b'"' => Self::handle_quotes(initial_state),

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
}
