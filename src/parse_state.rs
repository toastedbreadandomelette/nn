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

    CarriageRet,
    /// Read separator
    CellSep,
    /// Skip character
    SkipChar,
    /// Skip whitespace within cell
    SkippedStartWhitespace,
    /// Skip whitespace but assume read might be the end of cell
    /// for certain said state that is recorded.
    SkippedAssumeEndWhitespace(PrevState),
    /// Reading new line character
    NewLine,
    /// File end
    EndFile,
}

#[allow(unused)]
#[derive(Copy, Clone, Debug, PartialEq, PartialOrd)]
pub enum PrevState {
    /// Start of parsing section
    Start,

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
    /// Reading new line character
    NewLine,
    /// File end
    EndFile,
}

impl PrevState {
    pub fn get_end_of_parse_state(initial_state: Self) -> ParseState {
        match initial_state {
            Self::CellString | Self::CellCurrent => ParseState::CellSep,

            Self::CellDecimalStart
            | Self::CellDecimalCurrent
            | Self::CellDecimalEnd => ParseState::CellDecimalEnd,

            Self::CellDecimalCurrentWithPointRead
            | Self::CellDecimalEndWithPointRead
            | Self::CellDecimalStartWithPointRead => {
                ParseState::CellDecimalEndWithPointRead
            }

            Self::CellNumberStart
            | Self::CellNumberCurrent
            | Self::CellNumberEnd => ParseState::CellNumberEnd,

            Self::CellQuoteStart
            | Self::CellQuoteCurrent
            | Self::CellQuoteEnd => ParseState::CellQuoteEnd,

            Self::CellQuoteDecimalStart
            | Self::CellQuoteDecimalCurrent
            | Self::CellQuoteDecimalEnd => ParseState::CellQuoteDecimalEnd,

            Self::CellQuoteDecimalStartWithPointRead
            | Self::CellQuoteDecimalCurrentWithPointRead
            | Self::CellQuoteDecimalEndWithPointRead => {
                ParseState::CellQuoteDecimalEndWithPointRead
            }

            Self::CellQuoteNumberStart
            | Self::CellQuoteNumberCurrent
            | Self::CellQuoteNumberEnd => ParseState::CellQuoteNumberEnd,

            Self::CellSep => ParseState::CellSep,
            Self::EndFile => ParseState::EndFile,
            Self::NewLine => ParseState::NewLine,
            Self::SkipChar => ParseState::SkipChar,
            Self::Start => ParseState::Start
        }
    }
}

impl ParseState {
    /// Handle transition to states when current state is
    /// reading a decimal value
    #[inline(always)]
    fn handle_decimal_state(initial_state: Self) -> Self {
        match initial_state {
            Self::CellNumberCurrent | Self::CellNumberStart => {
                Self::CellDecimalCurrentWithPointRead
            }

            Self::CellQuoteStart => Self::CellQuoteDecimalStartWithPointRead,

            Self::CellQuoteCurrent => Self::CellQuoteCurrent,

            // While reading potential number, switch to
            // potential quoted decimal number
            Self::CellQuoteNumberStart | Self::CellQuoteNumberCurrent => {
                Self::CellQuoteDecimalCurrentWithPointRead
            }

            // If quoted decimal is already acknowledged, then it's not
            // a decimal value, but a quoted string
            Self::CellQuoteDecimalStartWithPointRead
            | Self::CellQuoteDecimalCurrentWithPointRead => {
                Self::CellQuoteCurrent
            }

            // If decimal is already acknowledged, then it's not
            // a decimal value, but a string
            Self::CellString
            | Self::CellDecimalStartWithPointRead
            | Self::CellDecimalCurrentWithPointRead
            | Self::CellCurrent => Self::CellCurrent,

            Self::SkippedAssumeEndWhitespace(_) => Self::CellCurrent,

            _ => Self::CellDecimalStartWithPointRead,
        }
    }

    /// Handle transition to states when current state is
    /// reading certain value
    #[inline(always)]
    fn handle_default(initial_state: Self) -> Self {
        match initial_state {
            // If quoted, continue reading.
            Self::CellQuoteStart | Self::CellQuoteCurrent => {
                Self::CellQuoteCurrent
            }

            // Does matter iff any special number, switch to
            // normal non-quoted string.
            Self::CellString
            | Self::CellCurrent
            | Self::CellNumberStart
            | Self::CellDecimalStart
            | Self::CellNumberCurrent
            | Self::CellDecimalCurrent
            | Self::CellDecimalStartWithPointRead
            | Self::CellDecimalCurrentWithPointRead
            | Self::CellQuoteDecimalStartWithPointRead
            | Self::CellQuoteDecimalCurrentWithPointRead => Self::CellCurrent,

            Self::SkippedAssumeEndWhitespace(_) => Self::CellCurrent,

            _ => Self::CellString,
        }
    }

    /// Handle transition to states when current state is
    /// reading separator
    #[inline(always)]
    fn handle_separator(initial_state: Self) -> Self {
        match initial_state {
            // Any quoted values defaults to quoted string, knowing
            // that separator is read.
            Self::CellQuoteCurrent
            | Self::CellQuoteStart
            | Self::CellQuoteDecimalCurrent
            | Self::CellQuoteDecimalStart
            | Self::CellQuoteDecimalStartWithPointRead
            | Self::CellQuoteDecimalCurrentWithPointRead => {
                Self::CellQuoteCurrent
            }

            Self::CellNumberCurrent | Self::CellNumberStart => {
                Self::CellNumberEnd
            }

            Self::CellDecimalCurrent
            | Self::CellDecimalStart
            | Self::CellDecimalStartWithPointRead
            | Self::CellDecimalCurrentWithPointRead => Self::CellDecimalEnd,

            Self::SkippedAssumeEndWhitespace(v) => {
                PrevState::get_end_of_parse_state(v)
            }

            _ => Self::CellSep,
        }
    }

    #[inline(always)]
    fn handle_number(initial_state: Self) -> Self {
        match initial_state {
            Self::CellQuoteDecimalStartWithPointRead
            | Self::CellQuoteDecimalCurrentWithPointRead => {
                Self::CellQuoteDecimalCurrentWithPointRead
            }

            Self::CellQuoteStart
            | Self::CellQuoteNumberStart
            | Self::CellQuoteNumberCurrent => Self::CellQuoteNumberCurrent,

            Self::CellQuoteCurrent => Self::CellQuoteCurrent,

            Self::CellNumberCurrent | Self::CellNumberStart => {
                Self::CellNumberCurrent
            }

            Self::CellDecimalStartWithPointRead
            | Self::CellDecimalCurrentWithPointRead => {
                Self::CellDecimalCurrentWithPointRead
            }

            Self::CellDecimalCurrent | Self::CellDecimalStart => {
                Self::CellDecimalCurrent
            }

            Self::CellString | Self::CellCurrent => Self::CellCurrent,

            Self::SkippedAssumeEndWhitespace(_) => Self::CellCurrent,

            _ => Self::CellNumberStart,
        }
    }

    /// Evaluate next state `Self` given the `initial_state`
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
    fn handle_lf(initial_state: Self, c: u8) -> Self {
        match initial_state {
            // Starting with quote and running into new-line characters
            // should default to normal quoted string.
            Self::CellQuoteCurrent
            | Self::CellQuoteStart
            | Self::CellQuoteDecimalCurrent
            | Self::CellQuoteDecimalStart
            | Self::CellQuoteDecimalStartWithPointRead
            | Self::CellQuoteDecimalCurrentWithPointRead => {
                Self::CellQuoteCurrent
            }

            Self::CellNumberCurrent | Self::CellNumberStart => {
                Self::CellNumberEnd
            }

            Self::CellDecimalCurrent
            | Self::CellDecimalStart
            | Self::CellDecimalStartWithPointRead
            | Self::CellDecimalCurrentWithPointRead => Self::CellDecimalEnd,

            Self::SkippedAssumeEndWhitespace(v) => {
                PrevState::get_end_of_parse_state(v)
            }

            _ => Self::NewLine
        }
    }

    #[inline(always)]
    fn handle_quotes(initial_state: Self) -> Self {
        match initial_state {
            // If previous started or running, end the values
            Self::CellQuoteStart | Self::CellQuoteCurrent => Self::CellQuoteEnd,
            Self::CellQuoteDecimalStart | Self::CellQuoteDecimalCurrent => {
                Self::CellQuoteDecimalEnd
            }
            Self::CellQuoteDecimalStartWithPointRead
            | Self::CellQuoteDecimalCurrentWithPointRead => {
                Self::CellQuoteDecimalEndWithPointRead
            }

            Self::SkippedAssumeEndWhitespace(v) => {
                PrevState::get_end_of_parse_state(v)
            }
            _ => Self::CellQuoteStart,
        }
    }

    #[inline(always)]
    fn handle_cr(initial_state: Self) -> Self {
        match initial_state {
            // Starting with quote and running into new-line characters
            // should default to normal quoted string.
            Self::CellQuoteCurrent
            | Self::CellQuoteStart
            | Self::CellQuoteDecimalCurrent
            | Self::CellQuoteDecimalStart
            | Self::CellQuoteDecimalStartWithPointRead
            | Self::CellQuoteDecimalCurrentWithPointRead => {
                Self::CellQuoteCurrent
            }

            Self::CellNumberCurrent | Self::CellNumberStart => {
                Self::CarriageRet
            }

            Self::CellDecimalCurrent
            | Self::CellDecimalStart => { 
                Self::SkippedAssumeEndWhitespace(PrevState::CellDecimalCurrent) 
            }

            Self::CellDecimalStartWithPointRead
            | Self::CellDecimalCurrentWithPointRead => {
                Self::SkippedAssumeEndWhitespace(PrevState::CellDecimalCurrentWithPointRead) 
            },

            Self::SkippedAssumeEndWhitespace(v) => {
                Self::SkippedAssumeEndWhitespace(v)
            }

            _ => Self::CarriageRet
        }
    }

    #[inline(always)]
    fn handle_white_space(initial_state: Self) -> Self {
        match initial_state {
            // Immediately after cell separator, assume
            // that cell has unwanted whitespace
            Self::CellSep | Self::SkippedStartWhitespace => {
                Self::SkippedStartWhitespace
            }

            // Quoted does not change their state unless numbered
            // If whitespace is found, assume that it is string.
            // just use state of current
            Self::CellQuoteStart
            | Self::CellQuoteCurrent
            | Self::CellQuoteNumberStart
            | Self::CellQuoteDecimalStart
            | Self::CellQuoteNumberCurrent
            | Self::CellQuoteDecimalCurrent
            | Self::CellQuoteDecimalStartWithPointRead
            | Self::CellQuoteDecimalCurrentWithPointRead => {
                Self::CellQuoteCurrent
            }

            Self::CellCurrent
            | Self::CellDecimalStartWithPointRead
            | Self::CellDecimalCurrentWithPointRead => {
                Self::CellCurrent
            }

            // If number read, assume that it is the end
            Self::CellNumberStart | Self::CellNumberCurrent => {
                Self::SkippedAssumeEndWhitespace(PrevState::CellNumberCurrent)
            }

            // If number read, assume that it is the end
            Self::CellDecimalStart | Self::CellDecimalCurrent => {
                Self::SkippedAssumeEndWhitespace(PrevState::CellDecimalCurrent)
            }

            Self::SkippedAssumeEndWhitespace(v) => {
                Self::SkippedAssumeEndWhitespace(v)
            }

            _ => Self::SkippedStartWhitespace,
        }
    }

    /// Evaluate next state `Self` given the `initial_state`
    /// and the `byte`.
    ///
    /// ## To Do
    /// - Handle for generic separator
    /// - Maybe move from byte to char or byte sequence
    #[inline]
    pub fn get_scan_state_from_data(initial_state: Self, c: u8) -> Self {
        match c {
            // If quote is started, end it else start the quote
            b'"' => Self::handle_quotes(initial_state),

            // Handle when a single point is read by the parser
            b'.' => Self::handle_decimal_state(initial_state),

            // Handle when a single point is read by the parser
            b'0'..=b'9' => Self::handle_number(initial_state),

            // To-do Handle generic separator
            b',' => Self::handle_separator(initial_state),

            b'\n' => Self::handle_lf(initial_state, c),
            b'\r' => Self::handle_cr(initial_state),
            // b' ' => Self::SkippedStartWhitespace,
            b' ' => Self::handle_white_space(initial_state),
            _ => Self::handle_default(initial_state),
        }
    }
}
