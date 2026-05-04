use pgrust_nodes::datum::Value;
use pgrust_nodes::parsenodes::ParseError;
use pgrust_nodes::primnodes::ScalarType;

pub type ExprResult<T> = Result<T, ExprError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegexError {
    pub sqlstate: &'static str,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub context: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ExprError {
    WithContext {
        source: Box<ExprError>,
        context: String,
    },
    Parse(ParseError),
    TypeMismatch {
        op: &'static str,
        left: Value,
        right: Value,
    },
    NonBoolQual(Value),
    UnsupportedStorageType {
        column: String,
        ty: ScalarType,
        attlen: i16,
        actual_len: Option<usize>,
    },
    InvalidStorageValue {
        column: String,
        details: String,
    },
    JsonInput {
        raw_input: String,
        message: String,
        detail: Option<String>,
        context: Option<String>,
        sqlstate: &'static str,
    },
    XmlInput {
        raw_input: String,
        message: String,
        detail: Option<String>,
        context: Option<String>,
        sqlstate: &'static str,
    },
    ArrayInput {
        message: String,
        value: String,
        detail: Option<String>,
        sqlstate: &'static str,
    },
    DetailedError {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
    DiagnosticError {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
        column_name: Option<String>,
        constraint_name: Option<String>,
        datatype_name: Option<String>,
        table_name: Option<String>,
        schema_name: Option<String>,
    },
    StringDataRightTruncation {
        ty: String,
    },
    MissingRequiredColumn(String),
    Regex(RegexError),
    InvalidRegex(String),
    RaiseException(String),
    DivisionByZero(&'static str),
    InvalidIntegerInput {
        ty: &'static str,
        value: String,
    },
    IntegerOutOfRange {
        ty: &'static str,
        value: String,
    },
    InvalidNumericInput(String),
    InvalidByteaInput {
        value: String,
    },
    InvalidUuidInput {
        value: String,
    },
    InvalidByteaHexDigit {
        value: String,
        digit: String,
    },
    InvalidByteaHexOddDigits {
        value: String,
    },
    InvalidGeometryInput {
        ty: &'static str,
        value: String,
    },
    InvalidRangeInput {
        ty: &'static str,
        value: String,
    },
    InvalidBitInput {
        digit: char,
        is_hex: bool,
    },
    BitStringLengthMismatch {
        actual: i32,
        expected: i32,
    },
    BitStringTooLong {
        actual: i32,
        limit: i32,
    },
    BitStringSizeMismatch {
        op: &'static str,
    },
    BitIndexOutOfRange {
        index: i32,
        max_index: i32,
    },
    NegativeSubstringLength,
    InvalidBooleanInput {
        value: String,
    },
    InvalidFloatInput {
        ty: &'static str,
        value: String,
    },
    FloatOutOfRange {
        ty: &'static str,
        value: String,
    },
    FloatOverflow,
    FloatUnderflow,
    NumericNaNToInt {
        ty: &'static str,
    },
    NumericInfinityToInt {
        ty: &'static str,
    },
    Int2OutOfRange,
    Int4OutOfRange,
    Int8OutOfRange,
    OidOutOfRange,
    NumericFieldOverflow,
    RequestedLengthTooLarge,
}

impl From<ParseError> for ExprError {
    fn from(value: ParseError) -> Self {
        Self::Parse(value)
    }
}
