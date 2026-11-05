//! Exact conversion from exchange decimal strings to the output's fixed point.
//!
//! The output schema stores prices and quantities as `i64` with six implied
//! decimals (1_000_000 = $1, or one contract). Exchanges send decimal strings
//! such as `"0.5500"` or `"-54.00"`. Parsing digits directly keeps every value
//! exact; going through `f64` would not.

use std::fmt;

/// One whole unit (one dollar, or one contract) in fixed point.
pub const SCALE: i64 = 1_000_000;
const DECIMALS: usize = 6;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseFixedError(pub String);

impl fmt::Display for ParseFixedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "not an exact 6-decimal fixed-point number: {:?}", self.0)
    }
}

impl std::error::Error for ParseFixedError {}

/// Parses `"-12.3400"` into `-12_340_000`.
///
/// Digits past the sixth decimal must be zero; anything else is not
/// representable exactly and is rejected rather than rounded.
pub fn parse(s: &str) -> Result<i64, ParseFixedError> {
    let err = || ParseFixedError(s.to_owned());
    let (negative, digits) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        Some(_) => (false, s),
        None => return Err(err()),
    };
    let (int_part, frac_part) = digits.split_once('.').unwrap_or((digits, ""));
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(err());
    }
    if !int_part
        .bytes()
        .chain(frac_part.bytes())
        .all(|b| b.is_ascii_digit())
    {
        return Err(err());
    }
    let (frac_kept, frac_rest) = frac_part.split_at(frac_part.len().min(DECIMALS));
    if frac_rest.bytes().any(|b| b != b'0') {
        return Err(err());
    }

    let mut whole: i64 = 0;
    for b in int_part.bytes() {
        whole = whole
            .checked_mul(10)
            .and_then(|v| v.checked_add(i64::from(b - b'0')))
            .ok_or_else(err)?;
    }
    let mut frac: i64 = 0;
    for b in frac_kept.bytes() {
        frac = frac * 10 + i64::from(b - b'0');
    }
    frac *= 10_i64.pow((DECIMALS - frac_kept.len()) as u32);

    let value = whole
        .checked_mul(SCALE)
        .and_then(|v| v.checked_add(frac))
        .ok_or_else(err)?;
    Ok(if negative { -value } else { value })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_kalshi_prices_and_quantities_exactly() {
        assert_eq!(parse("0.5500"), Ok(550_000));
        assert_eq!(parse("0.0001"), Ok(100));
        assert_eq!(parse("0.9999"), Ok(999_900));
        assert_eq!(parse("300.00"), Ok(300_000_000));
        assert_eq!(parse("-54.00"), Ok(-54_000_000));
        assert_eq!(parse("0.01"), Ok(10_000));
        assert_eq!(parse("1"), Ok(SCALE));
        assert_eq!(parse(".5"), Ok(500_000));
        assert_eq!(parse("+2.5"), Ok(2_500_000));
        assert_eq!(parse("0.12345600"), Ok(123_456));
    }

    #[test]
    fn rejects_values_it_cannot_hold_exactly() {
        for bad in [
            "",
            "-",
            ".",
            "abc",
            "1.2.3",
            "0.1234567",
            "1e3",
            " 1",
            "9223372036854.775808",
        ] {
            assert!(parse(bad).is_err(), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn agrees_with_integer_arithmetic_on_every_kalshi_tick() {
        // Every 4-decimal price Kalshi can quote, without touching floats.
        for ticks in 0..=10_000_i64 {
            let text = format!("{}.{:04}", ticks / 10_000, ticks % 10_000);
            assert_eq!(parse(&text), Ok(ticks * 100), "{text}");
        }
    }
}
