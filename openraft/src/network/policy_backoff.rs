//! The backoff a [`Config::backoff`] policy describes, read from the policy where the config holds
//! it, as [`Config::build_backoff`] builds it: unlike the policy's parser, which parses the policy
//! into new strings and vectors, it allocates nothing.
//!
//! It yields what `backoff_series::BackoffSeries::parse(policy)?.build()` yields: the durations
//! of the series, then those its extrapolation gives, each capped at the policy's max.

use std::sync::Arc;
use std::time::Duration;

use crate::Config;

/// The cap of a policy that gives none, as the policy's parser defaults it.
const DEFAULT_MAX: Duration = Duration::from_millis(1000);

/// The iterator of a policy's backoff durations.
#[derive(Clone, Debug)]
pub(crate) struct PolicyBackoff {
    /// The config that holds the policy, shared, not copied.
    config: Arc<Config>,
    /// The byte offset in the policy of the series durations not yet yielded.
    at: usize,
    /// How many series durations are left to yield.
    series_left: usize,
    /// The index of the next duration the extrapolation gives.
    next_x: usize,
    extrapolation: Extrapolation,
    max_ms: f64,
}

impl PolicyBackoff {
    /// The backoff `config`'s policy describes: `None` if the policy is invalid, as
    /// [`Config::validate`] rejects it.
    pub(crate) fn new(config: &Arc<Config>) -> Option<Self> {
        let policy = config.backoff.as_str();

        let mut series = 0;
        // The last three series durations, the latest last, in milliseconds.
        let mut last = [0.0; 3];
        let mut max = None;
        let mut tokens = Tokens::new(policy);
        while let Some(token) = tokens.next() {
            match token {
                Token::Separator => {
                    max = Some(Self::max(&mut tokens)?);
                    break;
                }
                Token::Text(text) => {
                    let duration = parse_duration(text)?;
                    last = [last[1], last[2], duration_to_ms(duration)];
                    series += 1;
                }
            }
        }

        Some(Self {
            config: config.clone(),
            at: 0,
            series_left: series,
            next_x: series,
            extrapolation: Extrapolation::fit(series, last),
            max_ms: duration_to_ms(max.unwrap_or(DEFAULT_MAX)),
        })
    }

    /// The one max duration after the separator, and nothing after it.
    fn max(tokens: &mut Tokens<'_>) -> Option<Duration> {
        let Some(Token::Text(max)) = tokens.next() else {
            return None;
        };
        if tokens.next().is_some() {
            return None;
        }
        parse_duration(max)
    }
}

impl Iterator for PolicyBackoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        if self.series_left > 0 {
            let policy = self.config.backoff.as_str();
            let mut tokens = Tokens::new(policy.get(self.at..)?);
            let Some(Token::Text(text)) = tokens.next() else {
                return None;
            };
            self.at = policy.len() - tokens.rest.len();
            self.series_left -= 1;
            return parse_duration(text);
        }

        let raw = self.extrapolation.eval(self.next_x);
        self.next_x += 1;
        Some(ms_to_duration(raw.clamp(0.0, self.max_ms)))
    }
}

/// A token of a policy, as the policy's parser splits it: it reads `...` and `..` as the
/// separator before the max, `,` as whitespace, and splits at whitespace.
#[derive(Debug, PartialEq, Eq)]
enum Token<'a> {
    Separator,
    Text(&'a str),
}

/// The tokens of a policy, read in place.
struct Tokens<'a> {
    rest: &'a str,
    /// Separators read from a run of dots, not yet yielded.
    separators: usize,
}

impl<'a> Tokens<'a> {
    fn new(policy: &'a str) -> Self {
        Self {
            rest: policy,
            separators: 0,
        }
    }
}

impl<'a> Iterator for Tokens<'a> {
    type Item = Token<'a>;

    fn next(&mut self) -> Option<Token<'a>> {
        if self.separators > 0 {
            self.separators -= 1;
            return Some(Token::Separator);
        }

        self.rest = self.rest.trim_start_matches(|c: char| c.is_whitespace() || c == ',');
        if self.rest.is_empty() {
            return None;
        }

        // The parser rewrites each `...` of a run of dots, then each `..` left, as a separator
        // between spaces: a run of 3q+2 dots is q+1 separators, of 3q dots q, and of 3q+1 dots
        // q, its last dot left to start the next token.
        let dots = self.rest.bytes().take_while(|b| *b == b'.').count();
        if dots >= 2 {
            let (separators, left) = match dots % 3 {
                0 => (dots / 3, 0),
                1 => (dots / 3, 1),
                _ => (dots / 3 + 1, 0),
            };
            self.rest = &self.rest[dots - left..];
            self.separators = separators - 1;
            return Some(Token::Separator);
        }

        // A token ends at whitespace, a `,`, or the `..` that starts a separator.
        let end = self
            .rest
            .char_indices()
            .find(|&(i, c)| c.is_whitespace() || c == ',' || (i > 0 && self.rest[i..].starts_with("..")))
            .map_or(self.rest.len(), |(i, _)| i);
        let (text, rest) = self.rest.split_at(end);
        self.rest = rest;
        Some(Token::Text(text))
    }
}

/// A duration such as `200ms` or `1.5s`, as the policy's parser reads it: `None` if it is not one.
fn parse_duration(s: &str) -> Option<Duration> {
    let split = s.find(|c: char| !c.is_ascii_digit() && c != '.')?;
    let (number, unit) = s.split_at(split);
    let value: f64 = number.parse().ok()?;
    if !value.is_finite() || value < 0.0 {
        return None;
    }

    let nanos_per_unit: f64 = match unit {
        "ns" => 1.0,
        "us" | "µs" => 1_000.0,
        "ms" => 1_000_000.0,
        "s" => 1_000_000_000.0,
        "m" => 60.0 * 1_000_000_000.0,
        "h" => 3_600.0 * 1_000_000_000.0,
        _ => return None,
    };

    let nanos = (value * nanos_per_unit).min(u64::MAX as f64) as u64;
    Some(Duration::from_nanos(nanos))
}

fn duration_to_ms(d: Duration) -> f64 {
    d.as_nanos() as f64 / 1_000_000.0
}

fn ms_to_duration(ms: f64) -> Duration {
    if !ms.is_finite() || ms <= 0.0 {
        return Duration::ZERO;
    }
    let nanos = (ms * 1_000_000.0).min(u64::MAX as f64) as u64;
    Duration::from_nanos(nanos)
}

/// The closed form that continues the series once it is exhausted, as the policy's parser fits
/// it to the series' last durations.
#[derive(Clone, Copy, Debug)]
enum Extrapolation {
    /// `k * a^x + t`
    Exponential { k: f64, a: f64, t: f64 },
    /// `k * x + t`
    Linear { k: f64, t: f64 },
}

impl Extrapolation {
    /// Fit to a series of `len` durations, `last` its last three in milliseconds, the latest
    /// last: those before the first of a shorter series are not read.
    fn fit(len: usize, last: [f64; 3]) -> Self {
        match len {
            0 => Self::Exponential { k: 1.0, a: 2.0, t: 0.0 },
            1 => Self::Exponential {
                k: last[2],
                a: 2.0,
                t: 0.0,
            },
            2 => {
                let (y0, y1) = (last[1], last[2]);
                if y0 <= 0.0 {
                    return Self::Linear { k: y1 - y0, t: y0 };
                }
                Self::Exponential {
                    k: y0,
                    a: y1 / y0,
                    t: 0.0,
                }
            }
            n => {
                let [y0, y1, y2] = last;
                let d1 = y1 - y0;
                let d2 = y2 - y1;

                if d1 != 0.0 {
                    let a = d2 / d1;
                    if a.is_finite() && a > 0.0 && a != 1.0 {
                        let a_x0 = a.powi((n - 3) as i32);
                        let k = d1 / (a_x0 * (a - 1.0));
                        let t = y0 - k * a_x0;
                        return Self::Exponential { k, a, t };
                    }
                }

                let k = d2;
                let t = y2 - k * (n - 1) as f64;
                Self::Linear { k, t }
            }
        }
    }

    fn eval(&self, x: usize) -> f64 {
        match *self {
            Self::Exponential { k, a, t } => k * a.powi(x as i32) + t,
            Self::Linear { k, t } => k * x as f64 + t,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use backoff_series::BackoffSeries;

    use super::PolicyBackoff;
    use crate::Config;
    use crate::metered::metered;

    fn config(policy: &str) -> Arc<Config> {
        Arc::new(Config {
            backoff: policy.to_string(),
            ..Default::default()
        })
    }

    /// It yields what the policy's parser builds, for every form of policy it parses, and is
    /// refused every policy the parser rejects.
    #[test]
    fn test_yields_what_the_parser_builds() {
        let policies = [
            "",
            "200ms",
            "100ms 200ms",
            "0ms 50ms",
            "100ms, 200ms, 400ms",
            "100ms 200ms 400ms .. 3s",
            "100ms 200ms 400ms...3s",
            "10ms 20ms 30ms 40ms ..500ms",
            "5ms 5ms 5ms",
            "1.5s 3s .. 1m",
            "1ns 2us 3µs 4ms",
            "0.5h..2h",
            "....5s",
            "1s2s",
            "50ms  ,  100ms,,300ms . . 1s",
            "100ms 200ms ..",
            "100ms .. 1s 2s",
            "100ms .. 1s .. 2s",
            "....",
            "1x",
            ".",
            "ms",
            "-1s",
        ];

        for policy in policies {
            let want = BackoffSeries::parse(policy).ok().map(|series| series.build().take(40).collect::<Vec<_>>());
            let got = PolicyBackoff::new(&config(policy)).map(|backoff| backoff.take(40).collect::<Vec<_>>());
            assert_eq!(want, got, "policy {policy:?}");
        }
    }

    /// Made and run, it allocates nothing.
    #[test]
    fn test_allocates_nothing() {
        let config = config("100ms 200ms 400ms .. 3s");

        let (sum, meter) = metered(|| {
            let backoff = PolicyBackoff::new(&config).unwrap();
            backoff.take(40).sum::<std::time::Duration>()
        });

        assert!(sum > std::time::Duration::ZERO);
        assert_eq!(0, meter.allocated, "a policy's backoff allocates nothing");
    }

    /// `Config::build_backoff` reads the policy where the config holds it, as it yields what the
    /// parser builds: made and run, it allocates nothing.
    #[test]
    fn test_build_backoff_allocates_nothing() {
        let config = config("100ms 200ms 400ms .. 3s");
        let want = BackoffSeries::parse(&config.backoff).unwrap().build().take(40).collect::<Vec<_>>();

        let (sum, meter) = metered(|| config.build_backoff().take(40).sum::<std::time::Duration>());

        assert_eq!(want.into_iter().sum::<std::time::Duration>(), sum);
        assert_eq!(0, meter.allocated, "the config's backoff allocates nothing");
    }
}
