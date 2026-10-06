//! The version gate (SPEC §24.3): PostgreSQL ≥ 12 (the `MATERIALIZED` CTE RESERVE needs), MySQL ≥
//! 8.0.1 and MariaDB ≥ 10.6 (`SKIP LOCKED`). SQLite is not a v1 store family (§24.15).
//!
//! The gate reads the RAW `version()` string the pool's existing probe learned (`ferrod`'s
//! `PoolRegistry` server-version cache, §22.2 (u)) — the same string `HELLO_ACK` advertises, never
//! normalised there, so the parsing is done here:
//!
//! - PostgreSQL: `PostgreSQL 16.13 (Ubuntu …) on …` → major `16`. A development build such as
//!   `PostgreSQL 18beta1` parses its leading digits (`18`).
//! - MySQL family: `8.4.11`, `8.0.36-0ubuntu0.22.04.1`, `11.8.2-MariaDB-ubu2404`,
//!   `10.11.6-MariaDB-0+deb12u1`. MariaDB is detected by the substring `mariadb` (case-insensitive) —
//!   the rule the Doctrine tier uses, and the only one available, since MySQL and MariaDB share the
//!   pool kind. A replication-compatibility prefix `5.5.5-` before a MariaDB version is skipped.
//!
//! **Fail closed:** a version string that does not parse is refused, never assumed new enough.

use crate::PoolFamily;

/// The server family the gate identified, which the store's statements are composed for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerFamily {
    Postgres,
    Mysql,
    MariaDb,
}

/// Why the gate refused. Its `Display` names the minimum and what was found; the version string is a
/// server banner, not a secret, so quoting it is log-safe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateRefusal {
    /// SQLite pools cannot hold a v1 store.
    UnsupportedFamily,
    /// Parsed, and below the minimum.
    TooOld {
        family: ServerFamily,
        found: String,
        minimum: &'static str,
    },
    /// The string did not parse, so the gate cannot pass.
    Unparseable { found: String },
}

impl std::fmt::Display for GateRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GateRefusal::UnsupportedFamily => {
                f.write_str("SQLite pools cannot hold a Ferro Queue store in v1 (SPEC §24.15)")
            }
            GateRefusal::TooOld {
                family,
                found,
                minimum,
            } => write!(
                f,
                "Ferro Queue needs {family:?} {minimum} or newer; this server reports {found:?}"
            ),
            GateRefusal::Unparseable { found } => write!(
                f,
                "Ferro Queue cannot verify the server version {found:?}, so its version gate refuses"
            ),
        }
    }
}

/// `(major, minor, patch)` read from the leading `d+(.d+(.d+)?)?` of `s`; a missing part is 0.
fn leading_version(s: &str) -> Option<(u32, u32, u32)> {
    let mut nums = [0u32; 3];
    let mut it = s.split('.');
    for (i, slot) in nums.iter_mut().enumerate() {
        let Some(part) = it.next() else { break };
        let digits: String = part.chars().take_while(char::is_ascii_digit).collect();
        if digits.is_empty() {
            if i == 0 {
                return None;
            }
            break;
        }
        *slot = digits.parse().ok()?;
        if digits.len() != part.len() {
            break; // the part continued with a non-digit (`36-0ubuntu`, `18beta1`): stop here
        }
    }
    Some((nums[0], nums[1], nums[2]))
}

/// Gate one pool's raw `version()` string.
pub fn gate(family: PoolFamily, version: &str) -> Result<ServerFamily, GateRefusal> {
    let unparseable = || GateRefusal::Unparseable {
        found: version.to_string(),
    };
    match family {
        PoolFamily::Sqlite => Err(GateRefusal::UnsupportedFamily),
        PoolFamily::Postgres => {
            let rest = version
                .strip_prefix("PostgreSQL ")
                .ok_or_else(unparseable)?;
            let (major, _, _) = leading_version(rest).ok_or_else(unparseable)?;
            if major >= 12 {
                Ok(ServerFamily::Postgres)
            } else {
                Err(GateRefusal::TooOld {
                    family: ServerFamily::Postgres,
                    found: version.to_string(),
                    minimum: "12",
                })
            }
        }
        PoolFamily::Mysql => {
            let mariadb = version.to_ascii_lowercase().contains("mariadb");
            let text = if mariadb {
                version.strip_prefix("5.5.5-").unwrap_or(version)
            } else {
                version
            };
            let v = leading_version(text).ok_or_else(unparseable)?;
            let (fam, min, minimum) = if mariadb {
                (ServerFamily::MariaDb, (10, 6, 0), "10.6")
            } else {
                (ServerFamily::Mysql, (8, 0, 1), "8.0.1")
            };
            if v >= min {
                Ok(fam)
            } else {
                Err(GateRefusal::TooOld {
                    family: fam,
                    found: version.to_string(),
                    minimum,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(f: PoolFamily, v: &str) -> ServerFamily {
        gate(f, v).unwrap_or_else(|e| panic!("{v}: {e}"))
    }

    #[test]
    fn postgres_12_and_newer_pass_and_11_does_not() {
        for v in [
            "PostgreSQL 12.0 on x86_64-pc-linux-gnu",
            "PostgreSQL 16.13 (Ubuntu 16.13-0ubuntu0.24.04.1) on x86_64-pc-linux-gnu, compiled by gcc",
            "PostgreSQL 17.10",
            "PostgreSQL 18beta1 on aarch64",
            "PostgreSQL 100.1",
        ] {
            assert_eq!(ok(PoolFamily::Postgres, v), ServerFamily::Postgres);
        }
        for v in [
            "PostgreSQL 11.22 on x86_64",
            "PostgreSQL 9.6.24",
            "PostgreSQL 1",
        ] {
            assert!(
                matches!(
                    gate(PoolFamily::Postgres, v),
                    Err(GateRefusal::TooOld { minimum: "12", .. })
                ),
                "{v}"
            );
        }
    }

    #[test]
    fn mysql_needs_8_0_1_and_mariadb_10_6() {
        for v in ["8.0.1", "8.0.36-0ubuntu0.22.04.1", "8.4.11", "9.1.0"] {
            assert_eq!(ok(PoolFamily::Mysql, v), ServerFamily::Mysql, "{v}");
        }
        for v in ["8.0.0", "5.7.44-log", "8.0"] {
            assert!(
                matches!(
                    gate(PoolFamily::Mysql, v),
                    Err(GateRefusal::TooOld {
                        family: ServerFamily::Mysql,
                        ..
                    })
                ),
                "{v}"
            );
        }
        for v in [
            "10.6.0-MariaDB",
            "10.11.6-MariaDB-0+deb12u1",
            "11.8.2-MariaDB-ubu2404",
            "5.5.5-10.11.6-MariaDB-log",
        ] {
            assert_eq!(ok(PoolFamily::Mysql, v), ServerFamily::MariaDb, "{v}");
        }
        for v in ["10.5.23-MariaDB", "10.4.0-mariadb", "5.5.5-10.5.9-MariaDB"] {
            assert!(
                matches!(
                    gate(PoolFamily::Mysql, v),
                    Err(GateRefusal::TooOld {
                        family: ServerFamily::MariaDb,
                        ..
                    })
                ),
                "{v}"
            );
        }
    }

    #[test]
    fn an_unparseable_version_fails_closed_and_sqlite_is_refused() {
        for (f, v) in [
            (PoolFamily::Postgres, ""),
            (PoolFamily::Postgres, "16.4"), // no product name: not what PG's version() says
            (PoolFamily::Postgres, "PostgreSQL beta"),
            (PoolFamily::Mysql, ""),
            (PoolFamily::Mysql, "MariaDB"),
            (PoolFamily::Mysql, "v8.0.1"),
        ] {
            assert!(
                matches!(gate(f, v), Err(GateRefusal::Unparseable { .. })),
                "{f:?} {v:?}"
            );
        }
        assert_eq!(
            gate(PoolFamily::Sqlite, "3.53.2"),
            Err(GateRefusal::UnsupportedFamily)
        );
    }

    #[test]
    fn leading_version_reads_only_the_leading_numeric_run() {
        assert_eq!(leading_version("8.0.36-0ubuntu"), Some((8, 0, 36)));
        assert_eq!(leading_version("18beta1"), Some((18, 0, 0)));
        assert_eq!(leading_version("10.11"), Some((10, 11, 0)));
        assert_eq!(leading_version("12"), Some((12, 0, 0)));
        assert_eq!(leading_version("x12"), None);
    }
}
