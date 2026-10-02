# Follow-up: the Doctrine tier's `quote()` on PostgreSQL depends on `standard_conforming_strings`

> **STATUS: OPEN** — found while fixing the same class in the Laravel tier at M2-C1g
> (SPEC §22.2 (cc)); not fixed there because it is a different package whose `quote()` is locked to
> DBAL's stock platform rule.

## What happens

`Ferro\DBAL\AbstractConnection::quoteString()` follows DBAL's stock platform rule per family. On
PostgreSQL that is `AbstractPlatform::quoteStringLiteral()`: double `'`, leave `\` raw. That is
correct only while the session has `standard_conforming_strings = on`, where a backslash is an
ordinary character. With it OFF, a backslash in an ordinary literal IS an escape, so a value whose
`'` is preceded by `\` ends the literal early.

Reproduced on PostgreSQL 16 with DBAL 4's own `PostgreSQLPlatform`:

```
DBAL PG literal: 'x\'' union select ''PWNED'' -- '
scs=on  rows=1 ["x\\' union select 'PWNED' -- "]
scs=off ERROR (literal broken out): syntax error at or near "''"
```

A stock PDO connection does not have this problem, because `pdo_pgsql` escapes with
`PQescapeStringConn()`, which reads the LIVE setting of the connection it is about to send on. The
Ferro tier has no such connection to read (the same reason the Laravel tier changed at C1g).

## Why it is not fixed yet

- The default (`on`, PostgreSQL's default since 9.1) is safe. Reaching the bad state takes a server, role or
  database configured `off`, or an application that turns it off inside its own transaction.
- `quoteString()` is deliberately pinned to DBAL's platform accessors by `DriverQuoteTest`, so that
  a DBAL change turns red. A fix is a deliberate divergence from the stock platform, which needs its
  own slice and its own tests.
- DBAL documents `quote()` as discouraged, and parameters are the supported path (SPEC §21 D5).

## The fix, when taken

Use the Laravel tier's mode-independent form (`Ferro\Laravel\FerroPdoShim::quote()`, §22.2 (cc)).
For a string with no backslash, keep doubling `'`: that is the stock bytes and is correct in both
settings. For a string with a backslash, emit `E'…'` with every `\` and `'` doubled. It reads the
same whatever the setting is. On MySQL the tier's rule (`\` → `\\`, `'` → `''`) never breaks out
in either ESCAPE mode, but it mis-renders backslashes under `NO_BACKSLASH_ESCAPES`. It can also break
out under a GBK-class connection charset, which an `init_connect` can set; that is equal to
`pdo_mysql`'s exposure. The Laravel tier's `_utf8mb4 X'<hex>'` form avoids both problems.
