<?php // /php/client/src/Protocol/Isolation.php
declare(strict_types=1);
namespace Ferro\Protocol;

/**
 * Transaction isolation level carried on the wire as the `u8` `BeginRequest.isolation`
 * (see BeginRequest / PROTOCOL.md §9.1). It is a message-field VALUE, NOT a `/proto` registry
 * constant (isolation is neither a method id, flag, error code, nor type tag — charter rule 2's
 * source-of-truth scope), so the mapping is fixed HERE and in the Rust `messages::tx::Isolation`,
 * never in `methods.toml`/`registry.lock.json`. The `u8` mapping is fixed: `ReadCommitted = 0`,
 * `RepeatableRead = 1`, `Serializable = 2`. There is no fourth value — PostgreSQL's
 * `READ UNCOMMITTED` is an alias for `READ COMMITTED` and maps to 0. `nil` on the wire means
 * "engine/pool default".
 */
enum Isolation: int
{
    case ReadCommitted = 0;
    case RepeatableRead = 1;
    case Serializable = 2;
}
