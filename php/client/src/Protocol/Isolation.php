<?php // /php/client/src/Protocol/Isolation.php
declare(strict_types=1);
namespace Ferro\Protocol;

/**
 * Transaction isolation level carried on the wire as the `u8` `BeginRequest.isolation`
 * (see BeginRequest / PROTOCOL.md §9.1). The `u8` mapping is fixed: `ReadCommitted = 0`,
 * `RepeatableRead = 1`, `Serializable = 2`. There is no fourth value — PostgreSQL's
 * `READ UNCOMMITTED` is an alias for `READ COMMITTED` and maps to 0. `nil` on the wire means
 * "engine/pool default".
 *
 * **This mapping is hand-written in TWO languages, so it is LOCKED by
 * {@see \Ferro\Tests\Conformance\IsolationCrossLanguageTest} — do not edit either side alone.**
 * The argument that kept it out of `/proto` is that it is a message-field VALUE rather than a
 * registry constant (not a method id, flag, error code or type tag — charter rule 2's
 * source-of-truth scope), so it lives here and in the Rust `messages::tx::Isolation` and nowhere
 * else. That argument is defensible but it left the two copies pinned by NOTHING spanning them:
 * the `tx_begin_request` golden vector carries `"isolation": 2` as a RAW INT that `BeginRequest`
 * copied straight through, so the byte lock never touched the enum, and swapping these values
 * measured GREEN across the whole PHP suite (M1-S8a whole-branch review, F5). Live impact then was
 * nil — both BEGIN call sites hardcode `null` — but S8b's `setTransactionIsolation(SERIALIZABLE)`
 * is the first real caller, where a drift would silently DOWNGRADE to READ COMMITTED with no loud
 * signal anywhere: exactly the failure class §9.1 "policies over guesses" exists to prevent.
 *
 * The test now locks it two ways: `Isolation::Serializable` is encoded through
 * {@see BeginRequest::encode} and byte-matched against the Rust-generated `tx_begin_request` frame
 * (the shared artifact), and ALL THREE cases are compared against the Rust enum's own discriminants
 * and `TryFrom<u8>` arms. The durable fix is still to generate both sides from `/proto` — see the
 * test's docblock for why that was out of scope for a review fix round.
 */
enum Isolation: int
{
    case ReadCommitted = 0;
    case RepeatableRead = 1;
    case Serializable = 2;
}
