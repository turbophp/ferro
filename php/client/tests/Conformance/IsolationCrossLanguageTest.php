<?php // /php/client/tests/Conformance/IsolationCrossLanguageTest.php
declare(strict_types=1);
namespace Ferro\Tests\Conformance;

use Ferro\Protocol\BeginRequest;
use Ferro\Protocol\Isolation;
use Ferro\Protocol\Msgpack\PurePacker;
use PHPUnit\Framework\TestCase;

/**
 * **THE cross-language lock for `BeginRequest.isolation` (M1-S8a whole-branch review, F5).**
 *
 * The `u8` 0/1/2 → ReadCommitted/RepeatableRead/Serializable mapping is written out BY HAND in both
 * `Ferro\Protocol\Isolation` and the Rust `messages::tx::Isolation`, and until this file existed it
 * was pinned by nothing that spanned the two: Rust had a unit test over its own enum, PHP had none,
 * and the `tx_begin_request` golden vector carries `"isolation": 2` as a RAW INT that
 * `BeginRequest::encode` copied straight through — so the vector byte lock never touched the enum.
 * Measured at review: swapping the PHP values (ReadCommitted=2, Serializable=0) left the full PHP
 * suite green (541 tests) and PHPStan level 9 [OK].
 *
 * Live impact was nil when this file landed — both BEGIN sites hardcoded `'isolation' => null`, so
 * the enum was unreachable — and a drift degrades SERIALIZABLE to READ COMMITTED silently, the exact
 * failure class §9.1 "policies over guesses" exists to prevent, so it was locked before the caller
 * landed rather than after. **The caller has now landed (M1-S8b Task 3):
 * {@see \Ferro\Client\Connection::begin} takes an `?Isolation` and passes the ENUM CASE to
 * {@see BeginRequest::encode}, so this lock now guards a live path.** Its behavioural half is
 * `tests/Live/BeginIsolationLiveTest`: with the two values below swapped, PostgreSQL reports
 * `repeatable read` inside a transaction that asked for `serializable` — measured (M1-S8b Task 3
 * Step 8 mutation 2), not assumed. The closure form ({@see \Ferro\Client\Connection::transaction})
 * still hardcodes `null`, i.e. the pool default.
 *
 * **Why not generate it from `/proto`, which is the RIGHT answer (charter rule 2).** Promoting
 * `isolation` into the registry touches `proto/methods.toml`, `ferro-proto`'s `registry.rs` /
 * `build.rs`, `proto/tools/gen-php.php`, a REGENERATED `proto/registry.lock.json` (which moves the
 * handshake registry hash), plus both hand-written enums and the two registry-sync tests. That is a
 * shared-registry change spanning the Rust engine, and this file landed in a review FIX round scoped
 * to `php/client`, running alongside other agents in other worktrees — regenerating the lock there
 * would have collided with all of them. So this is the reviewer's stated MINIMUM, done as strongly
 * as a PHP-side change can be, and the /proto promotion is recorded as the durable follow-up.
 *
 * The two locks below are deliberately of DIFFERENT kinds, because neither alone is enough:
 *
 *  * {@see testSerializableEncodesToTheGoldenVectorBytes} is BEHAVIOURAL and rides the shared
 *    artifact — the enum goes through the real encoder and the result must equal the frame the RUST
 *    codec produced. It can only cover `Serializable`: the vector carries one isolation value.
 *  * {@see testEveryCaseMatchesTheRustPeerDefinition} covers ALL THREE by reading the peer's own
 *    source. That is a declaration-to-declaration lock, not a behavioural one — but the Rust
 *    discriminant IS the wire value (`impl From<Isolation> for u8 { v as u8 }`), so it is the
 *    definition itself, not a proxy for it. It cross-checks the discriminants against the
 *    `TryFrom<u8>` match arms as well, so changing only one of the two Rust statements is also RED.
 */
final class IsolationCrossLanguageTest extends TestCase
{
    private const VECTOR = __DIR__ . '/../../../../proto/vectors/tx_begin_request.json';
    private const RUST_PEER = __DIR__ . '/../../../../engine/crates/ferro-proto/src/messages/tx.rs';

    /**
     * LOCK A(i) — the reviewer's literal minimum, expressed against the shared artifact instead of
     * against a hardcoded `2`: `Isolation::Serializable->value` must equal the `message.isolation`
     * the RUST codec wrote into `tx_begin_request`, in both directions.
     *
     * It is a SEPARATE test from the byte lock below on purpose. Folded in as a precondition it
     * would short-circuit the byte lock on exactly the drift both exist to catch, and the byte lock
     * would then never have been shown to bite.
     */
    public function testSerializableMatchesTheVectorsIsolationField(): void
    {
        $message = self::vectorMessage();

        $this->assertSame(
            Isolation::Serializable->value,
            $message['isolation'] ?? null,
            'PHP Serializable must equal the isolation byte the Rust codec wrote into the vector',
        );
        $this->assertSame(
            Isolation::Serializable,
            Isolation::from((int) ($message['isolation'] ?? -1)),
            'and the vector byte must map back to the SAME case',
        );
    }

    /**
     * LOCK A(ii) — behavioural, via the shared artifact.
     *
     * Encode the ENUM CASE (not an int literal) through the real `BeginRequest::encode` and require
     * the exact payload bytes of the Rust-generated `tx_begin_request` frame. If PHP's
     * `Serializable` were any value other than the one Rust wrote into that frame, the bytes move.
     * This is what the pre-existing golden-vector byte lock could never assert: `txRequestVectors`
     * feeds `encode()` the vector's own RAW INT, so the enum never participated in it.
     */
    public function testSerializableEncodesToTheGoldenVectorBytes(): void
    {
        $v = self::vector();
        $message = self::vectorMessage();

        $payload = substr((string) hex2bin((string) $v['frame_hex']), 16);
        $encoded = BeginRequest::encode(
            [
                'pool' => $message['pool'] ?? '',
                'isolation' => Isolation::Serializable,
                'readonly' => $message['readonly'] ?? false,
            ],
            new PurePacker(),
        );
        $this->assertSame(
            bin2hex($payload),
            bin2hex($encoded),
            'BeginRequest encoded with the Isolation ENUM must byte-match the Rust-generated vector',
        );
    }

    /**
     * LOCK B — all three cases, against the Rust peer's own definition.
     *
     * Parses `messages::tx::Isolation`'s discriminants AND its `TryFrom<u8>` match arms, requires the
     * two Rust statements to agree with each other, then requires the PHP enum to equal them exactly
     * (same case names, same values, same count). Swapping, renaming, adding or dropping a case on
     * EITHER side is RED.
     *
     * Every parse step fails LOUDLY rather than skipping. A lock that silently goes vacuous when the
     * peer file moves is worse than no lock — it reports green over an unguarded mapping.
     */
    public function testEveryCaseMatchesTheRustPeerDefinition(): void
    {
        $src = self::rustPeerSource();

        // --- the enum's discriminants: `ReadCommitted = 0,` ---
        $this->assertSame(
            1,
            preg_match('/enum\s+Isolation\s*\{(.*?)\}/s', $src, $block),
            'could not find `enum Isolation { … }` in the Rust peer — it was renamed or moved, and '
            . 'this lock must be re-pointed rather than left silently vacuous',
        );
        preg_match_all('/(\w+)\s*=\s*(\d+)\s*,/', $block[1], $m, PREG_SET_ORDER);
        $rustEnum = [];
        foreach ($m as $hit) { $rustEnum[$hit[1]] = (int) $hit[2]; }
        $this->assertCount(3, $rustEnum, 'expected exactly 3 Rust Isolation variants, parsed: ' . json_encode($rustEnum));

        // --- the decode arms: `0 => Ok(Isolation::ReadCommitted),` — an INDEPENDENT statement of
        //     the same mapping on the engine's actual decode path. ---
        preg_match_all('/(\d+)\s*=>\s*Ok\(Isolation::(\w+)\)/', $src, $m2, PREG_SET_ORDER);
        $rustTryFrom = [];
        foreach ($m2 as $hit) { $rustTryFrom[$hit[2]] = (int) $hit[1]; }
        $this->assertCount(3, $rustTryFrom, 'expected exactly 3 Rust TryFrom<u8> arms, parsed: ' . json_encode($rustTryFrom));

        ksort($rustEnum);
        ksort($rustTryFrom);
        $this->assertSame(
            $rustEnum,
            $rustTryFrom,
            'the Rust enum discriminants and its TryFrom<u8> arms disagree — the engine would encode '
            . 'one mapping and decode another',
        );

        // --- PHP ---
        $php = [];
        foreach (Isolation::cases() as $case) { $php[$case->name] = $case->value; }
        ksort($php);

        $this->assertSame(
            $rustEnum,
            $php,
            "PHP's Isolation enum has DRIFTED from the Rust peer "
            . '(engine/crates/ferro-proto/src/messages/tx.rs). These two are hand-written copies of '
            . 'one wire mapping; a mismatch silently downgrades a caller\'s isolation level. Fix both '
            . 'sides together — or, better, generate them from /proto.',
        );
    }

    /**
     * The vector's decoded `message` object.
     * @return array<string,mixed>
     */
    private static function vectorMessage(): array
    {
        $v = self::vector();
        /** @var array<string,mixed> $m */
        $m = is_array($v['message'] ?? null) ? $v['message'] : [];
        return $m;
    }

    /** @return array<string,mixed> */
    private static function vector(): array
    {
        $raw = is_file(self::VECTOR) ? file_get_contents(self::VECTOR) : false;
        if ($raw === false) {
            self::fail('missing golden vector ' . self::VECTOR . ' — the byte lock cannot run');
        }
        /** @var array<string,mixed> $v */
        $v = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
        return $v;
    }

    private static function rustPeerSource(): string
    {
        $src = is_file(self::RUST_PEER) ? file_get_contents(self::RUST_PEER) : false;
        if ($src === false) {
            self::fail(
                'missing Rust peer ' . self::RUST_PEER . '. The Isolation mapping is hand-written in '
                . 'both languages; if the peer moved, RE-POINT this lock. Never soften it to a skip.',
            );
        }
        return $src;
    }
}
