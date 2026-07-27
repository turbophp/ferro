<?php // /proto/tools/gen-php.php — reads registry.lock.json, emits Generated/Constants.php
declare(strict_types=1);
$root = dirname(__DIR__, 2);
$raw = (string) file_get_contents("$root/proto/registry.lock.json");
$lock = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);

/**
 * 64-bit FNV-1a over the RAW lock-file bytes, rendered as 16-char lowercase hex — byte-identical to
 * ferro-proto/build.rs `fnv1a_hex` (OFFSET_BASIS 0xcbf29ce484222325, PRIME 0x100000001b3). Computed
 * as 4x16-bit limbs so every partial product stays a native PHP int (no float overflow), which is
 * why this is exact where a naive `$h * $prime` would silently promote to float past PHP_INT_MAX.
 * This is the `type_registry_hash` the client sends in HELLO and ferrod hard-checks (SPEC §5).
 */
$fnv1a64_hex = function (string $bytes): string {
    $h = [0x2325, 0x8422, 0x9ce4, 0xcbf2]; // limb0(low)..limb3(high) of the offset basis
    $p = [0x01b3, 0x0000, 0x0100, 0x0000]; // limbs of the FNV prime
    $len = strlen($bytes);
    for ($i = 0; $i < $len; $i++) {
        $h[0] ^= ord($bytes[$i]);
        $r0 = $h[0] * $p[0];
        $r1 = $h[1] * $p[0] + $h[0] * $p[1];
        $r2 = $h[2] * $p[0] + $h[1] * $p[1] + $h[0] * $p[2];
        $r3 = $h[3] * $p[0] + $h[2] * $p[1] + $h[1] * $p[2] + $h[0] * $p[3];
        $c = 0;
        $t = $r0 + $c; $h[0] = $t & 0xffff; $c = $t >> 16;
        $t = $r1 + $c; $h[1] = $t & 0xffff; $c = $t >> 16;
        $t = $r2 + $c; $h[2] = $t & 0xffff; $c = $t >> 16;
        $t = $r3 + $c; $h[3] = $t & 0xffff; // final carry drops (mod 2^64)
    }
    return sprintf('%04x%04x%04x%04x', $h[3], $h[2], $h[1], $h[0]);
};
$out = "<?php\n\ndeclare(strict_types=1);\n\n// @generated from /proto/registry.lock.json — do not edit.\n\nnamespace Ferro\\Protocol\\Generated;\n\nfinal class Constants\n{\n";
$out .= "    public const PROTOCOL_VERSION = {$lock['protocol_version']};\n";
$out .= "    public const MAGIC = {$lock['magic']};\n";
$out .= "    public const MAX_FRAME_PAYLOAD = {$lock['max_frame_payload']};\n";
$out .= "    public const DEFAULT_CREDIT_FRAMES = {$lock['default_credit_frames']};\n";
$out .= "    public const DEFAULT_CREDIT_BYTES = {$lock['default_credit_bytes']};\n\n";
$emit = function (string $prefix, array $kv) {
    $s = '';
    foreach ($kv as $k => $v) { $s .= "    public const {$prefix}_{$k} = {$v};\n"; }
    return $s;
};
foreach ($lock['flags'] as $k => $v) { $out .= "    public const FLAG_{$k} = {$v};\n"; }
$out .= "\n";
foreach ($lock['services'] as $k => $v) { $out .= "    public const SERVICE_{$k} = {$v};\n"; }
$out .= "\n";
foreach ($lock['methods'] as $svc => $kv) {
    foreach ($kv as $k => $v) { $out .= "    public const METHOD_" . strtoupper($svc) . "_{$k} = {$v};\n"; }
}
$out .= "\n";
foreach ($lock['outcome'] as $k => $v) { $out .= "    public const OUTCOME_{$k} = {$v};\n"; }
$out .= "\n";
foreach ($lock['tags'] as $k => $v) { $out .= "    public const TAG_{$k} = {$v};\n"; }
$out .= "\n";
foreach ($lock['branches'] as $k => $v) {
    // camel-split to match Rust screaming(): NonRetryable -> NON_RETRYABLE
    $u = strtoupper(preg_replace('/(?<=[a-z0-9])(?=[A-Z])/', '_', $k));
    $out .= "    public const BRANCH_{$u} = {$v};\n";
}
$out .= "\n";
foreach ($lock['features'] as $side => $kv) {
    foreach ($kv as $k => $v) { $out .= "    public const FEATURE_" . strtoupper($side) . "_{$k} = {$v};\n"; }
}
$out .= "\n";
foreach ($lock['codes'] as $name => $ec) {
    $u = strtoupper(preg_replace('/(?<=[a-z0-9])(?=[A-Z])/', '_', $name));
    $out .= "    public const ERR_{$u} = {$ec['code']};\n";
    $out .= "    public const ERR_{$u}_BRANCH = {$ec['branch']};\n";
}
$out .= "\n";
$out .= "    public const TYPE_REGISTRY_HASH = '" . $fnv1a64_hex($raw) . "';\n";
$out .= "}\n";
$dir = "$root/php/client/src/Protocol/Generated";
@mkdir($dir, 0777, true);
file_put_contents("$dir/Constants.php", $out);
fwrite(STDERR, "wrote $dir/Constants.php\n");
