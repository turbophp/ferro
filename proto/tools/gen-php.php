<?php // /proto/tools/gen-php.php — reads registry.lock.json, emits Generated/Constants.php
declare(strict_types=1);
$root = dirname(__DIR__, 2);
$lock = json_decode(file_get_contents("$root/proto/registry.lock.json"), true, 512, JSON_THROW_ON_ERROR);
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
$out .= "}\n";
$dir = "$root/php/client/src/Protocol/Generated";
@mkdir($dir, 0777, true);
file_put_contents("$dir/Constants.php", $out);
fwrite(STDERR, "wrote $dir/Constants.php\n");
