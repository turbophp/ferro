<?php // /php/client/tests/Live/Fixtures/http-upstream.php
declare(strict_types=1);

/**
 * A scripted HTTP/1.1 upstream for the Ferro HTTP live tests (M6-F8): one process, one
 * `stream_select` loop, any number of concurrent connections, keep-alive. It RECORDS every request
 * it fully read (one JSON line in the log file), so at-most-once is a read-back assertion, and
 * every connection it saw closed by the peer.
 *
 * Usage: php http-upstream.php <port-file> <log-file>
 *   It listens on 127.0.0.1 on an ephemeral port and writes the port to <port-file> once ready.
 *
 * Routes (the path decides; query parameters tune):
 *   /echo                         200, JSON {method, target, headers, body}
 *   /status/<n>[?retry_after=v]   status <n>, body "status <n>"
 *   /delay/<ms>                   200 "delayed" after <ms>
 *   /hold                         read the request, never answer
 *   /stream?chunks=&size=&gap=    200, chunked: <chunks> chunks of <size> bytes, <gap> ms apart
 *   /head-then-hold?status=       head (chunked) + one 7-byte chunk "partial", then never more
 *   /big?bytes=                   200, Content-Length <bytes>, written as fast as the socket takes it;
 *                                 the bytes the kernel accepted are logged as {"written": n} lines
 */

[$portFile, $logFile] = [$argv[1] ?? '', $argv[2] ?? ''];
if ($portFile === '' || $logFile === '') {
    fwrite(STDERR, "usage: http-upstream.php <port-file> <log-file>\n");
    exit(2);
}
$server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
if ($server === false) {
    fwrite(STDERR, "listen failed: {$errstr}\n");
    exit(1);
}
stream_set_blocking($server, false);
$name = stream_socket_get_name($server, false);
$port = (int) substr((string) $name, strrpos((string) $name, ':') + 1);
file_put_contents($portFile . '.tmp', (string) $port);
rename($portFile . '.tmp', $portFile);

$log = static function (array $line) use ($logFile): void {
    file_put_contents($logFile, json_encode($line) . "\n", FILE_APPEND | LOCK_EX);
};

/** @var array<int, array{sock: resource, in: string, out: string, handler: ?Generator, wakeAt: float, target: string, written: int}> $conns */
$conns = [];

$respond = static function (int $status, array $headers, string $body): string {
    $out = "HTTP/1.1 {$status} " . ($status === 200 ? 'OK' : 'Status') . "\r\n";
    foreach ($headers as $k => $v) {
        $out .= "{$k}: {$v}\r\n";
    }
    return $out . 'Content-Length: ' . strlen($body) . "\r\n\r\n" . $body;
};

/**
 * A handler yields what it wants next: ['write', bytes] (resumed once written), ['sleep', seconds],
 * or ['hold'] (never resumed). Returning ends the response.
 */
$handle = static function (string $method, string $target, array $headers, string $body) use ($respond): Generator {
    $path = (string) parse_url($target, PHP_URL_PATH);
    parse_str((string) parse_url($target, PHP_URL_QUERY), $q);
    if ($path === '/echo') {
        yield ['write', $respond(200, ['Content-Type' => 'application/json', 'X-Upstream' => 'echo'], (string) json_encode([
            'method' => $method, 'target' => $target, 'headers' => $headers, 'body' => base64_encode($body),
        ]))];
        return;
    }
    if (preg_match('#^/status/(\d{3})$#', $path, $m) === 1) {
        $h = isset($q['retry_after']) ? ['Retry-After' => (string) $q['retry_after']] : [];
        yield ['write', $respond((int) $m[1], $h, "status {$m[1]}")];
        return;
    }
    if (preg_match('#^/delay/(\d+)$#', $path, $m) === 1) {
        yield ['sleep', (int) $m[1] / 1000];
        yield ['write', $respond(200, [], 'delayed')];
        return;
    }
    if ($path === '/hold') {
        yield ['hold'];
    }
    if ($path === '/stream') {
        $chunks = (int) ($q['chunks'] ?? 3);
        $size = (int) ($q['size'] ?? 5);
        $gap = (int) ($q['gap'] ?? 0) / 1000;
        yield ['write', "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n"];
        for ($i = 0; $i < $chunks; ++$i) {
            if ($i > 0 && $gap > 0) {
                yield ['sleep', $gap];
            }
            $data = str_pad((string) $i, $size, '.', STR_PAD_LEFT);
            yield ['write', dechex(strlen($data)) . "\r\n" . $data . "\r\n"];
        }
        yield ['write', "0\r\n\r\n"];
        return;
    }
    if ($path === '/head-then-hold') {
        $status = (int) ($q['status'] ?? 200);
        yield ['write', "HTTP/1.1 {$status} Status\r\nTransfer-Encoding: chunked\r\n\r\n7\r\npartial\r\n"];
        yield ['hold'];
    }
    if ($path === '/big') {
        $bytes = (int) ($q['bytes'] ?? 1024);
        yield ['write', "HTTP/1.1 200 OK\r\nContent-Length: {$bytes}\r\n\r\n"];
        $block = str_repeat('x', 65536);
        for ($left = $bytes; $left > 0; $left -= 65536) {
            yield ['write', $left >= 65536 ? $block : substr($block, 0, $left)];
        }
        return;
    }
    yield ['write', $respond(404, [], 'no route')];
};

$lastWrittenLog = [];
while (true) {
    $now = microtime(true);
    // Advance every handler that is ready.
    foreach ($conns as $id => &$c) {
        while ($c['handler'] !== null && $c['out'] === '' && $c['wakeAt'] <= $now) {
            $gen = $c['handler'];
            $action = $gen->current();
            if (!$gen->valid()) {
                $c['handler'] = null;
                break;
            }
            if ($action[0] === 'hold') {
                $c['wakeAt'] = INF;
                break;
            }
            if ($action[0] === 'sleep') {
                $c['wakeAt'] = $now + $action[1];
                $gen->next();
                break;
            }
            $c['out'] .= $action[1];
            $gen->next();
        }
    }
    unset($c);

    // Parse complete requests on idle connections.
    foreach ($conns as $id => &$c) {
        if ($c['handler'] !== null || $c['out'] !== '') {
            continue;
        }
        $end = strpos($c['in'], "\r\n\r\n");
        if ($end === false) {
            continue;
        }
        $headLines = explode("\r\n", substr($c['in'], 0, $end));
        [$method, $target] = explode(' ', (string) array_shift($headLines)) + ['', ''];
        $headers = [];
        $len = 0;
        foreach ($headLines as $line) {
            [$k, $v] = explode(':', $line, 2) + ['', ''];
            $headers[] = [$k, trim($v)];
            if (strtolower($k) === 'content-length') {
                $len = (int) trim($v);
            }
        }
        if (strlen($c['in']) < $end + 4 + $len) {
            continue;
        }
        $body = substr($c['in'], $end + 4, $len);
        $c['in'] = (string) substr($c['in'], $end + 4 + $len);
        $c['target'] = $target;
        $log(['method' => $method, 'target' => $target, 'body_len' => strlen($body), 'conn' => $id]);
        $c['handler'] = $handle($method, $target, $headers, $body);
        $c['wakeAt'] = 0.0;
    }
    unset($c);

    $read = [$server];
    $write = [];
    $timeout = 0.05;
    foreach ($conns as $c) {
        $read[] = $c['sock'];
        if ($c['out'] !== '') {
            $write[] = $c['sock'];
        } elseif ($c['handler'] !== null && $c['wakeAt'] !== INF) {
            $timeout = min($timeout, max(0.0, $c['wakeAt'] - microtime(true)));
        }
    }
    $except = null;
    $n = @stream_select($read, $write, $except, 0, (int) ($timeout * 1_000_000));
    if ($n === false) {
        continue;
    }
    foreach ($read as $sock) {
        if ($sock === $server) {
            $new = @stream_socket_accept($server, 0);
            if ($new !== false) {
                stream_set_blocking($new, false);
                $conns[(int) $new] = ['sock' => $new, 'in' => '', 'out' => '', 'handler' => null, 'wakeAt' => 0.0, 'target' => '', 'written' => 0];
            }
            continue;
        }
        $id = (int) $sock;
        $data = @fread($sock, 65536);
        if ($data === false || ($data === '' && feof($sock))) {
            $log(['event' => 'closed', 'target' => $conns[$id]['target'], 'written' => $conns[$id]['written']]);
            fclose($sock);
            unset($conns[$id]);
            continue;
        }
        $conns[$id]['in'] .= $data;
    }
    foreach ($write as $sock) {
        $id = (int) $sock;
        if (!isset($conns[$id])) {
            continue;
        }
        $w = @fwrite($sock, $conns[$id]['out']);
        if ($w === false) {
            $log(['event' => 'closed', 'target' => $conns[$id]['target'], 'written' => $conns[$id]['written']]);
            fclose($sock);
            unset($conns[$id]);
            continue;
        }
        $conns[$id]['out'] = (string) substr($conns[$id]['out'], $w);
        $conns[$id]['written'] += $w;
        if (str_starts_with($conns[$id]['target'], '/big') && ($lastWrittenLog[$id] ?? 0) + 262144 <= $conns[$id]['written']) {
            $lastWrittenLog[$id] = $conns[$id]['written'];
            $log(['written' => $conns[$id]['written'], 'conn' => $id]);
        }
    }
}
