<?php

declare(strict_types=1);

// @generated from /proto/registry.lock.json — do not edit.

namespace Ferro\Protocol\Generated;

final class Constants
{
    public const PROTOCOL_VERSION = 4;
    public const MAGIC = 247;
    public const MAX_FRAME_PAYLOAD = 16777216;
    public const DEFAULT_CREDIT_FRAMES = 64;
    public const DEFAULT_CREDIT_BYTES = 16777216;
    public const QUEUE_WAIT_GRACE_MS = 1000;
    public const QUEUE_HANDLE_MAX_BYTES = 1024;
    public const QUEUE_ENQUEUE_MAX_JOBS = 1000;
    public const QUEUE_RESERVE_MAX_QUEUES = 16;

    public const FLAG_CANCEL = 4;
    public const FLAG_COMPRESSED = 16;
    public const FLAG_END = 2;
    public const FLAG_OOB_FD = 8;
    public const FLAG_STREAM = 1;

    public const SERVICE_ADMIN = 5;
    public const SERVICE_CORE = 1;
    public const SERVICE_HTTP = 6;
    public const SERVICE_QUEUE = 7;
    public const SERVICE_SQL = 2;
    public const SERVICE_STREAM = 4;
    public const SERVICE_TX = 3;

    public const METHOD_ADMIN_BACKUP = 1;
    public const METHOD_CORE_GOODBYE = 5;
    public const METHOD_CORE_HELLO = 1;
    public const METHOD_CORE_HELLO_ACK = 2;
    public const METHOD_CORE_PING = 3;
    public const METHOD_CORE_PONG = 4;
    public const METHOD_CORE_WINDOW_UPDATE = 6;
    public const METHOD_HTTP_BODY = 3;
    public const METHOD_HTTP_HEAD = 2;
    public const METHOD_HTTP_REQUEST = 1;
    public const METHOD_QUEUE_ACK = 3;
    public const METHOD_QUEUE_CLEAR = 7;
    public const METHOD_QUEUE_ENQUEUE = 1;
    public const METHOD_QUEUE_EXTEND = 5;
    public const METHOD_QUEUE_RELEASE = 4;
    public const METHOD_QUEUE_RESERVE = 2;
    public const METHOD_QUEUE_SIZE = 6;
    public const METHOD_SQL_COPY_IN = 2;
    public const METHOD_SQL_COPY_OUT = 3;
    public const METHOD_SQL_EXEC = 1;
    public const METHOD_STREAM_COPY_DATA = 3;
    public const METHOD_STREAM_COPY_DONE = 4;
    public const METHOD_STREAM_DATA = 2;
    public const METHOD_STREAM_HEAD = 1;
    public const METHOD_TX_BEGIN = 1;
    public const METHOD_TX_COMMIT = 2;
    public const METHOD_TX_RELEASE = 5;
    public const METHOD_TX_ROLLBACK = 3;
    public const METHOD_TX_ROLLBACK_TO = 6;
    public const METHOD_TX_SAVEPOINT = 4;

    public const OUTCOME_CANCELLED = 2;
    public const OUTCOME_ERROR = 1;
    public const OUTCOME_OK = 0;

    public const OOB_ENCODING_FRAME_PAYLOAD = 0;

    public const ACK_OUTCOME_ACKED = 1;
    public const ACK_OUTCOME_GONE = 2;

    public const TAG_ARRAY = 14;
    public const TAG_BOOL = 1;
    public const TAG_BYTES = 7;
    public const TAG_DATE = 8;
    public const TAG_DECIMAL = 5;
    public const TAG_F64 = 4;
    public const TAG_I64 = 2;
    public const TAG_INET = 16;
    public const TAG_INTERVAL = 15;
    public const TAG_JSON = 13;
    public const TAG_NULL = 0;
    public const TAG_TEXT = 6;
    public const TAG_TIME = 9;
    public const TAG_TIMESTAMP = 10;
    public const TAG_TIMESTAMPTZ = 11;
    public const TAG_U64 = 3;
    public const TAG_UUID = 12;
    public const TAG_VECTOR = 17;

    public const BRANCH_INDETERMINATE = 2;
    public const BRANCH_NON_RETRYABLE = 3;
    public const BRANCH_RETRYABLE = 1;

    public const FEATURE_CLIENT_FIBERS = 2;
    public const FEATURE_CLIENT_MEMFD_RX = 1;
    public const FEATURE_ENGINE_HTTP = 8;
    public const FEATURE_ENGINE_LISTEN_STREAMS = 2;
    public const FEATURE_ENGINE_MANIFEST = 4;
    public const FEATURE_ENGINE_MEMFD = 1;

    public const ERR_AUTH = 12294;
    public const ERR_AUTH_BRANCH = 3;
    public const ERR_CANCELLED = 12296;
    public const ERR_CANCELLED_BRANCH = 3;
    public const ERR_CHECK = 12293;
    public const ERR_CHECK_BRANCH = 3;
    public const ERR_CONNECTION_LOST = 4097;
    public const ERR_CONNECTION_LOST_BRANCH = 1;
    public const ERR_DEADLOCK = 4100;
    public const ERR_DEADLOCK_BRANCH = 1;
    public const ERR_FORBIDDEN = 12300;
    public const ERR_FORBIDDEN_BRANCH = 3;
    public const ERR_FOREIGN_KEY = 12291;
    public const ERR_FOREIGN_KEY_BRANCH = 3;
    public const ERR_INVALID_HANDLE = 12305;
    public const ERR_INVALID_HANDLE_BRANCH = 3;
    public const ERR_LEASE_LOST = 12303;
    public const ERR_LEASE_LOST_BRANCH = 3;
    public const ERR_NOT_NULL = 12292;
    public const ERR_NOT_NULL_BRANCH = 3;
    public const ERR_POOL_MISMATCH = 12304;
    public const ERR_POOL_MISMATCH_BRANCH = 3;
    public const ERR_POOL_TIMEOUT = 4098;
    public const ERR_POOL_TIMEOUT_BRANCH = 1;
    public const ERR_PROTOCOL = 12297;
    public const ERR_PROTOCOL_BRANCH = 3;
    public const ERR_QUERY_TIMEOUT = 12295;
    public const ERR_QUERY_TIMEOUT_BRANCH = 3;
    public const ERR_RATE_LIMITED = 4104;
    public const ERR_RATE_LIMITED_BRANCH = 1;
    public const ERR_REPLICA_UNAVAILABLE = 4102;
    public const ERR_REPLICA_UNAVAILABLE_BRANCH = 1;
    public const ERR_RESPONSE_INCOMPLETE = 12302;
    public const ERR_RESPONSE_INCOMPLETE_BRANCH = 3;
    public const ERR_SERIALIZATION_FAILURE = 4101;
    public const ERR_SERIALIZATION_FAILURE_BRANCH = 1;
    public const ERR_SYNTAX = 12289;
    public const ERR_SYNTAX_BRANCH = 3;
    public const ERR_TLS_REFUSED = 12301;
    public const ERR_TLS_REFUSED_BRANCH = 3;
    public const ERR_TX_DEADLINE = 4099;
    public const ERR_TX_DEADLINE_BRANCH = 1;
    public const ERR_TX_NOT_FOUND = 12299;
    public const ERR_TX_NOT_FOUND_BRANCH = 3;
    public const ERR_UNIQUE = 12290;
    public const ERR_UNIQUE_BRANCH = 3;
    public const ERR_UNSUPPORTED = 12298;
    public const ERR_UNSUPPORTED_BRANCH = 3;
    public const ERR_UPSTREAM_UNAVAILABLE = 4103;
    public const ERR_UPSTREAM_UNAVAILABLE_BRANCH = 1;
    public const ERR_WRITE_UNCONFIRMED = 8193;
    public const ERR_WRITE_UNCONFIRMED_BRANCH = 2;

    public const HTTP_CAUSE_BODY_BUDGET = 'body_budget';
    public const HTTP_CAUSE_BODY_EOF = 'body_eof';
    public const HTTP_CAUSE_BODY_FRAMING = 'body_framing';
    public const HTTP_CAUSE_BODY_RESET = 'body_reset';
    public const HTTP_CAUSE_BREAKER_OPEN = 'breaker_open';
    public const HTTP_CAUSE_BREAKER_PROBE_BUSY = 'breaker_probe_busy';
    public const HTTP_CAUSE_CANCELLED = 'cancelled';
    public const HTTP_CAUSE_CONNECT_REFUSED = 'connect_refused';
    public const HTTP_CAUSE_CONNECT_TIMEOUT = 'connect_timeout';
    public const HTTP_CAUSE_CONNECT_UNREACHABLE = 'connect_unreachable';
    public const HTTP_CAUSE_DEADLINE = 'deadline';
    public const HTTP_CAUSE_DECODE = 'decode';
    public const HTTP_CAUSE_DNS = 'dns';
    public const HTTP_CAUSE_DRAINING = 'draining';
    public const HTTP_CAUSE_EOF_EMPTY = 'eof_empty';
    public const HTTP_CAUSE_EOF_PARTIAL_HEAD = 'eof_partial_head';
    public const HTTP_CAUSE_FORBIDDEN_ADDRESS = 'forbidden_address';
    public const HTTP_CAUSE_FORBIDDEN_BODY = 'forbidden_body';
    public const HTTP_CAUSE_FORBIDDEN_HEADER = 'forbidden_header';
    public const HTTP_CAUSE_FORBIDDEN_METHOD = 'forbidden_method';
    public const HTTP_CAUSE_FORBIDDEN_ORIGIN = 'forbidden_origin';
    public const HTTP_CAUSE_FORBIDDEN_TARGET = 'forbidden_target';
    public const HTTP_CAUSE_FORBIDDEN_UPSTREAM = 'forbidden_upstream';
    public const HTTP_CAUSE_H2_CONNECTION_ERROR = 'h2_connection_error';
    public const HTTP_CAUSE_H2_GOAWAY_ABOVE_LAST = 'h2_goaway_above_last';
    public const HTTP_CAUSE_H2_REFUSED_STREAM = 'h2_refused_stream';
    public const HTTP_CAUSE_H2_STREAM_ERROR = 'h2_stream_error';
    public const HTTP_CAUSE_INFORMATIONAL_101 = 'informational_101';
    public const HTTP_CAUSE_MALFORMED_HEAD = 'malformed_head';
    public const HTTP_CAUSE_MAX_RESPONSE_BYTES = 'max_response_bytes';
    public const HTTP_CAUSE_OVERSIZE_HEAD = 'oversize_head';
    public const HTTP_CAUSE_QUEUE_FULL = 'queue_full';
    public const HTTP_CAUSE_QUEUE_TIMEOUT = 'queue_timeout';
    public const HTTP_CAUSE_RATE_LIMITED = 'rate_limited';
    public const HTTP_CAUSE_READ_IDLE = 'read_idle';
    public const HTTP_CAUSE_RESET = 'reset';
    public const HTTP_CAUSE_RETRY_AFTER_HOLD = 'retry_after_hold';
    public const HTTP_CAUSE_TIMEOUT = 'timeout';
    public const HTTP_CAUSE_TLS_ALPN = 'tls_alpn';
    public const HTTP_CAUSE_TLS_HANDSHAKE = 'tls_handshake';
    public const HTTP_CAUSE_TLS_VERIFY = 'tls_verify';
    public const HTTP_CAUSE_TLS_VERSION = 'tls_version';
    public const HTTP_CAUSE_UNSENT_CLOSED = 'unsent_closed';
    public const HTTP_CAUSE_UNSENT_WRITE = 'unsent_write';
    public const HTTP_CAUSE_WRITE = 'write';
    public const HTTP_CAUSES = [
        'body_budget',
        'body_eof',
        'body_framing',
        'body_reset',
        'breaker_open',
        'breaker_probe_busy',
        'cancelled',
        'connect_refused',
        'connect_timeout',
        'connect_unreachable',
        'deadline',
        'decode',
        'dns',
        'draining',
        'eof_empty',
        'eof_partial_head',
        'forbidden_address',
        'forbidden_body',
        'forbidden_header',
        'forbidden_method',
        'forbidden_origin',
        'forbidden_target',
        'forbidden_upstream',
        'h2_connection_error',
        'h2_goaway_above_last',
        'h2_refused_stream',
        'h2_stream_error',
        'informational_101',
        'malformed_head',
        'max_response_bytes',
        'oversize_head',
        'queue_full',
        'queue_timeout',
        'rate_limited',
        'read_idle',
        'reset',
        'retry_after_hold',
        'timeout',
        'tls_alpn',
        'tls_handshake',
        'tls_verify',
        'tls_version',
        'unsent_closed',
        'unsent_write',
        'write',
    ];

    public const TYPE_REGISTRY_HASH = 'cc12fc0a92d82617';
}
