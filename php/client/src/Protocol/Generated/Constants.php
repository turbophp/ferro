<?php

declare(strict_types=1);

// @generated from /proto/registry.lock.json — do not edit.

namespace Ferro\Protocol\Generated;

final class Constants
{
    public const PROTOCOL_VERSION = 1;
    public const MAGIC = 247;
    public const MAX_FRAME_PAYLOAD = 16777216;
    public const DEFAULT_CREDIT_FRAMES = 64;
    public const DEFAULT_CREDIT_BYTES = 4194304;

    public const FLAG_CANCEL = 4;
    public const FLAG_COMPRESSED = 16;
    public const FLAG_END = 2;
    public const FLAG_OOB_FD = 8;
    public const FLAG_STREAM = 1;

    public const SERVICE_ADMIN = 5;
    public const SERVICE_CORE = 1;
    public const SERVICE_SQL = 2;
    public const SERVICE_STREAM = 4;
    public const SERVICE_TX = 3;

    public const METHOD_CORE_GOODBYE = 5;
    public const METHOD_CORE_HELLO = 1;
    public const METHOD_CORE_HELLO_ACK = 2;
    public const METHOD_CORE_PING = 3;
    public const METHOD_CORE_PONG = 4;
    public const METHOD_CORE_WINDOW_UPDATE = 6;

    public const OUTCOME_CANCELLED = 2;
    public const OUTCOME_ERROR = 1;
    public const OUTCOME_OK = 0;

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
    public const ERR_FOREIGN_KEY = 12291;
    public const ERR_FOREIGN_KEY_BRANCH = 3;
    public const ERR_NOT_NULL = 12292;
    public const ERR_NOT_NULL_BRANCH = 3;
    public const ERR_POOL_TIMEOUT = 4098;
    public const ERR_POOL_TIMEOUT_BRANCH = 1;
    public const ERR_PROTOCOL = 12297;
    public const ERR_PROTOCOL_BRANCH = 3;
    public const ERR_QUERY_TIMEOUT = 12295;
    public const ERR_QUERY_TIMEOUT_BRANCH = 3;
    public const ERR_REPLICA_UNAVAILABLE = 4102;
    public const ERR_REPLICA_UNAVAILABLE_BRANCH = 1;
    public const ERR_SERIALIZATION_FAILURE = 4101;
    public const ERR_SERIALIZATION_FAILURE_BRANCH = 1;
    public const ERR_SYNTAX = 12289;
    public const ERR_SYNTAX_BRANCH = 3;
    public const ERR_TX_DEADLINE = 4099;
    public const ERR_TX_DEADLINE_BRANCH = 1;
    public const ERR_UNIQUE = 12290;
    public const ERR_UNIQUE_BRANCH = 3;
    public const ERR_UNSUPPORTED = 12298;
    public const ERR_UNSUPPORTED_BRANCH = 3;
    public const ERR_WRITE_UNCONFIRMED = 8193;
    public const ERR_WRITE_UNCONFIRMED_BRANCH = 2;
}
