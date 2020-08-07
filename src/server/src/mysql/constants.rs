#![allow(dead_code)]

/// Use the improved version of Old Password Authentication.
pub const CAPABILITY_CLIENT_LONG_PASSWORD: u32 = 0x00000001;

/// Send found rows instead of affected rows in EOF_Packet.
pub const CAPABILITY_CLIENT_FOUND_ROWS: u32 = 0x00000002;

/// Longer flags in Protocol::ColumnDefinition320.
pub const CAPABILITY_CLIENT_LONG_FLAG: u32 = 0x00000004;

/// Database (schema) name can be specified on connect in Handshake Response Packet.
pub const CAPABILITY_CLIENT_CONNECT_WITH_DB: u32 = 0x00000008;

/// Do not permit database.table.column.
pub const CAPABILITY_CLIENT_NO_SCHEMA: u32 = 0x00000010;

/// Compression protocol supported.
pub const CAPABILITY_CLIENT_COMPRESS: u32 = 0x00000020;

/// Special handling of ODBC behavior.
pub const CAPABILITY_CLIENT_ODBC: u32 = 0x00000040;

/// Can use LOAD DATA LOCAL.
pub const CAPABILITY_CLIENT_LOCAL_FILES: u32 = 0x00000080;

/// CLIENT_IGNORE_SPACE
pub const CAPABILITY_CLIENT_IGNORE_SPACE: u32 = 0x00000100;

/// Supports the 4.1 protocol.
pub const CAPABILITY_CLIENT_PROTOCOL_41: u32 = 0x00000200;

/// wait_timeout versus wait_interactive_timeout.
pub const CAPABILITY_CLIENT_INTERACTIVE: u32 = 0x00000400;

/// Supports SSL.
pub const CAPABILITY_CLIENT_SSL: u32 = 0x00000800;

/// Do not issue SIGPIPE if network failures occur (libmysqlclient only).
pub const CAPABILITY_CLIENT_IGNORE_SIGPIPE: u32 = 0x00001000;

/// Can send status flags in EOF_Packet.
pub const CAPABILITY_CLIENT_TRANSACTIONS: u32 = 0x00002000;

/// Supports Authentication::Native41.
pub const CAPABILITY_CLIENT_SECURE_CONNECTION: u32 = 0x00008000;

/// Can handle multiple statements per COM_QUERY and COM_STMT_PREPARE.
pub const CAPABILITY_CLIENT_MULTI_STATEMENTS: u32 = 0x00010000;

/// Can send multiple resultsets for COM_QUERY.
pub const CAPABILITY_CLIENT_MULTI_RESULTS: u32 = 0x00020000;

/// Can send multiple resultsets for COM_STMT_EXECUTE.
pub const CAPABILITY_CLIENT_PS_MULTI_RESULTS: u32 = 0x00040000;

/// Sends extra data in Initial Handshake Packet and supports the pluggable authentication protocol.
pub const CAPABILITY_CLIENT_PLUGIN_AUTH: u32 = 0x00080000;

/// Permits connection attributes in Protocol::HandshakeResponse41.
pub const CAPABILITY_CLIENT_CONNECT_ATTRS: u32 = 0x00100000;

/// Understands length-encoded integer for auth response data in Protocol::HandshakeResponse41.
pub const CAPABILITY_CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 0x00200000;

/// Announces support for expired password extension.
pub const CAPABILITY_CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS: u32 = 0x00400000;

/// Can set SERVER_SESSION_STATE_CHANGED in the Status Flags and send session-state change data after a OK packet.
pub const CAPABILITY_CLIENT_SESSION_TRACK: u32 = 0x00800000;

/// Can send OK after a Text Resultset.
pub const CAPABILITY_CLIENT_DEPRECATE_EOF: u32 = 0x01000000;

pub const CHARSET_UTF8_GENERAL_CI: u8 = 33;

pub const STATUS_FLAG_AUTOCOMMIT: u16 = 2;

// https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
pub const MYSQL_TYPE_DECIMAL: u8 = 0x00;
pub const MYSQL_TYPE_TINY: u8 = 0x01;
pub const MYSQL_TYPE_SHORT: u8 = 0x02;
pub const MYSQL_TYPE_LONG: u8 = 0x03;
pub const MYSQL_TYPE_FLOAT: u8 = 0x04;
pub const MYSQL_TYPE_DOUBLE: u8 = 0x05;
pub const MYSQL_TYPE_NULL: u8 = 0x06;
pub const MYSQL_TYPE_TIMESTAMP: u8 = 0x07;
pub const MYSQL_TYPE_LONGLONG: u8 = 0x08;
pub const MYSQL_TYPE_INT24: u8 = 0x09;
pub const MYSQL_TYPE_DATE: u8 = 0x0a;
pub const MYSQL_TYPE_TIME: u8 = 0x0b;
pub const MYSQL_TYPE_DATETIME: u8 = 0x0c;
pub const MYSQL_TYPE_YEAR: u8 = 0x0d;
pub const MYSQL_TYPE_VARCHAR: u8 = 0x0f;
pub const MYSQL_TYPE_BIT: u8 = 0x10;
pub const MYSQL_TYPE_NEWDECIMAL: u8 = 0xf6;
pub const MYSQL_TYPE_ENUM: u8 = 0xf7;
pub const MYSQL_TYPE_SET: u8 = 0xf8;
pub const MYSQL_TYPE_TINY_BLOB: u8 = 0xf9;
pub const MYSQL_TYPE_MEDIUM_BLOB: u8 = 0xfa;
pub const MYSQL_TYPE_LONG_BLOB: u8 = 0xfb;
pub const MYSQL_TYPE_BLOB: u8 = 0xfc;
pub const MYSQL_TYPE_VAR_STRING: u8 = 0xfd;
pub const MYSQL_TYPE_STRING: u8 = 0xfe;
pub const MYSQL_TYPE_GEOMETRY: u8 = 0xff;

pub struct MyError<'a> {
    pub code: u16,
    pub msg: &'a str,
    pub sql_state: &'a str,
}

//https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
pub const MYSQL_ER_UNKNOWN_COM_ERROR: MyError<'static> = MyError {
    code: 1047,
    msg: "Unknown command",
    sql_state: "08S01",
};

pub const MYSQL_ER_BAD_DB_ERROR: MyError<'static> = MyError {
    code: 1049,
    msg: "Unknown database",
    sql_state: "42000",
};

pub const MYSQL_ER_BAD_FIELD_ERROR: MyError<'static> = MyError {
    code: 1054,
    msg: "Unknown column",
    sql_state: "42S22",
};

pub const MYSQL_ER_PARSE_ERROR: MyError<'static> = MyError {
    code: 1064,
    msg: "Parse Error",
    sql_state: "42000",
};

pub const MYSQL_ER_NO_TABLES_USED: MyError<'static> = MyError {
    code: 1096,
    msg: "No tables used",
    sql_state: "HY000",
};

pub const MYSQL_ER_QUERY_INTERRUPTED: MyError<'static> = MyError {
    code: 1317,
    msg: "Query execution was interrupted",
    sql_state: "70100",
};
