-- sqlite3 /data/post.db

CREATE TABLE IF NOT EXISTS t_position(kind text, type text PRIMARY KEY NOT NULL, file_name text, position integer);

INSERT INTO t_position(kind, type, file_name, position) VALUES('event', 'event', '', -1);
INSERT INTO t_position(kind, type, file_name, position) VALUES('asset', 'asset', '', -1);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'http', '', -1);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'mail', '', -1);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'ftp', '', -1);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'telnet', '', -1);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'dns', '', -1);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'icmp', '', -1);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'ssl_tls_vpn', '', -1);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'ldap', '', -1);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'database', '', -1);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'login', '', -1);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'tcp', '', -1);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'udp', '', -1);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'modbus', '', -1);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 's7', '', -1);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'opc', '', -1);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'goose', '', -1);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'mqtt', '', -1);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'coap', '', -1);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'sample', '', -1);