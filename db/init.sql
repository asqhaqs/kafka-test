-- sqlite3 /data/post.db

CREATE TABLE IF NOT EXISTS t_position(kind text, type text PRIMARY KEY NOT NULL, file_name text, position integer);

INSERT INTO t_position(kind, type, file_name, position) VALUES('event', 'event', '', 0);
INSERT INTO t_position(kind, type, file_name, position) VALUES('assert', 'assert', '', 0);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'http', '', 0);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'mail', '', 0);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'ftp', '', 0);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'telnet', '', 0);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'dns', '', 0);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'icmp', '', 0);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'ssl_tls_vpn', '', 0);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'ldap', '', 0);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'database', '', 0);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'login', '', 0);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'tcp', '', 0);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'udp', '', 0);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'modbus', '', 0);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 's7', '', 0);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'opc', '', 0);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'goose', '', 0);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'mqtt', '', 0);
INSERT INTO t_position(kind, type, file_name, position) VALUES('metadata', 'coap', '', 0);