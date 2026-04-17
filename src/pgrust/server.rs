pub use crate::backend::tcop::postgres::serve;

#[cfg(test)]
mod tests {
    use crate::backend::libpq::pqcomm::{cstr_from_bytes, read_i16_bytes, read_i32_bytes};
    use crate::backend::tcop::postgres::PROTOCOL_VERSION_3_0;
    #[cfg(not(unix))]
    use crate::backend::tcop::postgres::handle_connection;
    #[cfg(unix)]
    use crate::backend::tcop::postgres::handle_connection_with_io;
    use crate::pgrust::cluster::Cluster;
    use crate::pgrust::database::Session;
    use std::io::{Read, Write};
    use std::net::Shutdown;
    #[cfg(not(unix))]
    use std::net::{TcpListener, TcpStream};
    #[cfg(unix)]
    use std::os::unix::net::UnixStream;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;
    use std::time::Duration;

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);
    #[cfg(unix)]
    type TestStream = UnixStream;
    #[cfg(not(unix))]
    type TestStream = TcpStream;

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("pgrust_server_test_{label}_{id}"));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[cfg(unix)]
    fn start_test_connection() -> (TestStream, thread::JoinHandle<()>) {
        let cluster = Cluster::open(temp_dir("wire_copy"), 16).unwrap();
        start_test_connection_with_cluster(cluster)
    }

    #[cfg(unix)]
    fn start_test_connection_with_cluster(
        cluster: Cluster,
    ) -> (TestStream, thread::JoinHandle<()>) {
        let (server_stream, client_stream) = UnixStream::pair().unwrap();

        let server = thread::spawn(move || {
            let reader = server_stream.try_clone().unwrap();
            handle_connection_with_io(reader, server_stream, &cluster, 1).unwrap();
        });

        client_stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        (client_stream, server)
    }

    #[cfg(not(unix))]
    fn start_test_connection() -> (TestStream, thread::JoinHandle<()>) {
        let cluster = Cluster::open(temp_dir("wire_copy"), 16).unwrap();
        start_test_connection_with_cluster(cluster)
    }

    #[cfg(not(unix))]
    fn start_test_connection_with_cluster(
        cluster: Cluster,
    ) -> (TestStream, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            handle_connection(stream, &cluster, 1).unwrap();
        });

        let stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        (stream, server)
    }

    fn send_startup(stream: &mut impl Write) {
        send_startup_params(stream, &[("user", "postgres"), ("database", "postgres")]);
    }

    fn send_startup_params(stream: &mut impl Write, params: &[(&str, &str)]) {
        let mut body = Vec::new();
        body.extend_from_slice(&PROTOCOL_VERSION_3_0.to_be_bytes());
        for (key, value) in params {
            body.extend_from_slice(key.as_bytes());
            body.push(0);
            body.extend_from_slice(value.as_bytes());
            body.push(0);
        }
        body.push(0);
        stream
            .write_all(&((body.len() + 4) as i32).to_be_bytes())
            .unwrap();
        stream.write_all(&body).unwrap();
        stream.flush().unwrap();
    }

    fn send_typed_message(stream: &mut impl Write, kind: u8, body: &[u8]) {
        stream.write_all(&[kind]).unwrap();
        stream
            .write_all(&((body.len() + 4) as i32).to_be_bytes())
            .unwrap();
        stream.write_all(body).unwrap();
        stream.flush().unwrap();
    }

    fn send_query(stream: &mut impl Write, sql: &str) {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        send_typed_message(stream, b'Q', &body);
    }

    fn send_parse(stream: &mut impl Write, statement_name: &str, sql: &str) {
        let mut body = Vec::new();
        body.extend_from_slice(statement_name.as_bytes());
        body.push(0);
        body.extend_from_slice(sql.as_bytes());
        body.push(0);
        body.extend_from_slice(&0i16.to_be_bytes());
        send_typed_message(stream, b'P', &body);
    }

    fn send_bind(stream: &mut impl Write, portal_name: &str, statement_name: &str) {
        let mut body = Vec::new();
        body.extend_from_slice(portal_name.as_bytes());
        body.push(0);
        body.extend_from_slice(statement_name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0i16.to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());
        send_typed_message(stream, b'B', &body);
    }

    fn send_execute(stream: &mut impl Write, portal_name: &str) {
        let mut body = Vec::new();
        body.extend_from_slice(portal_name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0i32.to_be_bytes());
        send_typed_message(stream, b'E', &body);
    }

    fn send_sync(stream: &mut impl Write) {
        send_typed_message(stream, b'S', &[]);
    }

    fn send_copy_data(stream: &mut impl Write, data: &[u8]) {
        send_typed_message(stream, b'd', data);
    }

    fn send_copy_done(stream: &mut impl Write) {
        send_typed_message(stream, b'c', &[]);
    }

    fn read_message(stream: &mut impl Read, label: &str) -> (u8, Vec<u8>) {
        let mut kind = [0u8; 1];
        stream
            .read_exact(&mut kind)
            .unwrap_or_else(|e| panic!("{label}: failed reading kind: {e}"));
        let mut len = [0u8; 4];
        stream
            .read_exact(&mut len)
            .unwrap_or_else(|e| panic!("{label}: failed reading length: {e}"));
        let len = i32::from_be_bytes(len) as usize;
        let mut body = vec![0u8; len - 4];
        stream.read_exact(&mut body).unwrap_or_else(|e| {
            panic!(
                "{label}: failed reading body for message '{}' len {len}: {e}",
                kind[0] as char
            )
        });
        (kind[0], body)
    }

    fn read_until_ready(stream: &mut impl Read, label: &str) -> Vec<(u8, Vec<u8>)> {
        let mut messages = Vec::new();
        loop {
            let msg = read_message(stream, label);
            let done = msg.0 == b'Z';
            messages.push(msg);
            if done {
                return messages;
            }
        }
    }

    fn command_tag(body: &[u8]) -> String {
        cstr_from_bytes(body)
    }

    fn data_row_values(body: &[u8]) -> Vec<Option<String>> {
        let mut offset = 0;
        let ncols = read_i16_bytes(body, &mut offset).unwrap() as usize;
        let mut values = Vec::with_capacity(ncols);
        for _ in 0..ncols {
            let len = read_i32_bytes(body, &mut offset).unwrap();
            if len < 0 {
                values.push(None);
            } else {
                let end = offset + len as usize;
                values.push(Some(
                    String::from_utf8_lossy(&body[offset..end]).into_owned(),
                ));
                offset = end;
            }
        }
        values
    }

    fn row_description_fields(body: &[u8]) -> Vec<(String, i32, i16, i32)> {
        let mut offset = 0;
        let ncols = read_i16_bytes(body, &mut offset).unwrap() as usize;
        let mut fields = Vec::with_capacity(ncols);
        for _ in 0..ncols {
            let name_start = offset;
            while body[offset] != 0 {
                offset += 1;
            }
            let name = String::from_utf8_lossy(&body[name_start..offset]).into_owned();
            offset += 1;
            let _table_oid = read_i32_bytes(body, &mut offset).unwrap();
            let _attr_num = read_i16_bytes(body, &mut offset).unwrap();
            let type_oid = read_i32_bytes(body, &mut offset).unwrap();
            let typlen = read_i16_bytes(body, &mut offset).unwrap();
            let typmod = read_i32_bytes(body, &mut offset).unwrap();
            let _format = read_i16_bytes(body, &mut offset).unwrap();
            fields.push((name, type_oid, typlen, typmod));
        }
        fields
    }

    fn error_fields(body: &[u8]) -> Vec<(u8, String)> {
        let mut fields = Vec::new();
        let mut offset = 0usize;
        while offset < body.len() && body[offset] != 0 {
            let code = body[offset];
            offset += 1;
            let start = offset;
            while body[offset] != 0 {
                offset += 1;
            }
            fields.push((
                code,
                String::from_utf8_lossy(&body[start..offset]).into_owned(),
            ));
            offset += 1;
        }
        fields
    }

    #[test]
    fn copy_from_stdin_round_trips_over_wire_protocol() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let startup = read_until_ready(&mut stream, "startup");
        assert!(startup.iter().any(|(kind, _)| *kind == b'R'));
        assert!(matches!(startup.last(), Some((b'Z', _))));
        send_query(&mut stream, "create table t (id int, name text)");
        let create = read_until_ready(&mut stream, "create");
        assert_eq!(
            create
                .iter()
                .find(|(kind, _)| *kind == b'C')
                .map(|(_, body)| command_tag(body)),
            Some("CREATE TABLE".to_string())
        );
        send_query(&mut stream, "copy t from stdin");
        let copy_start = read_message(&mut stream, "copy_start");
        assert_eq!(copy_start.0, b'G');
        send_copy_data(&mut stream, b"1\talice\n");
        send_copy_done(&mut stream);
        let copy_finish = read_until_ready(&mut stream, "copy_finish");
        assert_eq!(
            copy_finish
                .iter()
                .find(|(kind, _)| *kind == b'C')
                .map(|(_, body)| command_tag(body)),
            Some("COPY".to_string())
        );
        send_query(&mut stream, "select id, name from t");
        let select = read_until_ready(&mut stream, "select");
        let rows = select
            .iter()
            .filter(|(kind, _)| *kind == b'D')
            .map(|(_, body)| data_row_values(body))
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![vec![Some("1".into()), Some("alice".into())]]);
        assert_eq!(
            select
                .iter()
                .find(|(kind, _)| *kind == b'C')
                .map(|(_, body)| command_tag(body)),
            Some("SELECT 1".to_string())
        );
        let _ = stream.shutdown(Shutdown::Both);
        server.join().unwrap();
    }

    #[test]
    fn copy_from_stdin_accepts_legacy_end_marker_before_copy_done() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let _ = read_until_ready(&mut stream, "startup");
        send_query(&mut stream, "create table t (id int, name text)");
        let _ = read_until_ready(&mut stream, "create");
        send_query(&mut stream, "copy t from stdin");
        let copy_start = read_message(&mut stream, "copy_start");
        assert_eq!(copy_start.0, b'G');
        send_copy_data(&mut stream, b"1\talice\n");
        send_copy_data(&mut stream, b"\\.\n");
        send_copy_done(&mut stream);
        let copy_finish = read_until_ready(&mut stream, "copy_finish");
        assert_eq!(
            copy_finish
                .iter()
                .find(|(kind, _)| *kind == b'C')
                .map(|(_, body)| command_tag(body)),
            Some("COPY".to_string())
        );
        send_query(&mut stream, "select id, name from t");
        let select = read_until_ready(&mut stream, "select");
        let rows = select
            .iter()
            .filter(|(kind, _)| *kind == b'D')
            .map(|(_, body)| data_row_values(body))
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![vec![Some("1".into()), Some("alice".into())]]);
        let _ = stream.shutdown(Shutdown::Both);
        server.join().unwrap();
    }

    #[test]
    fn copy_from_stdin_with_column_list_targets_subset_columns() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let _ = read_until_ready(&mut stream, "startup");
        send_query(
            &mut stream,
            "create table width_bucket_test (operand_num numeric, operand_f8 float8)",
        );
        let _ = read_until_ready(&mut stream, "create");
        send_query(
            &mut stream,
            "copy width_bucket_test (operand_num) from stdin",
        );
        let copy_start = read_message(&mut stream, "copy_start");
        assert_eq!(copy_start.0, b'G');
        send_copy_data(&mut stream, b"5.5\n");
        send_copy_done(&mut stream);
        let copy_finish = read_until_ready(&mut stream, "copy_finish");
        assert_eq!(
            copy_finish
                .iter()
                .find(|(kind, _)| *kind == b'C')
                .map(|(_, body)| command_tag(body)),
            Some("COPY".to_string())
        );
        send_query(
            &mut stream,
            "select operand_num, operand_f8 is null from width_bucket_test",
        );
        let select = read_until_ready(&mut stream, "select");
        let rows = select
            .iter()
            .filter(|(kind, _)| *kind == b'D')
            .map(|(_, body)| data_row_values(body))
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![vec![Some("5.5".into()), Some("t".into())]]);
        let _ = stream.shutdown(Shutdown::Both);
        server.join().unwrap();
    }

    #[test]
    fn copy_from_stdin_reports_missing_table_without_dropping_connection() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let _ = read_until_ready(&mut stream, "startup");
        send_query(&mut stream, "copy missing_copy_target from stdin");
        let copy_start = read_message(&mut stream, "copy_start");
        assert_eq!(copy_start.0, b'G');
        send_copy_data(&mut stream, b"1\n");
        send_copy_done(&mut stream);
        let copy_finish = read_until_ready(&mut stream, "copy_finish");
        let error = copy_finish
            .iter()
            .find(|(kind, _)| *kind == b'E')
            .expect("copy should return an error");
        let fields = error_fields(&error.1);
        assert!(fields.iter().any(|(code, value)| {
            *code == b'M' && value.contains("unknown table: missing_copy_target")
        }));
        assert!(matches!(copy_finish.last(), Some((b'Z', _))));

        send_query(&mut stream, "select 1");
        let select = read_until_ready(&mut stream, "select_after_copy_error");
        assert_eq!(
            select
                .iter()
                .find(|(kind, _)| *kind == b'C')
                .map(|(_, body)| command_tag(body)),
            Some("SELECT 1".to_string())
        );
        let _ = stream.shutdown(Shutdown::Both);
        server.join().unwrap();
    }

    #[test]
    fn row_description_reports_varchar_typmod() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let _ = read_until_ready(&mut stream, "startup");
        send_query(&mut stream, "select 'foo'::varchar(4)");
        let response = read_until_ready(&mut stream, "select_varchar");
        let fields = response
            .iter()
            .find(|(kind, _)| *kind == b'T')
            .map(|(_, body)| row_description_fields(body))
            .unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].1, 1043);
        assert_eq!(fields[0].2, -1);
        assert_eq!(fields[0].3, 8);
        let _ = stream.shutdown(Shutdown::Both);
        server.join().unwrap();
    }

    #[test]
    fn simple_query_protocol_supports_correlated_scalar_subqueries() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let _ = read_until_ready(&mut stream, "startup");
        send_query(
            &mut stream,
            "create table people (id int4 not null, name text)",
        );
        let _ = read_until_ready(&mut stream, "create_people");
        send_query(
            &mut stream,
            "create table pets (id int4 not null, owner_id int4, name text)",
        );
        let _ = read_until_ready(&mut stream, "create_pets");
        send_query(
            &mut stream,
            "insert into people (id, name) values (1, 'alice'), (2, 'bob'), (3, 'carol')",
        );
        let _ = read_until_ready(&mut stream, "insert_people");
        send_query(
            &mut stream,
            "insert into pets (id, owner_id, name) values (10, 1, 'mocha'), (11, 1, 'pixel'), (12, 2, 'otis')",
        );
        let _ = read_until_ready(&mut stream, "insert_pets");
        send_query(
            &mut stream,
            "select p.id, (select count(*) from pets q where q.owner_id = p.id) from people p order by p.id",
        );
        let response = read_until_ready(&mut stream, "correlated_select");
        let rows = response
            .iter()
            .filter(|(kind, _)| *kind == b'D')
            .map(|(_, body)| data_row_values(body))
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            vec![
                vec![Some("1".into()), Some("2".into())],
                vec![Some("2".into()), Some("1".into())],
                vec![Some("3".into()), Some("0".into())]
            ]
        );
        let _ = stream.shutdown(Shutdown::Both);
        server.join().unwrap();
    }

    #[test]
    fn row_description_reports_scalar_and_exists_subquery_types() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let _ = read_until_ready(&mut stream, "startup");
        send_query(
            &mut stream,
            "select (select 1), exists (select 1), (select 'x'::text)",
        );
        let response = read_until_ready(&mut stream, "subquery_row_description");
        let fields = response
            .iter()
            .find(|(kind, _)| *kind == b'T')
            .map(|(_, body)| row_description_fields(body))
            .unwrap();
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].1, 23);
        assert_eq!(fields[1].1, 16);
        assert_eq!(fields[2].1, 25);
        stream.shutdown(Shutdown::Both).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn row_description_reports_array_oid() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let _ = read_until_ready(&mut stream, "startup");
        send_query(&mut stream, "select ARRAY[1, 2]::int4[]");
        let response = read_until_ready(&mut stream, "array_row_description");
        let fields = response
            .iter()
            .find(|(kind, _)| *kind == b'T')
            .map(|(_, body)| row_description_fields(body))
            .unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].1, 1007);
        assert_eq!(fields[0].2, -1);
        stream.shutdown(Shutdown::Both).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn row_description_reports_extended_numeric_oids() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let _ = read_until_ready(&mut stream, "startup");
        send_query(
            &mut stream,
            "select '7'::int2, '9'::int8, '1.5'::real, '2.5'::double precision",
        );
        let response = read_until_ready(&mut stream, "extended_numeric_row_description");
        let fields = response
            .iter()
            .find(|(kind, _)| *kind == b'T')
            .map(|(_, body)| row_description_fields(body))
            .unwrap();
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[0].1, 21);
        assert_eq!(fields[1].1, 20);
        assert_eq!(fields[2].1, 700);
        assert_eq!(fields[3].1, 701);
        stream.shutdown(Shutdown::Both).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn row_description_reports_varchar_array_oid() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let _ = read_until_ready(&mut stream, "startup");
        send_query(&mut stream, "select ARRAY['x']::varchar[]");
        let response = read_until_ready(&mut stream, "varchar_array_row_description");
        let fields = response
            .iter()
            .find(|(kind, _)| *kind == b'T')
            .map(|(_, body)| row_description_fields(body))
            .unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].1, 1015);
        stream.shutdown(Shutdown::Both).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn row_description_reports_jsonb_oids() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let _ = read_until_ready(&mut stream, "startup");
        send_query(
            &mut stream,
            "select '{\"a\":1}'::jsonb, ARRAY['{\"a\":1}']::jsonb[]",
        );
        let response = read_until_ready(&mut stream, "jsonb_row_description");
        let fields = response
            .iter()
            .find(|(kind, _)| *kind == b'T')
            .map(|(_, body)| row_description_fields(body))
            .unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].1, 3802);
        assert_eq!(fields[1].1, 3807);
        stream.shutdown(Shutdown::Both).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn row_description_reports_jsonpath_oids() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let _ = read_until_ready(&mut stream, "startup");
        send_query(
            &mut stream,
            "select '$.a'::jsonpath, ARRAY['$.a']::jsonpath[]",
        );
        let response = read_until_ready(&mut stream, "jsonpath_row_description");
        let fields = response
            .iter()
            .find(|(kind, _)| *kind == b'T')
            .map(|(_, body)| row_description_fields(body))
            .unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].1, 4072);
        assert_eq!(fields[1].1, 4073);
        stream.shutdown(Shutdown::Both).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn simple_query_protocol_renders_array_text_values() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let _ = read_until_ready(&mut stream, "startup");
        send_query(
            &mut stream,
            "select ARRAY['a,b', 'c']::varchar[], ARRAY[1, null, 3]::int4[]",
        );
        let response = read_until_ready(&mut stream, "array_data_row");
        let rows = response
            .iter()
            .filter(|(kind, _)| *kind == b'D')
            .map(|(_, body)| data_row_values(body))
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            vec![vec![Some("{\"a,b\",c}".into()), Some("{1,NULL,3}".into())]]
        );
        stream.shutdown(Shutdown::Both).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn simple_query_reports_numeric_sqlstates() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let _ = read_until_ready(&mut stream, "startup");

        send_query(&mut stream, "select 'abc'::numeric");
        let response = read_until_ready(&mut stream, "invalid_numeric");
        let error = response
            .iter()
            .find(|(kind, _)| *kind == b'E')
            .map(|(_, body)| error_fields(body))
            .unwrap();
        assert!(
            error
                .iter()
                .any(|(code, value)| *code == b'C' && value == "22P02")
        );
        assert!(error.iter().any(|(code, value)| *code == b'M'
            && value == "invalid input syntax for type numeric: \"abc\""));

        send_query(&mut stream, "select '1234.56'::numeric(5,2)");
        let response = read_until_ready(&mut stream, "numeric_overflow");
        let error = response
            .iter()
            .find(|(kind, _)| *kind == b'E')
            .map(|(_, body)| error_fields(body))
            .unwrap();
        assert!(
            error
                .iter()
                .any(|(code, value)| *code == b'C' && value == "22003")
        );
        assert!(
            error
                .iter()
                .any(|(code, value)| *code == b'M' && value == "numeric field overflow")
        );

        send_query(&mut stream, "select 1.5::real % 1.0::real");
        let response = read_until_ready(&mut stream, "undefined_operator");
        let error = response
            .iter()
            .find(|(kind, _)| *kind == b'E')
            .map(|(_, body)| error_fields(body))
            .unwrap();
        assert!(
            error
                .iter()
                .any(|(code, value)| *code == b'C' && value == "42883")
        );
        assert!(
            error
                .iter()
                .any(|(code, value)| *code == b'M'
                    && value == "operator does not exist: real % real")
        );

        stream.shutdown(Shutdown::Both).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn simple_query_statement_timeout_returns_57014_and_ready() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let _ = read_until_ready(&mut stream, "startup");

        send_query(&mut stream, "set statement_timeout = '50ms'");
        let _ = read_until_ready(&mut stream, "set_timeout");

        send_query(&mut stream, "select * from generate_series(1, 1000000000)");
        let response = read_until_ready(&mut stream, "statement_timeout_simple");
        let error = response
            .iter()
            .find(|(kind, _)| *kind == b'E')
            .map(|(_, body)| error_fields(body))
            .expect("timeout should return an error");
        assert!(
            error
                .iter()
                .any(|(code, value)| *code == b'C' && value == "57014")
        );
        assert!(error.iter().any(|(code, value)| {
            *code == b'M' && value == "canceling statement due to statement timeout"
        }));
        assert!(matches!(response.last(), Some((b'Z', _))));

        send_query(&mut stream, "select 1");
        let after = read_until_ready(&mut stream, "after_statement_timeout");
        assert_eq!(
            after
                .iter()
                .find(|(kind, _)| *kind == b'C')
                .map(|(_, body)| command_tag(body)),
            Some("SELECT 1".to_string())
        );

        stream.shutdown(Shutdown::Both).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn extended_protocol_execute_statement_timeout_returns_57014() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let _ = read_until_ready(&mut stream, "startup");

        send_query(&mut stream, "set statement_timeout = '50ms'");
        let _ = read_until_ready(&mut stream, "set_timeout");

        send_parse(
            &mut stream,
            "timeout_stmt",
            "select * from generate_series(1, 1000000000)",
        );
        send_bind(&mut stream, "", "timeout_stmt");
        send_execute(&mut stream, "");
        send_sync(&mut stream);

        let response = read_until_ready(&mut stream, "statement_timeout_extended");
        assert!(response.iter().any(|(kind, _)| *kind == b'1'));
        assert!(response.iter().any(|(kind, _)| *kind == b'2'));
        let error = response
            .iter()
            .find(|(kind, _)| *kind == b'E')
            .map(|(_, body)| error_fields(body))
            .expect("timeout should return an error");
        assert!(
            error
                .iter()
                .any(|(code, value)| *code == b'C' && value == "57014")
        );
        assert!(error.iter().any(|(code, value)| {
            *code == b'M' && value == "canceling statement due to statement timeout"
        }));
        assert!(matches!(response.last(), Some((b'Z', _))));

        send_query(&mut stream, "select 1");
        let after = read_until_ready(&mut stream, "after_extended_timeout");
        assert_eq!(
            after
                .iter()
                .find(|(kind, _)| *kind == b'C')
                .map(|(_, body)| command_tag(body)),
            Some("SELECT 1".to_string())
        );

        stream.shutdown(Shutdown::Both).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn startup_packet_uses_requested_database() {
        let cluster = Cluster::open(temp_dir("wire_startup_db"), 16).unwrap();
        let postgres = cluster.connect_database("postgres").unwrap();
        let mut admin = Session::new(1);
        admin
            .execute(&postgres, "create database analytics")
            .unwrap();

        let analytics = cluster.connect_database("analytics").unwrap();
        let mut seed = Session::new(2);
        seed.execute(&analytics, "create table startup_only (id int4)")
            .unwrap();
        seed.execute(&analytics, "insert into startup_only values (9)")
            .unwrap();
        drop(seed);
        drop(analytics);
        drop(admin);
        drop(postgres);

        let (mut stream, server) = start_test_connection_with_cluster(cluster);
        send_startup_params(
            &mut stream,
            &[("user", "postgres"), ("database", "analytics")],
        );
        let startup = read_until_ready(&mut stream, "startup_requested_database");
        assert!(startup.iter().any(|(kind, _)| *kind == b'R'));

        send_query(&mut stream, "select id from startup_only");
        let response = read_until_ready(&mut stream, "startup_only_query");
        let rows = response
            .iter()
            .filter(|(kind, _)| *kind == b'D')
            .map(|(_, body)| data_row_values(body))
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![vec![Some("9".into())]]);

        stream.shutdown(Shutdown::Both).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn startup_packet_defaults_database_to_user_name() {
        let cluster = Cluster::open(temp_dir("wire_startup_user_default"), 16).unwrap();
        let (mut stream, server) = start_test_connection_with_cluster(cluster);
        send_startup_params(&mut stream, &[("user", "postgres")]);
        let startup = read_until_ready(&mut stream, "startup_user_default");
        assert!(startup.iter().any(|(kind, _)| *kind == b'R'));
        assert!(matches!(startup.last(), Some((b'Z', _))));

        stream.shutdown(Shutdown::Both).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn startup_packet_rejects_missing_database() {
        let cluster = Cluster::open(temp_dir("wire_missing_db"), 16).unwrap();
        let (mut stream, server) = start_test_connection_with_cluster(cluster);
        send_startup_params(
            &mut stream,
            &[("user", "postgres"), ("database", "missingdb")],
        );
        let error = read_message(&mut stream, "startup_missing_database");
        assert_eq!(error.0, b'E');
        let fields = error_fields(&error.1);
        assert!(
            fields
                .iter()
                .any(|(code, value)| *code == b'C' && value == "3D000")
        );
        assert!(fields.iter().any(|(code, value)| {
            *code == b'M' && value.contains("database \"missingdb\" does not exist")
        }));

        let _ = stream.shutdown(Shutdown::Both);
        server.join().unwrap();
    }

    #[test]
    fn startup_packet_rejects_template0_connections() {
        let cluster = Cluster::open(temp_dir("wire_template0_db"), 16).unwrap();
        let (mut stream, server) = start_test_connection_with_cluster(cluster);
        send_startup_params(
            &mut stream,
            &[("user", "postgres"), ("database", "template0")],
        );
        let error = read_message(&mut stream, "startup_template0");
        assert_eq!(error.0, b'E');
        let fields = error_fields(&error.1);
        assert!(
            fields
                .iter()
                .any(|(code, value)| *code == b'C' && value == "55000")
        );
        assert!(fields.iter().any(|(code, value)| {
            *code == b'M'
                && value.contains("database \"template0\" is not currently accepting connections")
        }));

        let _ = stream.shutdown(Shutdown::Both);
        server.join().unwrap();
    }
}
