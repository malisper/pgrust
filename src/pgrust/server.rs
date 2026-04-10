pub use crate::backend::tcop::postgres::serve;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::libpq::pqcomm::{cstr_from_bytes, read_i16_bytes, read_i32_bytes};
    use crate::backend::tcop::postgres::{PROTOCOL_VERSION_3_0, handle_connection};
    use crate::pgrust::database::Database;
    use std::io::{Read, Write};
    use std::net::{Shutdown, TcpListener, TcpStream};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;
    use std::time::Duration;

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("pgrust_server_test_{label}_{id}"));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn start_test_connection() -> (TcpStream, thread::JoinHandle<()>) {
        let db = Database::open(temp_dir("wire_copy"), 16).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            handle_connection(stream, &db, 1).unwrap();
        });

        let stream = TcpStream::connect(addr).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        (stream, server)
    }

    fn send_startup(stream: &mut TcpStream) {
        let mut body = Vec::new();
        body.extend_from_slice(&PROTOCOL_VERSION_3_0.to_be_bytes());
        body.extend_from_slice(b"user\0postgres\0database\0postgres\0\0");
        stream.write_all(&((body.len() + 4) as i32).to_be_bytes()).unwrap();
        stream.write_all(&body).unwrap();
        stream.flush().unwrap();
    }

    fn send_typed_message(stream: &mut TcpStream, kind: u8, body: &[u8]) {
        stream.write_all(&[kind]).unwrap();
        stream.write_all(&((body.len() + 4) as i32).to_be_bytes()).unwrap();
        stream.write_all(body).unwrap();
        stream.flush().unwrap();
    }

    fn send_query(stream: &mut TcpStream, sql: &str) {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        send_typed_message(stream, b'Q', &body);
    }

    fn send_copy_data(stream: &mut TcpStream, data: &[u8]) {
        send_typed_message(stream, b'd', data);
    }

    fn send_copy_done(stream: &mut TcpStream) {
        send_typed_message(stream, b'c', &[]);
    }

    fn read_message(stream: &mut TcpStream, label: &str) -> (u8, Vec<u8>) {
        let mut kind = [0u8; 1];
        stream.read_exact(&mut kind).unwrap_or_else(|e| panic!("{label}: failed reading kind: {e}"));
        let mut len = [0u8; 4];
        stream.read_exact(&mut len).unwrap_or_else(|e| panic!("{label}: failed reading length: {e}"));
        let len = i32::from_be_bytes(len) as usize;
        let mut body = vec![0u8; len - 4];
        stream.read_exact(&mut body).unwrap_or_else(|e| panic!("{label}: failed reading body for message '{}' len {len}: {e}", kind[0] as char));
        (kind[0], body)
    }

    fn read_until_ready(stream: &mut TcpStream, label: &str) -> Vec<(u8, Vec<u8>)> {
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
                values.push(Some(String::from_utf8_lossy(&body[offset..end]).into_owned()));
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

    #[test]
    fn copy_from_stdin_round_trips_over_wire_protocol() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let startup = read_until_ready(&mut stream, "startup");
        assert!(startup.iter().any(|(kind, _)| *kind == b'R'));
        assert!(matches!(startup.last(), Some((b'Z', _))));
        send_query(&mut stream, "create table t (id int, name text)");
        let create = read_until_ready(&mut stream, "create");
        assert_eq!(create.iter().find(|(kind, _)| *kind == b'C').map(|(_, body)| command_tag(body)), Some("CREATE TABLE".to_string()));
        send_query(&mut stream, "copy t from stdin");
        let copy_start = read_message(&mut stream, "copy_start");
        assert_eq!(copy_start.0, b'G');
        send_copy_data(&mut stream, b"1\talice\n");
        send_copy_done(&mut stream);
        let copy_finish = read_until_ready(&mut stream, "copy_finish");
        assert_eq!(copy_finish.iter().find(|(kind, _)| *kind == b'C').map(|(_, body)| command_tag(body)), Some("COPY".to_string()));
        send_query(&mut stream, "select id, name from t");
        let select = read_until_ready(&mut stream, "select");
        let rows = select.iter().filter(|(kind, _)| *kind == b'D').map(|(_, body)| data_row_values(body)).collect::<Vec<_>>();
        assert_eq!(rows, vec![vec![Some("1".into()), Some("alice".into())]]);
        assert_eq!(select.iter().find(|(kind, _)| *kind == b'C').map(|(_, body)| command_tag(body)), Some("SELECT 1".to_string()));
        stream.shutdown(Shutdown::Both).unwrap();
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
        assert_eq!(copy_finish.iter().find(|(kind, _)| *kind == b'C').map(|(_, body)| command_tag(body)), Some("COPY".to_string()));
        send_query(&mut stream, "select id, name from t");
        let select = read_until_ready(&mut stream, "select");
        let rows = select.iter().filter(|(kind, _)| *kind == b'D').map(|(_, body)| data_row_values(body)).collect::<Vec<_>>();
        assert_eq!(rows, vec![vec![Some("1".into()), Some("alice".into())]]);
        stream.shutdown(Shutdown::Both).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn row_description_reports_varchar_typmod() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let _ = read_until_ready(&mut stream, "startup");
        send_query(&mut stream, "select 'foo'::varchar(4)");
        let response = read_until_ready(&mut stream, "select_varchar");
        let fields = response.iter().find(|(kind, _)| *kind == b'T').map(|(_, body)| row_description_fields(body)).unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].1, 1043);
        assert_eq!(fields[0].2, -1);
        assert_eq!(fields[0].3, 8);
        stream.shutdown(Shutdown::Both).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn simple_query_protocol_supports_correlated_scalar_subqueries() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let _ = read_until_ready(&mut stream, "startup");
        send_query(&mut stream, "create table people (id int4 not null, name text)");
        let _ = read_until_ready(&mut stream, "create_people");
        send_query(&mut stream, "create table pets (id int4 not null, owner_id int4, name text)");
        let _ = read_until_ready(&mut stream, "create_pets");
        send_query(&mut stream, "insert into people (id, name) values (1, 'alice'), (2, 'bob'), (3, 'carol')");
        let _ = read_until_ready(&mut stream, "insert_people");
        send_query(&mut stream, "insert into pets (id, owner_id, name) values (10, 1, 'mocha'), (11, 1, 'pixel'), (12, 2, 'otis')");
        let _ = read_until_ready(&mut stream, "insert_pets");
        send_query(&mut stream, "select p.id, (select count(*) from pets q where q.owner_id = p.id) from people p order by p.id");
        let response = read_until_ready(&mut stream, "correlated_select");
        let rows = response.iter().filter(|(kind, _)| *kind == b'D').map(|(_, body)| data_row_values(body)).collect::<Vec<_>>();
        assert_eq!(rows, vec![vec![Some("1".into()), Some("2".into())], vec![Some("2".into()), Some("1".into())], vec![Some("3".into()), Some("0".into())]]);
        stream.shutdown(Shutdown::Both).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn row_description_reports_scalar_and_exists_subquery_types() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let _ = read_until_ready(&mut stream, "startup");
        send_query(&mut stream, "select (select 1), exists (select 1), (select 'x'::text)");
        let response = read_until_ready(&mut stream, "subquery_row_description");
        let fields = response.iter().find(|(kind, _)| *kind == b'T').map(|(_, body)| row_description_fields(body)).unwrap();
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
        let fields = response.iter().find(|(kind, _)| *kind == b'T').map(|(_, body)| row_description_fields(body)).unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].1, 1007);
        assert_eq!(fields[0].2, -1);
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
        let fields = response.iter().find(|(kind, _)| *kind == b'T').map(|(_, body)| row_description_fields(body)).unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].1, 1015);
        stream.shutdown(Shutdown::Both).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn simple_query_protocol_renders_array_text_values() {
        let (mut stream, server) = start_test_connection();
        send_startup(&mut stream);
        let _ = read_until_ready(&mut stream, "startup");
        send_query(&mut stream, "select ARRAY['a,b', 'c']::varchar[], ARRAY[1, null, 3]::int4[]");
        let response = read_until_ready(&mut stream, "array_data_row");
        let rows = response.iter().filter(|(kind, _)| *kind == b'D').map(|(_, body)| data_row_values(body)).collect::<Vec<_>>();
        assert_eq!(rows, vec![vec![Some("{\"a,b\",\"c\"}".into()), Some("{1,NULL,3}".into())]]);
        stream.shutdown(Shutdown::Both).unwrap();
        server.join().unwrap();
    }
}
