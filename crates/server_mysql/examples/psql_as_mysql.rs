//! This example implements a (simple) proxy that allows connecting to PostgreSQL as though it were
//! a MySQL database. To try this out, start a PostgreSQL database at localhost:5432, and then run
//! this example. Notice that `main` does *not* use PostgreSQL bindings, just MySQL ones!

extern crate mysql;
extern crate postgres;
extern crate server_mysql;
extern crate slab;

use mysql::prelude::*;
use server_mysql::*;
use slab::Slab;

use std::io;
use std::net;
use std::thread;

fn main() {
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let jh = thread::spawn(move || {
        if let Ok((s, _)) = listener.accept() {
            let client = postgres::Client::connect(
                "postgresql://postgres:123456@localhost:5432",
                postgres::NoTls,
            )
            .unwrap();
            MysqlIntermediary::run_on_tcp(Postgres::new(client), s).unwrap();
        }
    });

    // we connect using MySQL bindings, but no MySQL server is running!
    let mut db =
        mysql::Conn::new(&format!("mysql://root:123456@127.0.0.1:{}", port)).unwrap();
    assert_eq!(db.ping(), true);
    {
        let mut results = db
            .query_iter("SELECT INT4(1) AS foo, INT8(1) AS bar, 'BAZ' AS baz")
            .unwrap();
        {
            let cols = results.columns();
            let cols = cols.as_ref();
            assert_eq!(cols.len(), 3);
            assert_eq!(cols[0].name_str(), "foo");
            assert_eq!(cols[1].name_str(), "bar");
            assert_eq!(cols[2].name_str(), "baz");
        }
        let one = results.next();
        assert!(one.is_some());
        if let Some(row) = one {
            let row = row.unwrap();
            assert_eq!(row.len(), 3);
            let one = row.get::<i32, _>(0).unwrap();
            assert_eq!(one, 1);
            let one = row.get::<i64, _>(1).unwrap();
            assert_eq!(one, 1);
            let s = row.get::<Vec<u8>, _>(2).unwrap();
            assert_eq!(s, b"BAZ");
        }
        assert_eq!(results.count(), 0);
    }
    {
        let mut results = db
            .exec_iter("SELECT INT4(1) AS foo, INT8(1) AS bar, 'BAZ' AS baz", ())
            .unwrap();
        {
            let cols = results.columns();
            let cols = cols.as_ref();
            assert_eq!(cols.len(), 3);
            assert_eq!(cols[0].name_str(), "foo");
            assert_eq!(cols[1].name_str(), "bar");
            assert_eq!(cols[2].name_str(), "baz");
        }
        let one = results.next();
        assert!(one.is_some());
        if let Some(row) = one {
            let row = row.unwrap();
            assert_eq!(row.len(), 3);
            let one = row.get::<i32, _>(0).unwrap();
            assert_eq!(one, 1);
            let one = row.get::<i64, _>(1).unwrap();
            assert_eq!(one, 1);
            let s = row.get::<Vec<u8>, _>(2).unwrap();
            assert_eq!(s, b"BAZ");
        }
        assert_eq!(results.count(), 0);
    }
    drop(db);
    jh.join().unwrap();
}

// this is where the proxy server implementation starts

struct Prepared {
    stmt: postgres::Statement,
    params: Vec<Column>,
}

struct Postgres {
    client: postgres::Client,
    // NOTE: not *actually* static, but tied to our connection's lifetime.
    prepared: Slab<Prepared>,
}

impl Postgres {
    fn new(client: postgres::Client) -> Self {
        Postgres {
            client,
            prepared: Slab::new(),
        }
    }
}

impl<W: io::Write> MysqlShim<W> for Postgres {
    type Error = Box<dyn std::error::Error>;

    fn on_prepare(
        &mut self,
        query: &str,
        info: StatementMetaWriter<W>,
    ) -> Result<(), Self::Error> {
        match self.client.prepare(query) {
            Ok(stmt) => {
                // the PostgreSQL server will tell us about the parameter types and output columns
                // of the query we just prepared. we now need to communicate this back to our MySQL
                // client, which requires translating between psql and mysql types.
                use std::mem;
                let params: Vec<_> = stmt
                    .params()
                    .into_iter()
                    .map(|t| {
                        let ct = p2mt(t);
                        Column {
                            table: t.schema().to_owned(),
                            column: t.name().to_owned(),
                            coltype: ct,
                            colflags: ColumnFlags::empty(),
                        }
                    })
                    .collect();
                let columns: Vec<_> = stmt
                    .columns()
                    .into_iter()
                    .map(|c| {
                        let t = c.type_();
                        let ct = p2mt(t);
                        Column {
                            table: t.schema().to_owned(),
                            column: c.name().to_owned(),
                            coltype: ct,
                            colflags: ColumnFlags::empty(),
                        }
                    })
                    .collect();

                // keep track of the parameter types so we can decode the values provided by the
                // client when they later execute this statement.
                let stmt = Prepared { stmt, params };

                // the statement is tied to the connection, which as far as the compiler is aware
                // we only know lives for as long as the `&mut self` given to this function.
                // however, *we* know that the connection will live at least as long as the
                // prepared statement we insert into `self.prepared` (because there is no way to
                // get the prepared statements out!).
                let stmt = unsafe { mem::transmute(stmt) };

                let id = self.prepared.insert(stmt);
                let stmt = &self.prepared[id];
                info.reply(id as u32, &stmt.params, &columns)?;
                Ok(())
            }
            Err(e) => {
                if let Some(err) = e.as_db_error() {
                    info.error(ErrorKind::ER_NO, err.message().as_bytes())?;
                    return Ok(());
                }

                Err(e.into())
            }
        }
    }

    fn on_execute(
        &mut self,
        id: u32,
        ps: ParamParser,
        results: QueryResultWriter<W>,
    ) -> Result<(), Self::Error> {
        match self.prepared.get_mut(id as usize) {
            None => Ok(results.error(ErrorKind::ER_NO, b"no such prepared statement")?),
            Some(&mut Prepared { ref mut stmt, .. }) => {
                // this is a little nasty because we have to take MySQL-encoded arguments and
                // massage them into &ToSql things, which is what postgres::Statement::query takes.
                // we can only do that by first boxing all the values (so they can be kept in a
                // single vec), and then collecting a *second* vec with references to those, and
                // *then* take a slice of that vec.
                let args: Vec<Box<dyn postgres::types::ToSql + Sync>> = ps
                    .into_iter()
                    .map(|p| match p.coltype {
                        ColumnType::MYSQL_TYPE_SHORT => {
                            Box::new(Into::<i16>::into(p.value)) as Box<_>
                        }
                        ColumnType::MYSQL_TYPE_LONG => {
                            Box::new(Into::<i32>::into(p.value)) as Box<_>
                        }
                        ColumnType::MYSQL_TYPE_LONGLONG => {
                            Box::new(Into::<i64>::into(p.value)) as Box<_>
                        }
                        ColumnType::MYSQL_TYPE_FLOAT => {
                            Box::new(Into::<f32>::into(p.value)) as Box<_>
                        }
                        ColumnType::MYSQL_TYPE_DOUBLE => {
                            Box::new(Into::<f64>::into(p.value)) as Box<_>
                        }
                        ColumnType::MYSQL_TYPE_STRING => {
                            Box::new(Into::<&str>::into(p.value)) as Box<_>
                        }
                        ct => unimplemented!(
                            "don't know how to translate PostgreSQL \
                             argument type {:?} into MySQL value",
                            ct
                        ),
                    })
                    .collect();
                let args: Vec<_> = args.iter().map(|a| &**a).collect();

                // lazy_query unfortunately gets us into all sorts of lifetime trouble it seems...
                // so we do it eagerly instead.
                Ok(answer_rows(results, self.client.query(stmt, &args[..])?)?)
            }
        }
    }

    fn on_close(&mut self, id: u32) {
        self.prepared.remove(id as usize);
    }

    fn on_query(
        &mut self,
        query: &str,
        results: QueryResultWriter<W>,
    ) -> Result<(), Self::Error> {
        Ok(answer_rows(results, self.client.query(query, &[])?)?)
    }
}

impl Drop for Postgres {
    fn drop(&mut self) {
        // drop all the prepared statements *first*.
        self.prepared.clear();
        // *then* we can drop the connection (implicitly done).
    }
}

/// Take a set of rows from PostgreSQL and re-encode them as MySQL rows
fn answer_rows<W: io::Write>(
    results: QueryResultWriter<W>,
    rows: Vec<postgres::row::Row>,
) -> Result<(), Box<dyn std::error::Error>> {
    assert!(rows.len() > 0);
    let cols: Vec<_> = rows[0]
        .columns()
        .into_iter()
        .map(|c| {
            let t = c.type_();
            let ct = p2mt(t);
            Column {
                table: t.schema().to_owned(),
                column: c.name().to_owned(),
                coltype: ct,
                colflags: ColumnFlags::empty(),
            }
        })
        .collect();

    let mut writer = results.start(&cols)?;
    for row in &rows {
        for (c, col) in cols.iter().enumerate() {
            match col.coltype {
                ColumnType::MYSQL_TYPE_SHORT => writer.write_col(row.get::<_, i16>(c))?,
                ColumnType::MYSQL_TYPE_LONG => writer.write_col(row.get::<_, i32>(c))?,
                ColumnType::MYSQL_TYPE_LONGLONG => {
                    writer.write_col(row.get::<_, i64>(c))?
                }
                ColumnType::MYSQL_TYPE_FLOAT => writer.write_col(row.get::<_, f32>(c))?,
                ColumnType::MYSQL_TYPE_DOUBLE => {
                    writer.write_col(row.get::<_, f64>(c))?
                }
                ColumnType::MYSQL_TYPE_STRING => {
                    writer.write_col(row.get::<_, String>(c))?
                }
                ct => unimplemented!(
                    "don't know how to translate PostgreSQL \
                         argument type {:?} into MySQL value",
                    ct
                ),
            }
        }
        writer.end_row()?;
    }
    writer.finish()?;
    Ok(())
}

/// Convert a PostgreSQL data type and translate it into the corresponding MySQL type
fn p2mt(t: &postgres::types::Type) -> server_mysql::ColumnType {
    if let postgres::types::Kind::Simple = *t.kind() {
        match postgres::types::Type::from_oid(t.oid()) {
            Some(postgres::types::Type::INT2) => {
                server_mysql::ColumnType::MYSQL_TYPE_SHORT
            }
            Some(postgres::types::Type::INT4) => {
                server_mysql::ColumnType::MYSQL_TYPE_LONG
            }
            Some(postgres::types::Type::INT8) => {
                server_mysql::ColumnType::MYSQL_TYPE_LONGLONG
            }
            Some(postgres::types::Type::FLOAT4) => {
                server_mysql::ColumnType::MYSQL_TYPE_FLOAT
            }
            Some(postgres::types::Type::FLOAT8) => {
                server_mysql::ColumnType::MYSQL_TYPE_DOUBLE
            }
            Some(postgres::types::Type::TEXT) => {
                server_mysql::ColumnType::MYSQL_TYPE_STRING
            }
            t => {
                unimplemented!(
                    "don't know how to translate PostgreSQL type {:?} to a MySQL type",
                    t
                );
            }
        }
    } else {
        unimplemented!();
    }
}
