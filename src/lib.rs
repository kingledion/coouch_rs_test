//! A set of helper functions for executing tests with couch_rs library
//! 
//! Allows easy execution of tests by providing automated creation and destruction of test databases in a 
//! CouchDB instance. For a given database name, the created databases append a random string to guarantee
//! uniqueness of multiple tests in parallel in the same CouchDB instance. 
//! 
//! In a 'data layer' application model, there is a discrete structure that is used to represent the data
//! layer of an application. When using this package, that data layer is a CouchDB instance. Advanced 
//! functionality can be built into the data layer by implementing methods on the data layer struct. A 
//! example use is defining a commonly used but complex query as a method. 
//! 
//! If the data layer application model is used, a helper trait [CouchDBWrapper] can be applied to the data 
//! layer struct; this struct would wraps a [couch_rs::Client](https://docs.rs/couch_rs/latest/couch_rs/struct.Client.html) 
//! and the name of a database to use. For testing, 
//! implementing the [CouchDBWrapper] trait allows it to be wrapped in the [TestRepo] class
//! to provide automatic creationg and destruction of tables during integration tests. 
//! 
//! The struct against which [CouchDBWrapper] is implemented should only refer to a single CouchDB database.
//! Even though the primary interface is the [couch_rs::Client](https://docs.rs/couch_rs/latest/couch_rs/struct.Client.html), 
//! and not the couch_rs database, the automatic 
//! creation and destruction implies that only one underlying database is managed by one struct. 

#![warn(missing_docs)]

use couch_rs::{document::TypedCouchDocument, error::CouchError, Client};
use rand::{distributions::Alphanumeric, Rng};
use tokio_util::sync::CancellationToken;

/// A wrapper for a struct that manages on CouchDB database
/// 
/// This trait should be implemented on a struct that manages a CouchDB database, thus enabling that 
/// struct to be used in [TestRepo]. 
pub trait CouchDBWrapper {
    /// Create a new instance of the underlying struct, by passing the sepcifications for the couch_rs
    /// client and the name of the database.
    fn wrap(uri: &str, username: &str, password: &str, db_name: &str) -> Self;
    /// Return the underlying client
    fn client(&self) -> &Client;
    /// Return the underlying database name
    fn dbname(&self) -> &str;
}

/// Configuration for [TestRepo]. This configuration is also passed to the underlying struct on which 
/// [CouchDBWrapper] is implemented, in order to create a new [couch_rs::Client](https://docs.rs/couch_rs/latest/couch_rs/struct.Client.html) 
/// and identify the associated database. 
#[derive(Clone)]
pub struct TestRepoConfig {
    uri: String,
    username: String,
    password: String,
    db_name: String,
}

impl TestRepoConfig {
    /// Create a new configuration; identifying the uri, username and password for the CouchDB instance as
    /// well as the database name.
    pub fn new(uri: &str, uname: &str, pwd: &str, dbname: &str) -> TestRepoConfig {
        TestRepoConfig {
            uri: uri.to_string(),
            username: uname.to_string(),
            password: pwd.to_string(),
            db_name: dbname.to_string(),
        }
    }

    fn with_name(self, db_unique_name: String) -> TestRepoConfig {
        TestRepoConfig {
            db_name: db_unique_name,
            ..self
        }
    }
}

/// A wrapper for a struct that encapsulates functionality of an application's data layer. 
/// 
/// The type parameter is the type of the data layer struct to be wrapped. The data layer struct must 
/// implement the [CouchDBWrapper] trait. 
/// 
/// Creation of a new instance of this struct will create a unique, associated CouchDB database. Internally,
/// this struct implements a drop token and watcher to determine when this struct is de-allocated, thus
/// triggering destruction of the associated CouchDB database. 
pub struct TestRepo<T: CouchDBWrapper> {
    /// A struct that encapsulates the functionality of an application's data layer. Test using 
    /// this wrapper should call for this struct in order to perform test actions against this 
    /// TestRepo's ephemeral CouchDB database. 
    pub repo: T,

    drop_token: CancellationToken,
    dropped_token: CancellationToken,
}

impl<T: CouchDBWrapper> TestRepo<T> {
    /// Creates a new instance of TestRepo wrapping a new instance of the type paramter (an implementation
    /// of [CouchDBWrapper]). This function will create a new [couch_rs::Client](https://docs.rs/couch_rs/latest/couch_rs/struct.Client.html)
    /// from the parameters passed
    /// as part of the [TestRepoConfig] argument. It will then create a new database in CouchDB using the 
    /// client connection and a database name consisting of the name defined in config plus a random suffix.
    /// This randomization of database names helps prevent collisions during parallel test runs against 
    /// the same CouchDB instance. 
    /// 
    /// This function also creates a drop token and watcher to determine when this instance is de-allocated
    /// The watcher spawn an asynchronous thread that will observe the drop token every 100 milliseconds.
    /// when this instance is deallocated, the drop token is destroyed and the watcher will trigger the
    /// destruction of the database instance created by this method. 
    pub async fn new(arg_cfg: TestRepoConfig) -> TestRepo<T> {
        // create random identifier for database and append to db name
        let test_identifier = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(12)
            .map(char::from)
            .collect::<String>()
            .to_lowercase();

        let db_unique_name = format!("{}-{}", arg_cfg.db_name, test_identifier);
        let cfg = arg_cfg.with_name(db_unique_name);

        let wrapped = T::wrap(&cfg.uri, &cfg.username, &cfg.password, &cfg.db_name);

        let drop_token = CancellationToken::new();
        let dropped_token = TestRepo::<T>::start_drop_watcher(&drop_token, cfg.clone()).await;

        // connect to database and return wrapping repository
        log::info!("Creating database {} for testing", cfg.db_name);

        // create test database - panic on fail
        let c = wrapped.client();
        match c.make_db(&cfg.db_name).await {
            Ok(_) => {}
            Err(e) => {
                match e.status() {
                    Some(code) => {
                        match code {
                            // database already exists; this should not happen,
                            // requires manual cleanup
                            http::status::StatusCode::PRECONDITION_FAILED => {
                                panic!(
                                    "Database {} already exists and must be manually removed.",
                                    cfg.db_name
                                )
                            }
                            _ => panic!("Error while creating new database: {}", e),
                        }
                    }
                    None => panic!("Error while creating new database: {}", e),
                }
            }
        };

        TestRepo {
            repo: wrapped,
            drop_token: drop_token,
            dropped_token: dropped_token,
        }
    }

    /// Pushes data to the unique database associated with this instance. Data is pushed via the 
    /// [couch_rs::database::bulk_docs](https://docs.rs/couch_rs/latest/couch_rs/database/struct.Database.html#method.bulk_docs)
    /// method. 
    pub async fn with_data<S: TypedCouchDocument>(
        &self,
        data: &mut [S],
    ) -> Result<usize, CouchError> {
        let db = self.repo.client().db(&self.repo.dbname()).await?;
        let result = db.bulk_docs(data).await?;
        return Ok(result.len());
    }

    async fn start_drop_watcher(
        drop_token: &CancellationToken,
        cfg: TestRepoConfig,
    ) -> CancellationToken {
        let drop_child = drop_token.child_token();

        let dropped_token = CancellationToken::new();
        let dropped_child = dropped_token.child_token();

        tokio::spawn(async move {
            while !drop_child.is_cancelled() {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }

            TestRepo::<T>::drop(cfg).await;

            dropped_token.cancel();
        });

        dropped_child
    }

    async fn drop(cfg: TestRepoConfig) {
        // delete test db - panic on fail
        let c = couch_rs::Client::new(&cfg.uri, &cfg.username, &cfg.password).unwrap();

        match c.destroy_db(&cfg.db_name).await {
            Ok(b) => match b {
                true => log::info!("Cleaned up database {}", cfg.db_name),
                false => log::info!("Failed to clean up database {}", cfg.db_name),
            },

            Err(e) => log::error!("Error while cleaning up {}: {}", cfg.db_name, e),
        };
    }

}

impl<T: CouchDBWrapper> Drop for TestRepo<T> {
    fn drop(&mut self) {
        self.drop_token.cancel();

        while !self.dropped_token.is_cancelled() {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
}
