//! A set of helper functions for executing tests with couch_rs library
//! 
//! Allows easy execution of tests by providing automated creation and destruction of test databases in a 
//! CouchDB instance. For a given database name, the created databases append a random string to guarantee
//! uniqueness of multiple tests in parallel in the same CouchDB instance. 
//! 
//! ```rust
//! use serde_json::json;
//! use couch_rs_test::{TestRepo, TestRepoConfig};
//! 
//! async fn new_database(
//!     config_uri: &str,
//!     config_username: &str,
//!     config_password: &str,
//!     config_dbname: &str,
//! ) -> TestRepo {
//! 
//!     let repo: TestRepo = match TestRepo::new(
//!         TestRepoConfig::new(
//!             config_uri,
//!             config_username,
//!             config_password,
//!             config_dbname,
//!         )
//!     ).await {
//!         Ok(r) => r,
//!         // if db creation fails, test will fail, so just panic
//!         Err(e) => panic!("Failed to create test database: {}", e), 
//!     };
//! 
//!     // write test data into the newly created database
//!     let data = &mut vec!{
//!         json!{{"some": "data"}},
//!         json!{{"some": "other-data"}}
//!     };
//! 
//!     match repo.with_data(data).await {
//!         Ok(cnt) => log::info!("Added {} entries to test database {}", cnt, repo.db.name()),
//!         Err(e) => panic!("Failed to set up database: {}", e),
//!     };
//!     repo
//! }
//! ```

#![warn(missing_docs)]

use std::error::Error;
use couch_rs::{database::Database, document::TypedCouchDocument, error::CouchError, Client};
use rand::{distributions::Alphanumeric, Rng};
use tokio_util::sync::CancellationToken;

/// Configuration for [TestRepo]. 
/// 
/// This configuration is to create a new [couch_rs::Client](https://docs.rs/couch_rs/latest/couch_rs/struct.Client.html) 
/// and name the associated [couch_rs::database::Database](https://docs.rs/couch_rs/latest/couch_rs/database/struct.Database.html). 
#[derive(Clone)]
pub struct TestRepoConfig {
    uri: String,
    username: String,
    password: String,
    db_name: String,
}

impl TestRepoConfig {
    /// Create a new configuration; identifying the uri, username and password for the CouchDB client
    /// instance as well as the database name for the underlying database. 
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
/// Creation of a new instance of this struct will create a unique [couch_rs::database::Database](https://docs.rs/couch_rs/latest/couch_rs/database/struct.Database.html). 
/// Internally, this struct implements a drop token and watcher to determine when this struct is de-allocated, 
/// thus triggering destruction of the associated CouchDB database. 
pub struct TestRepo {
    /// A [couch_rs::database::Database](https://docs.rs/couch_rs/latest/couch_rs/database/struct.Database.html). 
    /// Tests using this wrapper should access this struct in order to perform test actions against this 
    /// TestRepo's ephemeral CouchDB database. 
    pub db: Database,

    drop_token: CancellationToken,
    dropped_token: CancellationToken,
}

impl TestRepo {
    /// Creates a new instance of TestRepo wrapping a new instance of [couch_rs::database::Database](https://docs.rs/couch_rs/latest/couch_rs/database/struct.Database.html). 
    /// This function will create a new [couch_rs::Client](https://docs.rs/couch_rs/latest/couch_rs/struct.Client.html)
    /// from the parameters passed as part of the [TestRepoConfig] argument and  then create a new database 
    /// in CouchDB using the client connection and a database name consisting of the name defined in config 
    /// plus a random suffix.This randomization of database names helps prevent collisions during parallel 
    /// test excutions against the same CouchDB instance. 
    /// 
    /// This function also creates a drop token and watcher to determine when this instance is de-allocated
    /// The watcher spawn an asynchronous thread that will observe the drop token every 100 milliseconds.
    /// when this instance is deallocated, the drop token is destroyed and the watcher will trigger the
    /// destruction of the database instance created by this method. 
    pub async fn new(arg_cfg: TestRepoConfig) -> Result<TestRepo, Box<dyn Error>> {
        // create random identifier for database and append to db name
        let test_identifier = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(12)
            .map(char::from)
            .collect::<String>()
            .to_lowercase();

        let db_unique_name = format!("{}-{}", arg_cfg.db_name, test_identifier);
        let cfg = arg_cfg.with_name(db_unique_name);

        let client = Client::new(&cfg.uri, &cfg.username, &cfg.password)?;


        let drop_token = CancellationToken::new();
        let dropped_token = TestRepo::start_drop_watcher(&drop_token, cfg.clone()).await;

        // connect to database and return wrapping repository
        log::info!("Creating database {} for testing", cfg.db_name);

        // create test database - panic on fail
        match client.make_db(&cfg.db_name).await {
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

        Ok(TestRepo {
            db: client.db(&cfg.db_name).await?,
            drop_token: drop_token,
            dropped_token: dropped_token,
        })
    }

    /// Pushes data to the unique database associated with this instance. Data is pushed via the 
    /// [couch_rs::database::bulk_docs](https://docs.rs/couch_rs/latest/couch_rs/database/struct.Database.html#method.bulk_docs)
    /// method. 
    pub async fn with_data<S: TypedCouchDocument>(
        &self,
        data: &mut [S],
    ) -> Result<usize, CouchError> {
        let result = self.db.bulk_docs(data).await?;
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

            TestRepo::drop(cfg).await;

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

impl Drop for TestRepo {
    fn drop(&mut self) {
        self.drop_token.cancel();

        while !self.dropped_token.is_cancelled() {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
}
