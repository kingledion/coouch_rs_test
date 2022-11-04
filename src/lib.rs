use couch_rs::{document::TypedCouchDocument, error::CouchError, Client};
use rand::{distributions::Alphanumeric, Rng};
use tokio_util::sync::CancellationToken;

pub trait CouchDBWrapper {
    fn new(uri: &str, username: &str, password: &str, db_name: &str) -> Self;
    fn client(&self) -> &Client;
    fn dbname(&self) -> &str;
}


#[derive(Clone)]
pub struct TestRepoConfig {
    uri: String,
    username: String,
    password: String,
    db_name: String,
}

impl TestRepoConfig {
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

pub struct TestRepo<T: CouchDBWrapper> {
    pub repo: T,

    drop_token: CancellationToken,
    dropped_token: CancellationToken,
}

impl<T: CouchDBWrapper> TestRepo<T> {
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

        let wrapped = T::new(&cfg.uri, &cfg.username, &cfg.password, &cfg.db_name);

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
