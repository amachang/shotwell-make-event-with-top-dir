use std::{path::{Path, PathBuf}, collections::{hash_map, HashMap}};
use clap::{Parser, crate_name};
use serde::{Deserialize, Serialize};
use dirs::{data_local_dir, picture_dir};
use sqlx::{sqlite::SqliteConnection, Connection, Row};
use anyhow::Result;
use futures::stream::StreamExt;
use chrono::Utc;

const KNOWN_SCHEMA_VERSION: i64 = 24;

#[derive(Parser, Debug)]
struct Args {
    #[clap(short, long, help = "Run the program, otherwise dry-run only")]
    run: bool,
}

#[derive(Deserialize, Serialize, Debug)]
struct Config {
    profile_dir: PathBuf,
    library_dir: PathBuf,
    n_levels_dir_using_as_event_name: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            profile_dir: data_local_dir().unwrap().join("shotwell"),
            library_dir: picture_dir().unwrap(),
            n_levels_dir_using_as_event_name: 1,
        }
    }
}

impl Config {
    fn db_path(&self) -> PathBuf {
        self.profile_dir.join("data").join("photo.db")
    }

    fn assert_dirs(&self) {
        if !self.profile_dir.exists() {
            panic!("Profile directory {:?} does not exist", self.profile_dir);
        }
        if !self.library_dir.exists() {
            panic!("Library directory {:?} does not exist", self.library_dir);
        }
        let db_path = self.db_path();
        if !self.profile_dir.join("data").join("photo.db").exists() {
            panic!("Database file {:?} does not exist", db_path);
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("Invalid unicode path: {0}")]
    InvalidUnicodePath(PathBuf),
    #[error("Missing managed file: {0}")]
    MissingManagedFile(PathBuf),
    #[error("No event directory for file: {0}")]
    NoEventDirForFile(PathBuf),
    #[error("Invalid photo id: {0}")]
    InvalidPhotoId(i64),
    #[error("Unknown schema version: {0}, expected: {1}")]
    UnknownSchemaVersion(i64, i64),
    #[error("Photo time not found for id: {0}")]
    PhotoTimeNotFound(i64),
    #[error("No first photo id for event: {0}")]
    NoFirstPhotoIdForEvent(String),
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let config = jdt::project(crate_name!()).config::<Config>();
    config.assert_dirs();

    let args = Args::parse();
    let dry_run = !args.run;

    let db_path = config.db_path();
    log::info!("Backup database file: {:?}", db_path);

    jdt::backup(&db_path)?;
    let db_path = match db_path.to_str() {
        Some(path) => path,
        None => Err(Error::InvalidUnicodePath(db_path.into()))?,
    };
    let db_url = format!("sqlite://{}", db_path);

    let lib_dir = config.library_dir;
    let n_levels = config.n_levels_dir_using_as_event_name;

    let mut conn = SqliteConnection::connect(&db_url).await?;
    let mut tx = conn.begin().await?;

    let tx_result: Result<()> = loop {
        // check schema_version
        log::info!("Querying VersionTable");

        let query = "SELECT schema_version FROM VersionTable ORDER BY schema_version DESC LIMIT 1";
        log::debug!("SQL: {}", query);
        let row = sqlx::query(query)
            .fetch_one(&mut *tx)
            .await;
        let row = match row {
            Ok(row) => row,
            Err(e) => break Err(e.into()),
        };
        let schema_version = match row.try_get::<i64, _>(0) {
            Ok(schema_version) => schema_version,
            Err(e) => break Err(e.into()),
        };
        if schema_version != KNOWN_SCHEMA_VERSION {
            break Err(Error::UnknownSchemaVersion(schema_version, KNOWN_SCHEMA_VERSION).into());
        }

        /*
         * PhotoTable schema:
         * CREATE TABLE PhotoTable (id INTEGER PRIMARY KEY, filename TEXT UNIQUE NOT NULL, width INTEGER, height INTEGER, filesize INTEGER, timestamp INTEGER, exposure_time INTEGER, orientation INTEGER, original_orientation INTEGER, import_id INTEGER, event_id INTEGER, transformations TEXT, md5 TEXT, thumbnail_md5 TEXT, exif_md5 TEXT, time_created INTEGER, flags INTEGER DEFAULT 0, rating INTEGER DEFAULT 0, file_format INTEGER DEFAULT 0, title TEXT, backlinks TEXT, time_reimported INTEGER, editable_id INTEGER DEFAULT -1, metadata_dirty INTEGER DEFAULT 0, developer TEXT, develop_shotwell_id INTEGER DEFAULT -1, develop_camera_id INTEGER DEFAULT -1, develop_embedded_id INTEGER DEFAULT -1, has_gps INTEGER DEFAULT -1, gps_lat REAL, gps_lon REAL, comment TEXT);
         */
        let mut id_to_event_name: HashMap<i64, String> = HashMap::new();
        let mut event_name_to_first_photo_id_and_time: HashMap<String, (i64, i64)> = HashMap::new();
        {
            log::info!("Querying PhotoTable");

            let query = "SELECT id, filename, exposure_time, time_created FROM PhotoTable";
            log::debug!("SQL: {}", query);
            let mut rows = sqlx::query(query)
                .fetch(&mut *tx);
            let tx_result: Result<()> = loop {
                let row = match rows.next().await {
                    Some(row) => match row {
                        Ok(row) => row,
                        Err(e) => break Err(e.into()),
                    },
                    None => break Ok(()),
                };
                let id = match row.try_get::<i64, _>(0) {
                    Ok(id) => id,
                    Err(e) => break Err(e.into()),
                };
                let path = match row.try_get::<String, _>(1) {
                    Ok(path) => path,
                    Err(e) => break Err(e.into()),
                };
                let exposure_time = match row.try_get::<Option<i64>, _>(2) {
                    Ok(exposure_time) => exposure_time,
                    Err(e) => break Err(e.into()),
                };
                let time_created = match row.try_get::<Option<i64>, _>(3) {
                    Ok(time_created) => time_created,
                    Err(e) => break Err(e.into()),
                };


                if id <= 0 {
                    break Err(Error::InvalidPhotoId(id).into());
                }

                let path = Path::new(&path);
                if !path.exists() {
                    break Err(Error::MissingManagedFile(path.into()).into());
                }
                let rel_path = match path.strip_prefix(&lib_dir) {
                    Ok(rel_path) => rel_path,
                    Err(e) => break Err(e.into()),
                };
                let event_name_components = rel_path.components().take(n_levels as usize).collect::<Vec<_>>();
                if event_name_components.is_empty() {
                    break Err(Error::NoEventDirForFile(rel_path.into()).into());
                }
                let event_name = event_name_components.iter().map(|c| c.as_os_str().to_string_lossy()).collect::<Vec<_>>().join("/");
                id_to_event_name.insert(id, event_name.clone());

                // we use the earliest photo used as the primary photo
                let photo_time = match (exposure_time, time_created) {
                    (Some(exposure_time), None) => exposure_time,
                    (None, Some(time_created)) => time_created,
                    (Some(exposure_time), Some(time_created)) => {
                        if exposure_time < time_created {
                            exposure_time
                        } else {
                            time_created
                        }
                    },
                    (None, None) => break Err(Error::PhotoTimeNotFound(id).into()),
                };
                match event_name_to_first_photo_id_and_time.entry(event_name) {
                    hash_map::Entry::Occupied(mut entry) => {
                        let (_, prev_first_photo_time) = entry.get();
                        if photo_time < *prev_first_photo_time {
                            entry.insert((id, photo_time));
                        }
                    },
                    hash_map::Entry::Vacant(entry) => {
                        entry.insert((id, photo_time));
                    },
                }
            };

            if let Err(e) = tx_result {
                break Err(e);
            }
        }

        /*
         * EventTable schema:
         * CREATE TABLE EventTable (id INTEGER PRIMARY KEY, name TEXT, primary_photo_id INTEGER, time_created INTEGER,primary_source_id TEXT,comment TEXT);
         */
        let mut id_event_name_iter = id_to_event_name.iter();
        let tx_result: Result<()> = loop {
            let (id, event_name) = match id_event_name_iter.next() {
                Some((id, event_name)) => (id, event_name),
                None => break Ok(()),
            };

            log::info!("Processing photo id: {}", id);

            // insert if not exists
            let query = "SELECT id FROM EventTable WHERE name = ?";
            log::debug!("SQL: {}: ({})", query, event_name);
            let event_row = sqlx::query(query)
                .bind(&event_name)
                .fetch_optional(&mut *tx)
                .await;
            let event_row = match event_row {
                Ok(event_row) => event_row,
                Err(e) => break Err(e.into()),
            };
            let event_id = match event_row {
                Some(row) => match row.try_get::<i64, _>(0) {
                    Ok(id) => id,
                    Err(e) => break Err(e.into()),
                },
                None => {
                    log::info!("Event not found, creating new event: {}", event_name);

                    // use the first photo as the primary photo
                    let first_photo_id = match event_name_to_first_photo_id_and_time.get(event_name) {
                        Some((id, _)) => *id,
                        None => break Err(Error::NoFirstPhotoIdForEvent(event_name.clone()).into()),
                    };
                    let primary_resource_id = upgrade_photo_id_to_resource_id(first_photo_id);

                    let unix_timestamp = Utc::now().timestamp();
                    let query = "INSERT INTO EventTable (name, time_created, primary_source_id) VALUES (?, ?, ?)";
                    log::debug!("SQL: {}: ({}, {}, {})", query, event_name, unix_timestamp, primary_resource_id);
                    let result = sqlx::query(query)
                        .bind(&event_name)
                        .bind(&unix_timestamp)
                        .bind(&primary_resource_id)
                        .execute(&mut *tx)
                        .await;
                    let result = match result {
                        Ok(result) => result,
                        Err(e) => break Err(e.into()),
                    };
                    let event_id = result.last_insert_rowid();
                    event_id
                },
            };

            let query = "UPDATE PhotoTable SET event_id = ? WHERE id = ?";
            log::debug!("SQL: {}: ({}, {})", query, event_id, id);
            let result = sqlx::query(query)
                .bind(&event_id)
                .bind(&id)
                .execute(&mut *tx)
                .await;
            match result {
                Ok(result) => result,
                Err(e) => break Err(e.into()),
            };
        };
        break tx_result;
    };

    match tx_result {
        Ok(()) => {
            if dry_run {
                tx.rollback().await?;
            } else {
                tx.commit().await?;
            }
        }
        Err(e) => {
            tx.rollback().await?;
            return Err(e);
        }
    }

    Ok(())
}

fn upgrade_photo_id_to_resource_id(id: i64) -> String {
    // return ("%s%016" + int64.FORMAT_MODIFIER + "x").printf(Photo.TYPENAME, photo_id.id);
    let shotwell_photo_typename = "thumb";
    format!("{}{:016x}", shotwell_photo_typename, id)
}

// test
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upgrade_photo_id_to_resource_id() {
        assert_eq!(upgrade_photo_id_to_resource_id(1), "thumb0000000000000001");
        assert_eq!(upgrade_photo_id_to_resource_id(0x123456789abcdef0), "thumb123456789abcdef0");
    }
}
