use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::errors::StoreError;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum WalEntry {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        lease: i64,
        revision: i64,
    },
    Delete {
        key: Vec<u8>,
        revision: i64,
    },
}

#[derive(Debug)]
pub struct UnifiedWal {
    path: PathBuf,
    file: Mutex<File>,
}

impl UnifiedWal {
    pub fn open(data_dir: &Path) -> Result<Self, StoreError> {
        std::fs::create_dir_all(data_dir)?;
        let path = data_dir.join("unified-raft.wal");
        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        Ok(Self {
            path,
            file: Mutex::new(file),
        })
    }

    pub fn append(&self, entry: &WalEntry) -> Result<(), StoreError> {
        let mut guard = self.file.lock();
        let line = serde_json::to_string(entry)?;
        guard.write_all(line.as_bytes())?;
        guard.write_all(b"\n")?;
        guard.sync_data()?;
        Ok(())
    }

    pub fn replay(&self) -> Result<Vec<WalEntry>, StoreError> {
        let file = OpenOptions::new().read(true).open(&self.path)?;
        let reader = BufReader::new(file);

        let mut out = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            out.push(serde_json::from_str::<WalEntry>(&line)?);
        }
        Ok(out)
    }
}
