use bitcode::{Encode, Decode};
use std::path::{Path, PathBuf};
use std::error::Error;
use std::fs::{self};
use std::time::SystemTime;

#[derive(Clone, Debug, Encode, Decode)]
pub struct WALEntry {
    pub entry_type: EntryType,
    pub data: Option<Vec<u8>>,
    pub timestamp: f64,
    pub transaction_id: u64,
}

impl WALEntry {
    fn size(&self) -> usize {
        let data_size = self.data.as_ref().map_or(0, |data| data.len());

        size_of::<EntryType>() + size_of::<f64>() + size_of::<u64>() + data_size
    }
}

#[derive(Clone, Debug, Encode, Decode)]
pub enum EntryType {
    Insert,
    Set,
    Delete,
    Checkpoint,

    TransactionBegin,
    TransactionCommit,
}

pub struct WALManager {
    sequence: usize,
    page_size: usize,
    buffered: Vec<WALEntry>,
    directory: PathBuf,
}

// TODO: gz 압축 구현
// TODO: thiserror
impl WALManager {
    pub fn builder() -> WALBuilder {
        WALBuilder::default()
    }

    fn check_and_mark(&mut self, entry: &WALEntry) -> Result<(), Box<dyn Error>> {
        let size = self.buffered.iter().map(|entry| entry.size()).sum::<usize>();

        if size > self.page_size {
            self.checkpoint()?;
        }

        Ok(())
    }

    fn append(&mut self, entry: WALEntry) -> Result<(), Box<dyn Error>>{
        self.buffered.push(entry);
        let path = Path::join(&self.directory, format!("wal{}.log", self.sequence));
        let bytes = bitcode::encode(&self.buffered)?;

        fs::write(path, bytes)?;

        Ok(())
    }

    pub fn append_log(&mut self, entry: WALEntry) -> Result<(), Box<dyn Error>>{
        self.check_and_mark(&entry)?;

        self.append(entry)?;

        Ok(())
    }

    pub fn checkpoint(&mut self) -> Result<(), Box<dyn Error>> {
        self.append(WALEntry {
            data: None,
            entry_type: EntryType::Checkpoint,
            timestamp: WALManager::get_current_secs(),
            transaction_id: 0
        })?;

        self.buffered.clear();
        self.sequence += 1;

        Ok(())
    }

    pub fn get_current_secs() -> f64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Cannot getting since time")
            .as_secs_f64()
    }

}

pub struct WALBuilder {
    page_size: usize,
    directory: PathBuf,
}

impl Default for WALBuilder {
    fn default() -> Self {
        Self { page_size: 4096, directory: PathBuf::from(".") }
    }
}

impl WALBuilder {
    pub fn set_page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size;
        self
    }

    pub fn set_directory(mut self, directory: PathBuf) -> Self {
        self.directory = directory;
        self
    }

    fn load_data(&self) -> Result<(usize, Vec<WALEntry>), std::io::Error> {
        let mut log_sequence = 1;
        let log_files = std::fs::read_dir(&self.directory)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension() == Some(std::ffi::OsStr::new("log")))
            .collect::<Vec<_>>();

        let mut entries = Vec::new();

        if let Some(last_log) = log_files.last() {
            log_sequence = log_files.len();
            let file_content = std::fs::read(last_log.path())?;
            let saved_entries: Vec<WALEntry> = bitcode::decode(&file_content)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

            
            match saved_entries.last() {
                Some(last_entry) => {
                    match last_entry.entry_type {
                        EntryType::Checkpoint => log_sequence += 1,
                        _ => entries = saved_entries.clone(),
                        
                    }
                },
                _ => {}
            }
        }

        Ok((log_sequence, entries))
    }

    pub fn build(self) -> Result<WALManager, std::io::Error> {
        let (sequence, buffered) = self.load_data()?;

        Ok(WALManager {
            sequence: sequence,
            page_size: self.page_size,
            directory: self.directory,
            buffered: Vec::new(),
        })
    }
}

#[cfg(test)]
mod io_tests {
    use super::{WALEntry, WALManager, EntryType};

    #[test]
    fn test_create() {
        let builder = WALManager::builder()
            .build();
        assert!(builder.is_ok());

        let builder = builder.unwrap();
        assert_eq!(builder.sequence, 1);
    }

    #[test]
    fn test_append_wal() {
        let mut wal_manager = WALManager::builder()
            .build().expect("Cannot create WALManager");

        let start = WALManager::get_current_secs();
        for _ in 0..100 {
            let entry = WALEntry {
                entry_type: EntryType::Insert,
                data: Some(Vec::from([10u8;100])),
                timestamp: WALManager::get_current_secs(),
                transaction_id: 0
            };

            let result = wal_manager.append_log(entry);
            assert!(result.is_ok());
        }
        let end = WALManager::get_current_secs();

        println!("elapsed: {}s", end - start);
    }
}