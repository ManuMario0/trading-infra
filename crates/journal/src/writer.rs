//! Write logs to disk. It handles file creation, renaming, capacity etc ...

use std::{fs::OpenOptions, io::Write, path::PathBuf, time};

use memmap2::MmapMut;
use trading_sdk::identity::ServiceId;

const FILE_CAPACITY: u64 = 1024 * 1024; // 1MB
const TMP_FILE_NAME: &str = "tmp_journal.log";

pub(crate) struct Writer {
    // -- directory information
    working_dir: PathBuf,
    log_dir: PathBuf,

    // -- identity information (for file naming)
    id: ServiceId,

    // -- current file information
    current_file_path: PathBuf,
    current_file_handle: Option<std::fs::File>,
    cursor: u64,
    mapped_region: Option<MmapMut>,
}

impl Writer {
    pub(crate) fn new(working_dir: PathBuf, log_dir: PathBuf, id: ServiceId) -> Self {
        let current_file_path = working_dir.join(TMP_FILE_NAME);
        let current_file_handle = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .truncate(true)
            .open(&current_file_path)
            .unwrap();

        current_file_handle.set_len(FILE_CAPACITY).unwrap();

        let mapped_region = unsafe { MmapMut::map_mut(&current_file_handle) }.unwrap();

        Self {
            working_dir,
            log_dir,
            id,
            current_file_path,
            current_file_handle: Some(current_file_handle),
            cursor: 0,
            mapped_region: Some(mapped_region),
        }
    }

    pub(crate) fn write(&mut self, data: &[u8]) -> Result<(), std::io::Error> {
        // Check if there is enough place left in the current file
        if self.cursor + data.len() as u64 > FILE_CAPACITY {
            // Rotate the file
            self.rotate()?;
        }

        // Copy the data to the mapped region
        self.mapped_region.as_mut().unwrap()
            [self.cursor as usize..self.cursor as usize + data.len()]
            .copy_from_slice(data);
        self.cursor += data.len() as u64;

        Ok(())
    }

    /// Rename the temporary file to a definitive name and open a new temporary file for log writing.
    ///
    /// Having a temporary names prevents any type of later automatic log gatherer to aggressively pull and delete current working file.
    fn rotate(&mut self) -> Result<(), std::io::Error> {
        // 1. Flush the mmap to disk
        self.mapped_region.as_mut().unwrap().flush()?;

        // 2. Unmap the buffer before truncating the file
        let _ = self.mapped_region.take();

        // 3. Safely truncate the physical file down to exact bytes written
        let mut handle = self.current_file_handle.take().unwrap();
        handle.set_len(self.cursor)?;
        handle.flush()?;
        handle.sync_all()?;
        drop(handle); // File is closed

        // 4. Rename the closed file
        let new_file_path = self.log_dir.join(format!(
            "{}_{}.log",
            self.id.get_inner(),
            time::SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        std::fs::rename(&self.current_file_path, &new_file_path)?;

        // 5. Create a new file
        let current_file_path = self.working_dir.join(TMP_FILE_NAME);
        let current_file_handle = OpenOptions::new()
            .read(true) // map_mut requires the file to be opened with read access!
            .write(true)
            .create_new(true)
            .truncate(true)
            .open(&current_file_path)?;

        current_file_handle.set_len(FILE_CAPACITY)?;

        let mapped_region = unsafe { MmapMut::map_mut(&current_file_handle) }?;

        self.current_file_path = current_file_path;
        self.current_file_handle = Some(current_file_handle);
        self.cursor = 0;
        self.mapped_region = Some(mapped_region);

        Ok(())
    }
}
