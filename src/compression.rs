use std::io;
use std::os::windows::fs::OpenOptionsExt;
use std::path::PathBuf;

use async_trait::async_trait;
use compresstimator::Compresstimator;
use crossbeam_channel::{Receiver, Sender};
use filetime::FileTime;
use fs2::FileExt;
use winapi::um::winnt::{FILE_READ_DATA, FILE_WRITE_ATTRIBUTES};

use crate::background::{yield_now, ControlToken};
use crate::background::{Background, BackgroundFuture};
use crate::compact::{self, Compression};
use std::fs::File;

pub struct BackgroundCompactorFuture<Next> {
    compression: Option<Compression>,
    next: Next,
}

impl<Next> BackgroundCompactorFuture<Next> {
    pub fn new(compression: Option<Compression>, next: Next) -> Self {
        Self { compression, next }
    }
}

#[async_trait]
impl<Next: Send + Sync> BackgroundFuture for BackgroundCompactorFuture<Next>
where
    Next: BackgroundFuture<Input = (File, io::Result<bool>)>,
{
    type Input = (File, u64);

    async fn run(&self, (file, _size): Self::Input) {
        let ret = handle_file(&file, self.compression).await;
        self.next.run((file, ret)).await
    }
}

#[derive(Debug)]
pub struct BackgroundCompactor {
    compression: Option<Compression>,
    files_in: Receiver<(PathBuf, u64)>,
    files_out: Sender<(PathBuf, io::Result<bool>)>,
}

impl BackgroundCompactor {
    pub fn new(
        compression: Option<Compression>,
        files_in: Receiver<(PathBuf, u64)>,
        files_out: Sender<(PathBuf, io::Result<bool>)>,
    ) -> Self {
        Self {
            compression,
            files_in,
            files_out,
        }
    }
}

async fn handle_file(handle: &File, compression: Option<Compression>) -> io::Result<bool> {
    let est = Compresstimator::with_block_size(8192);
    let meta = handle.metadata()?;

    handle.try_lock_exclusive()?;

    crate::background::yield_now().await;

    let ret = match compression {
        Some(compression) => {
            let compressimate = est.compresstimate(handle, meta.len());
            yield_now().await;
            match compressimate {
                Ok(ratio) if ratio < 0.95 => compact::compress_file_handle(handle, compression),
                Ok(_) => Ok(false),
                Err(e) => Err(e),
            }
        }
        None => compact::uncompress_file_handle(handle).map(|_| true),
    };

    let _ = filetime::set_file_handle_times(
        handle,
        Some(FileTime::from_last_access_time(&meta)),
        Some(FileTime::from_last_modification_time(&meta)),
    );

    handle.unlock()?;

    ret
}

impl Background for BackgroundCompactor {
    type Output = ();
    type Status = ();

    fn run(self, control: &ControlToken<Self::Status>) -> Self::Output {
        for file in &self.files_in {
            if control.is_cancelled_with_pause() {
                break;
            }

            let file = file.0;
            unimplemented!()
            /*let ret = handle_file(&file, self.compression);
            if self.files_out.send((file, ret)).is_err() {
                break;
            }
             */
        }
    }
}
