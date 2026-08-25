//! Push one local file into one dataset repository.
//!
//! Not a command, and deliberately not one. Doc 14 has no verb for "put this
//! file somewhere on the hub", because the tool publishes segments and manifests
//! and nothing else, and a general purpose upload command would be a way to put
//! anything at all under the organisation's name. This is here because the
//! fixtures that live outside the repository still have to get there somehow:
//! the wide golden corpus from doc 11.10 and its dataset card were both pushed
//! with it, from a box with no python and no hugging face client on it, through
//! the same [`Hub`] the publisher uses.
//!
//! ```text
//! HF_TOKEN=... cargo run --release -p umi-publish --example upload -- \
//!     open-index/umi-golden wide.parquet ~/umi-golden/wide.parquet
//! ```
//!
//! The token comes out of the environment and is never an argument, because
//! arguments are in `ps` output and in shell history and an environment
//! variable is in neither.

use std::path::PathBuf;

use umi_publish::{Hub, Upload};

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let repo = args.next().expect("a repository, as org/name");
    let path = args.next().expect("a path inside the repository");
    let local = PathBuf::from(args.next().expect("a local file"));

    let bytes = std::fs::read(&local).expect("the file reads");
    let sha256 = {
        use sha2::Digest as _;
        let mut hasher = sha2::Sha256::new();
        hasher.update(&bytes);
        hex::encode(hasher.finalize())
    };

    // A dataset card has to be a real file in the git tree or the hub renders
    // the lfs pointer instead of the markdown, so anything small rides inside
    // the commit and anything big goes through lfs. One megabyte is well under
    // the hub's own threshold and well over any card.
    let file = if bytes.len() < 1 << 20 {
        Upload::Inline {
            path: path.clone(),
            bytes: bytes.clone(),
        }
    } else {
        Upload::Blob {
            path: path.clone(),
            local,
            size: bytes.len() as u64,
            sha256,
        }
    };

    let hub = Hub::new(std::env::var("HF_TOKEN").expect("HF_TOKEN is set")).expect("a client");
    hub.ensure_dataset(&repo).await.expect("the dataset exists");
    let commit = hub
        .upload(&repo, &[file], &format!("Add {path}"))
        .await
        .expect("the upload lands");
    println!(
        "{} bytes at {repo}/{path}, commit {}",
        bytes.len(),
        commit.oid
    );
}
