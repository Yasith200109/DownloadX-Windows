//! Integration coverage for the live adaptive supervisor.
//!
//! These tests deliberately run longer than one supervisor sampling interval and exercise
//! pause/resume while adaptive connection management is active.

use mdm_engine::{Destination, Download, Options, Request};
use mdm_labserver::{Config, Faults, LabServer, content};

#[tokio::test]
async fn adaptive_supervisor_pause_and_resume_preserves_every_byte() {
    let server = LabServer::start(Config {
        faults: Faults::parse("rate=128K&stall_at=24M").unwrap(),
        ..Config::default()
    })
    .await
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let request = Request {
        url: server
            .url("/gen/32M/adaptive.bin?seed=2026")
            .parse()
            .unwrap(),
        headers: Default::default(),
        dest: Destination::Dir(dir.path().to_owned()),
    };
    let opts = Options {
        retry_delay: std::time::Duration::from_millis(50),
        ..Options::default()
    };
    let download = Download::start(request, opts.clone());

    // The lab server supports byte ranges by default. Wait for real progress and more than
    // one three-second supervisor interval; request log entries are emitted only when a
    // response body closes, so they cannot prove range support while the transfer is active.
    // The server's rate and stall keep the transfer alive.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let started = std::time::Instant::now();
    let file = loop {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let snapshot = download.snapshot();
        let sampling_window_elapsed = started.elapsed() >= std::time::Duration::from_secs(4);
        if snapshot.downloaded >= 1 << 20 && sampling_window_elapsed {
            break snapshot.file.expect("the download has a destination");
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the adaptive download did not reach its observation window ({} bytes)",
            snapshot.downloaded
        );
    };

    download.cancel();
    assert!(matches!(
        download.wait().await,
        Err(mdm_engine::Error::Cancelled)
    ));

    let state = mdm_engine::state::state_path(&file);
    assert!(state.exists(), "pause keeps a resumable state");

    // Remove the deliberate bandwidth/stall faults before waiting for the closed responses.
    server.set_faults(Faults::default());
    tokio::time::timeout(std::time::Duration::from_secs(20), server.settled())
        .await
        .expect("the paused requests have ended");

    let outcome = Download::resume(state, Default::default(), opts)
        .expect("the adaptive download resumes")
        .wait()
        .await
        .expect("the resumed download completes");

    assert_eq!(outcome.size, 32 << 20);
    let data = std::fs::read(&outcome.path).unwrap();
    let mut expected = vec![0; 32 << 20];
    content::fill(2026, 0, &mut expected);
    assert_eq!(
        data, expected,
        "pause/resume must not lose or duplicate bytes"
    );
}
