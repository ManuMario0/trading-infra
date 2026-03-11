//! Provides tools to launch a service.

use std::path::PathBuf;
use std::time::Duration;

use tokio::time;

use crate::port::allocator::Allocator;
use crate::service::info::ServiceInfo;
use crate::service_id::ServiceIdAllocator;

use journal::event::orchestrator::ServiceBootError;

impl ServiceInfo {
    pub async fn startup_service(
        port_allocator: &mut Allocator,
        service_allocator: &mut ServiceIdAllocator,
        path: PathBuf,
    ) -> Result<Self, ServiceBootError> {
        // First get the manifest.
        let mut child = std::process::Command::new(&path)
            .env(service::env::MANIFEST_REQUEST, true.to_string())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| ServiceBootError::SpawnFailed {
                path: path.clone(),
                os_error_code: e.raw_os_error(),
                reason: e.to_string(),
            })?;

        time::sleep(Duration::from_millis(100)).await;
        if child
            .try_wait()
            .map_err(|e| ServiceBootError::SpawnFailed {
                path: path.clone(),
                os_error_code: e.raw_os_error(),
                reason: format!("Failed to wait on child: {}", e),
            })?
            .is_none()
        {
            let _ = child.kill();
            return Err(ServiceBootError::ManifestTimeout { path });
        }

        let stdout = child.stdout.take();
        if stdout.is_none() {
            return Err(ServiceBootError::StdoutCaptureFailed { path });
        }
        let mut stdout = stdout.unwrap();
        let mut manifest_str = String::new();
        std::io::Read::read_to_string(&mut stdout, &mut manifest_str)
            .map_err(|_| ServiceBootError::StdoutCaptureFailed { path: path.clone() })?;

        let manifest: service::manifest::Manifest =
            serde_json::from_str(&manifest_str).map_err(|e| {
                ServiceBootError::ManifestParseError {
                    path: path.clone(),
                    reason: e.to_string(),
                }
            })?;

        // Now bootup the service for real,
        // providing it with logger addr and service id
        let service_id = service_allocator.next();
        let child = std::process::Command::new(&path)
            .env(service::env::SERVICE_ID, service_id.get_inner().to_string())
            .spawn()
            .map_err(|e| ServiceBootError::SpawnFailed {
                path: path.clone(),
                os_error_code: e.raw_os_error(),
                reason: format!("Failed to launch final service: {}", e),
            })?;

        Ok(ServiceInfo {
            handle: child,
            identifier: service_id,
            manifest,
        })
    }
}
