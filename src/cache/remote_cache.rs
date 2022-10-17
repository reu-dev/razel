#[cfg(test)]
mod tests {
    use crate::bazel_remote_exec;
    use crate::bazel_remote_exec::action_cache_client::ActionCacheClient;
    use crate::bazel_remote_exec::capabilities_client::CapabilitiesClient;
    use crate::bazel_remote_exec::content_addressable_storage_client::ContentAddressableStorageClient;
    use crate::bazel_remote_exec::{
        batch_update_blobs_request, ActionResult, BatchReadBlobsRequest, BatchUpdateBlobsRequest,
        Code, Digest, GetActionResultRequest, GetCapabilitiesRequest, ServerCapabilities,
        UpdateActionResultRequest,
    };

    const INSTANCE_NAME: &str = "";
    const CACHE_URL: &str = "grpc://localhost:9092";

    #[tokio::test]
    async fn grpc_server_capabilities() {
        let mut client = CapabilitiesClient::connect(CACHE_URL).await.unwrap();
        let response: ServerCapabilities = client
            .get_capabilities(tonic::Request::new(GetCapabilitiesRequest {
                instance_name: INSTANCE_NAME.to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response
                .cache_capabilities
                .unwrap()
                .action_cache_update_capabilities
                .unwrap()
                .update_enabled,
            true
        );
    }

    /// Test a AC cache server using a random/unique Action
    #[tokio::test]
    async fn grpc_server_ac() {
        let mut client = ActionCacheClient::connect(CACHE_URL).await.unwrap();
        let stdout = format!(
            "Hello pid {} at {:?}",
            std::process::id(),
            std::time::Instant::now()
        );
        let action_digest = Digest::for_message(&bazel_remote_exec::Action {
            command_digest: Some(Digest::for_message(&bazel_remote_exec::Command {
                arguments: vec!["echo".into(), stdout.clone()],
                ..Default::default()
            })),
            ..Default::default()
        });
        let action_result = ActionResult {
            stdout_raw: stdout.clone().into(),
            ..Default::default()
        };
        // download should fail because the Action is unique
        let response = client
            .get_action_result(tonic::Request::new(GetActionResultRequest {
                instance_name: INSTANCE_NAME.to_string(),
                action_digest: Some(action_digest.clone()),
                inline_stdout: true,
                ..Default::default()
            }))
            .await;
        assert_eq!(response.unwrap_err().code(), tonic::Code::NotFound);
        // upload it
        let response = client
            .update_action_result(tonic::Request::new(UpdateActionResultRequest {
                instance_name: INSTANCE_NAME.to_string(),
                action_digest: Some(action_digest.clone()),
                action_result: Some(action_result.clone()),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.exit_code, action_result.exit_code);
        assert_eq!(response.stdout_raw, action_result.stdout_raw);
        // now download should succeed
        let response = client
            .get_action_result(tonic::Request::new(GetActionResultRequest {
                instance_name: INSTANCE_NAME.to_string(),
                action_digest: Some(action_digest.clone()),
                inline_stdout: true,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.exit_code, action_result.exit_code);
        assert_eq!(response.stdout_raw, action_result.stdout_raw);
    }

    #[tokio::test]
    async fn grpc_server_cas() {
        let mut client = ContentAddressableStorageClient::connect(CACHE_URL)
            .await
            .unwrap();
        let content = format!(
            "Hello pid {} at {:?}",
            std::process::id(),
            std::time::Instant::now()
        );
        let digest = Digest::for_string(&content);
        // download should fail because the content is unique
        let response = client
            .batch_read_blobs(tonic::Request::new(BatchReadBlobsRequest {
                instance_name: INSTANCE_NAME.to_string(),
                digests: vec![digest.clone()],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.responses.len(), 1);
        let response_0 = &response.responses[0];
        assert_eq!(response_0.digest, Some(digest.clone()));
        assert_eq!(
            response_0.status.as_ref().unwrap().code,
            Code::NotFound as i32
        );
        assert_eq!(response_0.data, Vec::<u8>::new());
        assert_eq!(response_0.compressor, 0);
        // upload it
        let response = client
            .batch_update_blobs(tonic::Request::new(BatchUpdateBlobsRequest {
                instance_name: INSTANCE_NAME.to_string(),
                requests: vec![batch_update_blobs_request::Request {
                    digest: Some(digest.clone()),
                    data: content.clone().into_bytes(),
                    compressor: 0,
                }],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.responses.len(), 1);
        let response_0 = &response.responses[0];
        assert_eq!(response_0.digest, Some(digest.clone()));
        assert_eq!(response_0.status.as_ref().unwrap().code, Code::Ok as i32);
        // now download should succeed
        let response = client
            .batch_read_blobs(tonic::Request::new(BatchReadBlobsRequest {
                instance_name: INSTANCE_NAME.to_string(),
                digests: vec![digest.clone()],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.responses.len(), 1);
        let response_0 = &response.responses[0];
        assert_eq!(response_0.digest, Some(digest.clone()));
        assert_eq!(response_0.status.as_ref().unwrap().code, Code::Ok as i32);
        assert_eq!(response_0.data, content.into_bytes());
        assert_eq!(response_0.compressor, 0);
    }
}
