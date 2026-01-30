use crate::build::PROJECT_NAME;
use crate::APP_VERSION;
use rmcp::model::{
    CallToolResult, Content, GetPromptRequestParams, GetPromptResult, Implementation,
    InitializeRequestParams, InitializeResult, ListPromptsResult, PaginatedRequestParam, Prompt,
    PromptArgument, PromptMessage, PromptMessageContent, PromptMessageRole, ProtocolVersion,
};
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router, ErrorData, RoleServer, ServerHandler,
};

use ariadne_core::memgraph_async::MemgraphAsync;
use rmcp::service::RequestContext;

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ExecuteCypherQueryRequest {
    pub query: String,
}

#[derive(Debug, Clone)]
pub struct KubeTool {
    cluster_name: String,
    memgraph: MemgraphAsync,
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl KubeTool {
    pub fn new_tool(cluster_name: String, memgraph: MemgraphAsync) -> Self {
        Self {
            cluster_name,
            memgraph,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(name = "execute_cypher_query", description = "Execute a Cypher query")]
    async fn execute_cypher_query(
        &self,
        Parameters(ExecuteCypherQueryRequest { query }): Parameters<ExecuteCypherQueryRequest>,
    ) -> Result<CallToolResult, ErrorData> {
        let records = {
            let records = self
                .memgraph
                .execute_query(query.as_str())
                .await
                .map_err(|e| ErrorData::internal_error(e.to_string(), None))?;
            records
        };
        let content = Content::json(records)?;
        Ok(CallToolResult::success(vec![content]))
    }
}

const CURRENT_PROMPT: &str = include_str!("../../prompt.txt");

#[tool_handler]
impl ServerHandler for KubeTool {
    async fn initialize(
        &self,
        _request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, ErrorData> {
        if let Some(http_request_part) = context.extensions.get::<axum::http::request::Parts>() {
            let initialize_headers = &http_request_part.headers;
            let initialize_uri = &http_request_part.uri;
            tracing::info!(?initialize_headers, %initialize_uri, "initialize from http server");
        }
        Ok(self.get_info())
    }

    fn get_info(&self) -> ServerInfo {
        let instruction = format!(
            "This MCP server provides a way to extract information about Kubernetes cluster {}",
            self.cluster_name
        );
        ServerInfo {
            protocol_version: ProtocolVersion::V_2025_03_26,
            capabilities: ServerCapabilities::builder()
                .enable_prompts()
                .enable_resources()
                .enable_tools()
                .build(),
            server_info: Implementation {
                name: PROJECT_NAME.to_owned(),
                title: Some(PROJECT_NAME.to_owned()),
                version: APP_VERSION.to_owned(),
                icons: None,
                website_url: None,
            },
            instructions: Some(instruction),
        }
    }

    async fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParam>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, ErrorData> {
        Ok(ListPromptsResult {
            meta: None,
            next_cursor: None,
            prompts: vec![Prompt::new(
                "analyze_question",
                Some("Asks the LLM to analyze user question and suggest next steps"),
                Some(vec![PromptArgument {
                    name: "question".to_string(),
                    title: Some("Question".to_string()),
                    description: Some("A question to analyze".to_string()),
                    required: Some(true),
                }]),
            )],
        })
    }

    async fn get_prompt(
        &self,
        GetPromptRequestParams {
            name, arguments, ..
        }: GetPromptRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, ErrorData> {
        match name.as_str() {
            "analyze_question" => {
                let question = arguments
                    .and_then(|json| json.get("question")?.as_str().map(|s| s.to_string()))
                    .ok_or_else(|| {
                        ErrorData::invalid_params("No message provided to analyze_question", None)
                    })?;

                let prompt = format!("{CURRENT_PROMPT}\n\nUser question: '{question}'");
                Ok(GetPromptResult {
                    description: None,
                    messages: vec![PromptMessage {
                        role: PromptMessageRole::User,
                        content: PromptMessageContent::text(prompt),
                    }],
                })
            }
            _ => Err(ErrorData::invalid_params(
                format!("prompt {name} not found"),
                None,
            )),
        }
    }
}
