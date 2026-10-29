export interface IAddLlmRequestBody {
  llm_factory: string; // Ollama
  model: string;
  model_type: string;
  base_url?: string; // chat|embedding|speech2text|image2text
  api_key: string;
  max_tokens: number;
}

export interface IDeleteLlmRequestBody {
  llm_factory: string; // Ollama
  model?: string;
}
