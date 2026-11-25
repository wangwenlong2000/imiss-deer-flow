export interface IThirdOAIModel {
  available: boolean;
  create_date: string;
  fid: string;
  id: number;
  model: string;
  max_tokens: number;
  model_type: string;
  status: string;
  tags: string;
  update_date: string;
}

export type IThirdOAIModelCollection = Record<string, IThirdOAIModel[]>;

export interface IFactory {
  create_date: string;
  logo: string;
  name: string;
  status: string;
  tags: string;
  update_date: string;
}

export interface IMyLlmValue {
  llm: Llm[];
  tags: string;
}

export interface Llm {
  name: string;
  type: string;
  used_token: number;
  status: string;
}
