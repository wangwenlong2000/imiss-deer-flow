import api from '@/utils/api';
import request from '@/utils/request';

const {
  getMcpConfig,
  updateMcpConfig,
  getServerConfig,
  updateServerConfig,
  deleteServerConfig,
  reloadMcpConfig,
  getA2ACommonPath,
  createMcpConfig,
  createSchema,
  deleteSchema,
  updateSchema,
  getSchemaList,
  reloadMcp,
  getSchemaExist,
} = api;

export const mcpService = {
  // 获取A2A列表
  getA2AList: (params?: {
    page?: number;
    pageSize?: number;
    search?: string;
    type?: string;
    status?: number;
  }) => {
    return request.get(api.getA2ACommonPath, { params });
  },

  // 更新A2A配置
  updateA2AConfig: (data: any) => {
    console.log('Updating A2A config with data:', data, updateMcpConfig);
    return request.put(`${getA2ACommonPath}/${data.agent_id}`, {
      data: data,
    });
  },

  // 创建a2a配置
  createA2AConfig: (data: any) => {
    return request.post(getA2ACommonPath, {
      data: data,
    });
  },

  // 删除a2a配置
  deleteA2AConfig: (id: string) => {
    return request.delete(`${api.getA2ACommonPath}/${id}`);
  },

  // 获取MCP配置（支持分页和搜索）
  getMcpConfig: (params?: {
    page?: number;
    pageSize?: number;
    search?: string;
  }) => {
    return request.get(getMcpConfig, { params });
  },

  // 重载mcp配置
  reloadMcp: (data: any, url: string) => {
    return request.post(url, { data });
  },

  // 更新MCP配置
  updateMcpConfig: (data: any) => {
    return request.put(updateMcpConfig, {
      data: data,
    });
  },

  // 创建方案
  createSchema: (data: any) => {
    return request.post(createSchema, { data });
  },

  // 查询方案是否存在
  searchSchemaExist: (name: string) => {
    return request.get(`${getSchemaExist}`, {
      params: {
        scene: name,
      },
    });
  },

  // 修改方案
  updateSchema: (data: any) => {
    return request.put(updateSchema(data.id), { data });
  },

  // 删除方案
  deleteSchema: (data: any) => {
    return request.delete(deleteSchema(data.id));
  },

  // 获取方案列表
  getSchemaList: (data: any) => {
    return request.get(`${getSchemaList}`, {
      params: data,
    });
  },

  // 获取指定服务器配置
  getServerConfig: (serverId: string) => {
    return request.get(getServerConfig(serverId));
  },

  // 创建新的MCP配置
  createMcpConfig: (data: any) => {
    return request.post(createMcpConfig, {
      data: data,
    });
  },

  // 更新指定服务器配置
  updateServerConfig: (serverId: string, data: any) => {
    return request.put(updateServerConfig(serverId), {
      data: data,
    });
  },

  // 删除指定服务器配置
  deleteServerConfig: (serverId: string) => {
    return request.delete(deleteServerConfig(serverId));
  },

  // 重新加载MCP配置
  reloadMcpConfig: (serverId?: string) => {
    if (serverId) {
      // 使用查询参数而不是请求体传递server_id
      return request(reloadMcpConfig, {
        method: 'POST',
        params: { server_id: serverId },
      });
    } else {
      // 不指定特定服务器，重载所有服务器
      return request(reloadMcpConfig, {
        method: 'POST',
      });
    }
  },
};

export default mcpService;
