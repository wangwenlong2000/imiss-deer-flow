import { useTranslate } from '@/hooks/common-hooks';
import { mcpService } from '@/services/mcp-service';
import userService from '@/services/user-service';
import {
  DeleteOutlined,
  DownOutlined,
  EditOutlined,
  PlusOutlined,
  ReloadOutlined,
  UpOutlined,
} from '@ant-design/icons';
import { Button, Card, Form, Popconfirm, Space, Table, message } from 'antd';
import 'antd/dist/reset.css';
import _ from 'lodash';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import A2AConfigModal from './Modal/a2aConfigModal';
import McpConfigModal from './Modal/mcpModal';
import './index.less'; // 引入样式文件

// 定义二级表格数据类型
export interface ServerItem {
  agent_id: string;
  args: string;
  command: string;
  connection_type: 'sse' | 'stdio';
  config_id: string;
  create_date: string;
  mcp_server_name: string;
  mcp_tools_info: string;
  server_url: string;
  status: number;
  update_date: string;
  headers: string;
}
interface TableParams {
  pagination: any;
  filters?: any;
  sortOrder?: any;
  sortField?: any;
}
// 定义一级表格数据类型
interface A2AItem {
  agent_name: string;
  agent_url: string;
  agent_id: string;
  agent_type: number;
  type: number;
  status: string;
  create_date: string;
  update_date: string;
  description: string;
  mcp_configs: ServerItem[];
}

const agentTypeMap: Record<number, string> = {
  0: '意图识别智能体',
  1: '规划智能体',
  2: '执行智能体',
  3: '反馈智能体',
  4: '问题智能体',
};

const A2ATable: React.FC = () => {
  const { t } = useTranslate('setting');
  const { t: t_t } = useTranslation();
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [modalVisible, setModalVisible] = useState(false);
  const [editingServer, setEditingServer] = useState<string | null>(null);
  const [searchText, setSearchText] = useState('');
  const [reloading, setReloading] = useState(false);
  // const [pagination, setPagination] = useState({
  //   current: 1,
  //   pageSize: 10,
  //   total: 0,
  // });
  const [isShowA2AModal, setIsShowA2AModal] = useState(false);
  const [a2aForm] = Form.useForm();

  // 示例数据
  const [tableData, setTableData] = useState<A2AItem[]>();

  // 控制展开状态
  const [expandedKeys, setExpandedKeys] = useState<string[]>([]);
  const [editStatus, setEditStatus] = useState(-1); // -1表示没有编辑状态
  const [currentAgentId, setCurrentAgentId] = useState<string | null>(null);
  const [currentMcpId, setCurrentMcpId] = useState<string | null>(null);
  //表格分页参数
  const [tableParams, setTableParams] = useState<TableParams>({
    pagination: {
      current: 1,
      pageSize: 10,
      showSizeChanger: true,
      pageSizeOptions: ['5', '10', '20'],
      total: '',
      showTotal: (total: any) => t_t('schemas.total', { total }),
    },
  });
  useEffect(() => {
    // 初始化数据
    fetchA2aList();
  }, []);
  // 关闭模态框
  const closeModal = () => {
    setModalVisible(false);
    setEditStatus(-1);
    setEditingServer(null);
    form.resetFields();
  };
  // 保存服务器配置 - 添加防抖处理
  const saveServerConfig = useMemo(
    () =>
      _.debounce(
        async () => {
          try {
            const values = await form.validateFields();
            const { command, args, type, url, serverId } = values;
            const serverConfig = {
              connection_type: type,
              server_url: url || '',
              command: command || '',
              args: args
                ? args.split(' ').filter((arg: string) => arg.trim())
                : [],
              mcp_server_name: serverId || '',
              agent_id: currentAgentId || '',
              headers: values.headers,
            };

            if (editingServer) {
              const res = await mcpService.updateServerConfig(
                currentMcpId || '',
                serverConfig,
              );
              if (res.data?.message === 'success') {
                message.success('服务器配置更新成功');
                closeModal();
                fetchA2aList();
              }
            } else {
              const res = await mcpService.createMcpConfig(serverConfig);
              const responseData = res.data;
              if (responseData?.message === 'success') {
                message.success('服务器配置添加成功');
                closeModal();
                fetchA2aList();
              }
            }
          } catch (error) {
            // message.error('保存配置失败');
          }
        },
        500,
        { leading: false, trailing: true },
      ),
    [
      currentAgentId,
      currentMcpId,
      editingServer,
      form,
      fetchA2aList,
      closeModal,
    ],
  );

  // 打开添加/编辑模态框
  const openModal = (
    agent_id: string,
    config_id?: string,
    serverId?: string,
  ) => {
    setEditingServer(serverId || null);
    setCurrentAgentId(agent_id || null);
    setCurrentMcpId(config_id || null);
    if (serverId) {
      const agent = tableData?.find((item) => item.agent_id === agent_id);
      const server = agent?.mcp_configs?.find(
        (s) => s.mcp_server_name === serverId,
      );

      if (server) {
        form.setFieldsValue({
          serverId: server.mcp_server_name,
          type: server.connection_type,
          url: server.server_url,
          command: server.command,
          headers: server.headers,
          args:
            server.args && Array.isArray(server.args)
              ? server.args.join(' ')
              : '',
        });
      }
    } else {
      form.setFieldsValue({
        type: null,
        command: '',
        args: '',
        url: '',
        serverId: '',
        headers: '',
      });
    }
    setModalVisible(true);
  };

  // 删除服务器配置
  const deleteServerConfig = async (mcpId: string) => {
    try {
      await mcpService.deleteServerConfig(mcpId || '');
      message.success('服务器配置删除成功');
      // 如果当前页没有数据了，回到上一页
      // const newTotal = pagination.total - 1;
      // const maxPage = Math.ceil(newTotal / pagination.pageSize);
      // const targetPage =
      //   pagination.current > maxPage
      //     ? Math.max(1, maxPage)
      //     : pagination.current;
      fetchA2aList();
    } catch (error) {
      // message.error('删除配置失败');
    }
  };

  // 重新加载配置
  const reloadConfig = async (serverId?: string) => {
    setReloading(true);
    try {
      await mcpService.reloadMcpConfig(serverId);
      message.success(
        serverId ? `服务器 ${serverId} 重载成功` : '所有服务器重载成功',
      );
      // fetchMcpConfig(pagination.current, pagination.pageSize, searchText);
    } catch (error) {
      // message.error('重新加载配置失败');
    } finally {
      setReloading(false);
    }
  };

  // 一级表格重载操作
  const handleReload = async () => {
    setReloading(true);
    const res = await userService.reloadAgent();
    setReloading(false);
    if (res.data.code === 0) {
      message.success({
        content: `A2A重载完成`,
      });
    }
    // setTimeout(() => {
    //   message.success({
    //     content: `A2A 重载完成`,
    //   });
    // }, 800);
  };

  // 二级表格重载操作
  const handleServerReload = async (agent_id: string, url: string) => {
    (async () => {
      setReloading(true);
      try {
        const res = await mcpService.reloadMcp(
          {
            agent_id: agent_id,
          },
          url + '/update_mcp_config',
        );
        if (res?.data?.code === 0) {
          let msg = res.data.data;
          if (msg?.unconnect_mcp_tools && msg?.unconnect_mcp_tools.length > 0) {
            message.error({
              content: `${msg?.unconnect_mcp_tools.join()}重载失败`,
            });
            return;
          }
          message.success({
            content: `重载完成`,
          });
        }
      } catch (error) {
        // console.error('重载MCP失败:', error);
        message.error('重载失败，请稍后重试');
      } finally {
        setReloading(false);
      }
    })();
  };

  // 二级表格列定义
  const serverColumns = [
    {
      title: 'MCP名称',
      dataIndex: 'mcp_server_name',
      key: 'mcp_server_name',
      width: '15%',
      ellipsis: false,
      render: (text: string) => (
        <div
          style={{
            wordWrap: 'break-word',
            wordBreak: 'break-all',
            whiteSpace: 'normal',
            lineHeight: 1.5,
          }}
        >
          {text}
        </div>
      ),
    },
    {
      title: '类型',
      dataIndex: 'connection_type',
      key: 'connection_type',
      width: '10%',
    },
    {
      title: '服务器URL',
      dataIndex: 'server_url',
      key: 'server_url',
      width: '25%',
      ellipsis: false, // 禁用省略号
      render: (text: string) => (
        <div
          style={{
            wordWrap: 'break-word',
            wordBreak: 'break-all',
            whiteSpace: 'normal',
            lineHeight: 1.5,
          }}
        >
          {text}
        </div>
      ),
    },
    // {
    //   title: '请求头',
    //   dataIndex: 'headers',
    //   key: 'headers',
    //   render: (_: any, record: ServerItem) => (
    //     <pre
    //       style={{
    //         maxWidth: '500px', // 强制内容最大宽度
    //         wordBreak: 'break-all', // 长文本自动换行
    //       }}
    //     >
    //       {record.headers}
    //     </pre>
    //   ),
    // },
    {
      title: '操作',
      key: 'action',
      width: '5%',
      render: (_: any, record: ServerItem, a2aKey: string) => (
        <Space size="small">
          <Button
            type="text"
            icon={<EditOutlined />}
            onClick={() =>
              openModal(
                record.agent_id,
                record.config_id,
                record.mcp_server_name,
              )
            }
            disabled={reloading}
            className="text-blue-600"
          >
            编辑
          </Button>
          <Popconfirm
            title="确定要删除这个服务器吗？"
            onConfirm={() => deleteServerConfig(record.config_id)}
            okText="是"
            cancelText="否"
          >
            <Button
              type="text"
              icon={<DeleteOutlined />}
              disabled={reloading}
              className="text-red-600"
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  e.preventDefault();
                }
              }}
            >
              删除
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];
  //新增智能体
  const handleA2AModal = useMemo(() => {
    return _.debounce((val: boolean, editStatus: number, agent_id?: string) => {
      setIsShowA2AModal(val);
      setEditStatus(editStatus);
      setCurrentAgentId(agent_id || null);
      a2aForm.resetFields();
      if (editStatus === 1) {
        const agent = tableData?.find((item) => item.agent_id === agent_id);
        if (agent) {
          a2aForm.setFieldsValue({
            agent_name: agent.agent_name,
            agent_url: agent.agent_url,
          });
        }
      }
    }, 500);
  }, [a2aForm, tableData]);

  // 切换展开/折叠状态
  const toggleExpand = (key: string) => {
    setExpandedKeys((prev) =>
      prev.includes(key) ? prev.filter((k) => k !== key) : [...prev, key],
    );
  };

  // 一级表格列定义
  const columns = [
    {
      title: 'agent名称',
      dataIndex: 'agent_name',
      key: 'agent_name',
      width: '10%',
      ellipsis: false,
      render: (text: string) => (
        <div
          style={{
            wordWrap: 'break-word',
            wordBreak: 'break-all',
            whiteSpace: 'normal',
            lineHeight: 1.5,
          }}
        >
          {text}
        </div>
      ),
    },
    {
      title: '类型',
      dataIndex: 'agent_type',
      key: 'agent_type',
      width: '10%',
      render: (_: any, record: A2AItem) => {
        return agentTypeMap[record.agent_type];
      },
    },
    {
      title: '地址',
      dataIndex: 'agent_url',
      key: 'agent_url',
      width: '35%',
      ellipsis: false, // 禁用省略号
      render: (text: string) => (
        <div
          style={{
            wordWrap: 'break-word',
            wordBreak: 'break-all',
            whiteSpace: 'normal',
            lineHeight: 1.5,
          }}
        >
          {text}
        </div>
      ),
    },
    {
      title: '操作',
      key: 'action',
      width: '5%',
      render: (_: any, record: A2AItem) => (
        <Space size="small">
          <Button
            type="text"
            icon={
              expandedKeys.includes(record.agent_id) ? (
                <UpOutlined />
              ) : (
                <DownOutlined />
              )
            }
            onClick={() => toggleExpand(record.agent_id)}
            className="text-blue-600 hover:text-blue-800"
            disabled={record.agent_type !== 2}
          >
            MCP
          </Button>
          {/* <Button
            type="text"
            icon={<EditOutlined />}
            onClick={() => handleA2AModal(true, 1, record.agent_id)}
            className="text-blue-600 hover:text-blue-800"
            disabled={record.agent_type !== 2}
          >
            编辑
          </Button>
          <Popconfirm
            title="确定要删除这一行吗？"
            onConfirm={() => deleteA2AConfig(record.agent_id)}
            okText="是"
            cancelText="否"
            disabled={record.agent_type !== 2}
          >
            <Button
              type="text"
              icon={<DeleteOutlined />}
              className="text-red-600 hover:text-red-800"
              disabled={record.agent_type !== 2}
            >
              删除
            </Button>
          </Popconfirm> */}
        </Space>
      ),
    },
  ];

  // 渲染二级表格
  const renderSubTable = (record: A2AItem) => (
    <div className="mt-1">
      {/* MCP信息小标题 */}
      <div className="flex justify-between items-center">
        <h1 className="text-gray-700 text-[18px] inline-block">MCP信息</h1>
        <div>
          <Popconfirm
            placement="bottom"
            title="确定要重载吗？"
            onConfirm={() =>
              handleServerReload(record.agent_id, record.agent_url)
            }
            okText="确定"
            cancelText="取消"
          >
            <Button
              type="default"
              icon={<ReloadOutlined />}
              className="text-green-600 hover:text-green-800 mr-1"
              //disabled={record.mcp_configs.length === 0}
              loading={reloading}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  e.preventDefault();
                }
              }}
            >
              重载
            </Button>
          </Popconfirm>
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => openModal(record.agent_id)}
            disabled={record.mcp_configs.length === 5}
          >
            新增MCP
          </Button>
        </div>
      </div>
      <Table
        columns={serverColumns}
        dataSource={record.mcp_configs as any}
        pagination={false}
        rowKey="mcp_server_name"
        size="small"
        className="mt-1 border-t border-gray-100"
        style={{
          border: '1px solid #f0f0f0',
          borderRadius: '4px',
          boxShadow: '0 1px 2px rgba(0,0,0,0.05)',
        }}
      />
    </div>
  );

  const renderCardTitle = () => {
    return (
      <div className="flex justify-between items-center w-full">
        {/* 左侧标题 */}
        <h4 className="font-semibold text-gray-800 m-0">智能体信息</h4>
        {/* <div>
          <Popconfirm
            placement="bottom"
            title="确定要重载吗？"
            onConfirm={() => handleReload()}
            okText="确定"
            showCancel={false}
          >
            <Button
              type="default"
              icon={<ReloadOutlined />}
              className="text-green-600 hover:text-green-800 mr-1"
              loading={reloading}
              disabled={tableData?.length === 0}
            >
              重载
            </Button>
          </Popconfirm>
          
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => handleA2AModal(true, 0)}
          >
            新增智能体
          </Button>
        </div> */}
      </div>
    );
  };

  const fetchA2aList = async (pageParams?: {
    current: number;
    pageSize: number;
  }) => {
    // 使用传入的分页参数或当前状态的分页参数
    const currentPage = pageParams?.current || tableParams.pagination.current;
    const pageSize = pageParams?.pageSize || tableParams.pagination.pageSize;
    const res = await mcpService.getA2AList({
      page: currentPage,
      pageSize: pageSize,
    });
    if (res?.data?.code === 0) {
      const a2aList = res?.data?.data?.agents || [];
      const formattedData = a2aList.map((item: any, index: number) => {
        return {
          ...item,
          index: index,
        };
      });
      setTableData(formattedData);
      setTableParams({
        ...tableParams,
        pagination: {
          ...tableParams.pagination,
          current: currentPage,
          pageSize: pageSize,
          total: res.data?.data?.total,
        },
      });
    } else {
      // message.error('获取A2A列表失败');
    }
  };

  const deleteA2AConfig = async (agent_id: string) => {
    const res = await mcpService.deleteA2AConfig(agent_id);
    if (res?.data?.code === 0) {
      message.success('A2A配置删除成功');
      fetchA2aList(); // 重新获取A2A列表
    } else {
      // message.error('A2A配置删除失败');
    }
  };

  // 使用useMemo创建防抖版本的onA2ASave函数
  const onA2ASave = useMemo(() => {
    return _.debounce(async (values: any) => {
      let res = null;
      const currentAgent = tableData?.find(
        (item) => item.agent_id === currentAgentId,
      );
      if (editStatus === 1) {
        res = await mcpService.updateA2AConfig({
          agent_id: currentAgent?.agent_id,
          ...values,
        });
      } else {
        res = await mcpService.createA2AConfig(values);
      }
      if (res?.data?.code === 0) {
        message.success('A2A配置更新成功');
        a2aForm.resetFields();
      }
      // else {
      //   message.error('A2A配置更新失败');
      // }
      fetchA2aList(); // 重新获取A2A列表
      // 关闭A2A配置模态框
      setIsShowA2AModal(false);
    }, 500);
  }, [tableData, currentAgentId, editStatus, a2aForm, fetchA2aList]);

  // handleA2AModal函数不需要防抖，因为它只是控制模态框显示/隐藏
  //表格分页
  const handleTableChange = (pagination: any, filters: any, sorter: any) => {
    setTableParams({
      pagination,
      filters,
      sortOrder: Array.isArray(sorter) ? undefined : sorter.order,
      sortField: Array.isArray(sorter) ? undefined : sorter.field,
    });

    // 当分页变化时重新获取数据
    fetchA2aList(pagination);
    if (pagination.pageSize !== tableParams.pagination?.pageSize) {
      setTableData([]);
    }
  };
  return (
    <Card
      title={renderCardTitle()}
      className="w-full mx-auto max-w-6xl shadow-md m-0 overflow-scroll"
    >
      <Table
        columns={columns}
        dataSource={tableData}
        expandable={{
          expandedRowRender: renderSubTable,
          expandedRowKeys: expandedKeys,
          onExpand: (expanded, record) => {
            toggleExpand(record.agent_id);
          },
          expandIcon: () => null,
          showExpandColumn: false,
        }}
        bordered
        rowKey="agent_id"
        // size="middle"
        pagination={tableParams.pagination}
        onChange={handleTableChange}
        style={{ border: '1px solid #f0f0f0' }}
      />
      <McpConfigModal
        modalVisible={modalVisible}
        editingServer={editingServer}
        saveServerConfig={() => saveServerConfig()}
        closeModal={() => closeModal()}
        form={form}
      ></McpConfigModal>
      <A2AConfigModal
        modalVisible={isShowA2AModal}
        closeModal={() => handleA2AModal(false, -1)}
        onSave={onA2ASave}
        form={a2aForm}
      ></A2AConfigModal>
    </Card>
  );
};

export default A2ATable;
