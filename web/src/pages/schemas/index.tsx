// 原 SchemesPage.tsx（改造后）
import { useFetchUserInfo } from '@/hooks/user-setting-hooks';
import { mcpService } from '@/services/mcp-service';
import { DeleteOutlined, EditOutlined, PlusOutlined } from '@ant-design/icons';
import {
  Button,
  Card,
  Input,
  Popconfirm,
  Space,
  Table,
  Typography,
  message,
} from 'antd';
import 'antd/dist/reset.css';
import React, { useEffect, useState } from 'react';
// 引入拆分后的 SchemeModal 组件
import type { GetProps } from 'antd';
import { useTranslation } from 'react-i18next';
import SchemeModal from './Modal';
type SearchProps = GetProps<typeof Input.Search>;
const { Search } = Input;
// 定义方案数据类型
interface Scheme {
  id: string;
  scene: string;
  plan: string | string[];
  owner: 'public' | 'person';
}
// 添加缺失的类型定义
interface TableParams {
  pagination: any;
  filters?: any;
  sortOrder?: any;
  sortField?: any;
}
const planMap = {
  person: '个人',
  public: '内置',
};

const { Title, Text } = Typography;
const SchemesPage: React.FC = () => {
  const { t } = useTranslation();
  // 方案列表数据
  const [schemes, setSchemes] = useState<Scheme[]>([]);
  // 搜索的场景名称
  const [searchScene, setSearchScene] = useState<string | undefined>(undefined);
  const [searchValue, setSearchValue] = useState('');
  // 模态框状态（仅管理显隐，内容由子组件处理）
  const [modalVisible, setModalVisible] = useState(false);
  // 模态框标题（区分新增/编辑）
  const [modalTitle, setModalTitle] = useState('新增方案');
  // 当前编辑的方案（null表示新增）
  const [currentEditScheme, setCurrentEditScheme] = useState<Scheme | null>(
    null,
  );
  //表格分页参数
  const [tableParams, setTableParams] = useState<TableParams>({
    pagination: {
      current: 1,
      pageSize: 10,
      showSizeChanger: true,
      pageSizeOptions: ['5', '10', '20'],
      total: '',
      showTotal: (total: any) => t('schemas.total', { total }),
    },
  });
  const { data } = useFetchUserInfo();

  // 1. 获取方案列表
  const getSchemaList = React.useCallback(
    async (
      pageParams?: {
        current: number;
        pageSize: number;
      },
      scene?: string,
    ) => {
      if (!data.id) return;
      // 使用传入的分页参数或默认值
      const currentPage = pageParams?.current ?? 1;
      const pageSize = pageParams?.pageSize ?? 10;

      let sceneParam: string | undefined;
      if (scene === '') {
        sceneParam = undefined;
      } else {
        sceneParam = scene !== undefined ? scene : searchScene;
      }

      const res = await mcpService.getSchemaList({
        page: currentPage,
        pageSize: pageSize,
        user_id: data.id,
        scene: sceneParam,
      });
      if (res.data.code === 0) {
        const schemaList = res.data?.data?.data;
        setSchemes(schemaList);
        setTableParams((prev) => ({
          ...prev,
          pagination: {
            ...prev.pagination,
            current: currentPage,
            pageSize: pageSize,
            total: res.data?.data?.total,
          },
        }));
      }
    },
    [data.id, searchScene],
  );

  // 初始化数据：当 data.id 变化时触发
  useEffect(() => {
    if (data?.id) {
      getSchemaList();
    }
  }, [data?.id]);

  // 当搜索场景变化时，保持当前分页重新获取数据
  useEffect(() => {
    if (data?.id) {
      getSchemaList(tableParams.pagination);
    }
  }, [data?.id, searchScene]);

  // 2. 打开新增模态框
  const handleAdd = () => {
    setCurrentEditScheme(null); // 清空编辑数据
    setModalTitle('新增方案');
    setModalVisible(true);
  };

  // 3. 打开编辑模态框
  const handleEdit = (record: Scheme) => {
    setCurrentEditScheme(record); // 传递当前编辑的方案
    setModalTitle('编辑方案');
    setModalVisible(true);
  };

  // 4. 取消模态框（仅控制显隐）
  const handleModalCancel = () => {
    setModalVisible(false);
    setCurrentEditScheme(null); // 清空编辑数据
  };

  // 5. 新增方案（子组件提交回调）
  const createNewSchema = async (scene: string, planList: string[]) => {
    const res = await mcpService.createSchema({
      scene,
      plan: planList,
    });
    if (res.data.code === 0) {
      message.success('方案新增成功');
      setModalVisible(false); // 关闭模态框
      const newPagination = {
        ...tableParams.pagination,
        current: 1,
      };
      getSchemaList(newPagination); // 重新获取列表，保持当前页码
    } else {
      // message.error('方案新增失败');
      throw new Error('新增失败'); // 抛出错误，让子组件感知提交失败
    }
  };

  // 6. 更新方案（子组件提交回调）
  const updatedSchemes = async (scene: string, planList: string[]) => {
    if (!currentEditScheme) return;
    const res = await mcpService.updateSchema({
      id: currentEditScheme.id,
      scene,
      plan: planList,
      owner: currentEditScheme.owner,
    });
    if (res.data.code === 0) {
      message.success('方案更新成功');
      getSchemaList(tableParams.pagination); // 重新获取列表，保持当前页码
      setModalVisible(false); // 关闭模态框
    } else {
      message.error('方案更新失败');
      throw new Error('更新失败'); // 抛出错误，让子组件感知提交失败
    }
  };

  // 7. 提交回调（统一处理新增/编辑）
  const handleModalSubmit = async (scene: string, planList: string[]) => {
    if (currentEditScheme) {
      // 编辑模式：调用更新接口
      await updatedSchemes(scene, planList);
    } else {
      // 新增模式：调用新增接口
      await createNewSchema(scene, planList);
    }
  };

  // 8. 删除方案
  const handleDelete = async (id: string) => {
    const res = await mcpService.deleteSchema({ id });
    if (res.data?.code === 0) {
      message.success('方案已删除');
    }
    getSchemaList(tableParams.pagination); // 重新获取列表，保持当前页码
  };

  // 表格列配置（保持不变）
  const columns = [
    {
      title: '方案名称',
      dataIndex: 'scene',
      key: 'scene',
      width: 200,
      // ellipsis: true,
      render: (text: string) => <Text strong>{text}</Text>,
    },
    {
      title: '方案详情',
      dataIndex: 'plan',
      key: 'plan',
      // width: '60%',
      width: 900,
      render: (plan: string | string[]) => {
        if (!Array.isArray(plan)) return <Text>{plan}</Text>;
        return (
          <div>
            {plan.map((item, index) => (
              <div key={index} className="mb-1">
                <Text type="secondary">{index + 1}. </Text>
                <Text>{item}</Text>
              </div>
            ))}
          </div>
        );
      },
    },
    {
      title: '方案属性',
      dataIndex: 'owner',
      key: 'owner',
      // width: '10%',
      width: 200,
      render: (_: string | string[], record: Scheme) => {
        return <Text>{planMap[record.owner]}</Text>;
      },
    },
    {
      title: '操作',
      key: 'action',
      // width: '10%',

      fixed: 'right',
      render: (_: any, record: Scheme) => (
        <Space size="middle">
          <Button
            type="text"
            icon={<EditOutlined />}
            size="small"
            onClick={() => handleEdit(record)}
          >
            编辑
          </Button>
          <Popconfirm
            title="确定要删除此方案吗？"
            description="删除后将无法恢复，请谨慎操作"
            onConfirm={() => handleDelete(record.id)}
            okText="确认"
            cancelText="取消"
            keyboard={false}
            okButtonProps={{
              onKeyDown: (e) => {
                if (e.key === 'Enter') {
                  e.preventDefault();
                }
              },
            }}
            cancelButtonProps={{
              onKeyDown: (e) => {
                if (e.key === 'Enter') {
                  e.preventDefault();
                }
              },
            }}
          >
            <Button
              type="text"
              danger
              icon={<DeleteOutlined />}
              size="small"
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  e.preventDefault();
                }
              }}
              disabled={record?.owner === 'public'}
            >
              删除
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];
  //表格分页
  const handleTableChange = (pagination: any, filters: any, sorter: any) => {
    setTableParams((prev) => ({
      ...prev,
      pagination: {
        ...prev.pagination,
        ...pagination,
      },
      filters,
      sortOrder: Array.isArray(sorter) ? undefined : sorter.order,
      sortField: Array.isArray(sorter) ? undefined : sorter.field,
    }));

    setTimeout(() => {
      const scrollContainer = document.getElementById('scheme-page-container');
      if (scrollContainer) {
        scrollContainer.scrollTop = 0;
      } else {
        window.scrollTo(0, 0);
      }
    }, 0);

    // 当分页变化时重新获取数据
    getSchemaList(pagination, searchScene);
    if (pagination.pageSize !== tableParams.pagination?.pageSize) {
      setSchemes([]);
    }
  };
  //搜索
  const onSearch: SearchProps['onSearch'] = (value) => {
    const newPagination = {
      ...tableParams.pagination,
      current: 1,
    };
    setTableParams((prev) => ({
      ...prev,
      pagination: newPagination,
    }));
    const raw = value && typeof value === 'string' ? value : '';
    const trimmed = raw.trim();
    setSearchScene(trimmed);
  };
  return (
    <div
      id="scheme-page-container"
      className="max-w-7xl mx-auto max-h-full overflow-y-scroll"
    >
      {/* 页面标题与新增按钮（保持不变） */}
      <div className="flex justify-between items-center mb-2">
        <Title level={2} style={{ margin: 0 }}>
          方案库
        </Title>
        <Button type="primary" icon={<PlusOutlined />} onClick={handleAdd}>
          新增方案
        </Button>
      </div>

      {/* 方案表格（保持不变） */}
      <Card>
        <Search
          placeholder="输入方案名称"
          onSearch={onSearch}
          value={searchValue}
          allowClear
          style={{ width: 300, marginBottom: 20 }}
          onChange={(e) => {
            // 过滤掉空格字符
            const filteredValue = e.target.value.replace(/\s/g, '');
            // 更新状态（组件会自动同步到输入框）
            setSearchValue(filteredValue);
          }}
        />
        <Table
          dataSource={schemes}
          columns={columns as any}
          rowKey={(record) => record.id}
          bordered
          pagination={tableParams.pagination}
          onChange={handleTableChange}
          // scroll={{ x: 'max-content' }}
          locale={{ emptyText: '暂无方案数据，请点击"新增方案"添加' }}
        />
      </Card>

      {/* 引入拆分后的 SchemeModal 组件 */}
      <SchemeModal
        visible={modalVisible}
        title={modalTitle}
        // 编辑时传递初始数据，新增时不传（使用默认值）
        initialScene={currentEditScheme?.scene}
        type={currentEditScheme?.owner}
        initialPlanList={
          currentEditScheme
            ? Array.isArray(currentEditScheme.plan)
              ? currentEditScheme.plan
              : [currentEditScheme.plan || '']
            : undefined
        }
        onCancel={handleModalCancel}
        onSubmit={handleModalSubmit}
      />
    </div>
  );
};

export default SchemesPage;
