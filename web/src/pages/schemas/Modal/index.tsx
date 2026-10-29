// @/components/SchemeModal/index.tsx
import { mcpService } from '@/services/mcp-service';
import {
  DeleteOutlined,
  ExclamationCircleFilled,
  PlusOutlined,
  SaveOutlined,
} from '@ant-design/icons';
import { Button, Form, Input, Modal, Space } from 'antd';
import _ from 'lodash';
import React, { useEffect, useMemo } from 'react';
import styles from './index.less';
const { confirm } = Modal;
const { TextArea } = Input;
// 定义方案数据类型（与父组件保持一致，可抽离到公共类型文件）
interface Scheme {
  id: string;
  scene: string;
  plan: string | string[];
  owner: 'public' | 'person';
}

// 定义组件 Props 类型
interface SchemeModalProps {
  visible: boolean; // 模态框显隐状态
  title: string; // 模态框标题（新增/编辑）
  initialScene?: string; // 初始方案名称（编辑时传入）
  initialPlanList?: string[]; // 初始方案详情列表（编辑时传入）
  type?: string; // 方案类型（个人/内置）
  fromChat?: boolean; // 是否来自聊天页面（影响部分文案显示）
  onCancel: () => void; // 取消回调
  onSubmit: (
    scene: string,
    planList: string[],
    isNew?: number,
  ) => Promise<void>; // 提交回调（返回Promise用于处理loading）
}
const SchemeModal: React.FC<SchemeModalProps> = ({
  visible,
  title,
  initialScene = '',
  type = '',
  initialPlanList = [],
  onCancel,
  onSubmit,
  fromChat = false,
}) => {
  const [form] = Form.useForm();

  useEffect(() => {
    if (visible) {
      // 重置表单并设置初始方案名称
      const planListValue = Array.isArray(initialPlanList)
        ? initialPlanList
        : [initialPlanList || ''];
      form.setFieldsValue({ scene: initialScene, planList: planListValue });
      // console.log(initialPlanList, '回显方案详情');
    } else {
      // 模态框关闭时重置状态
      form.resetFields();
    }
  }, [visible, initialScene, initialPlanList, form]);

  const showConfirm = async (id: number) => {
    // 表单基础校验（方案名称必填）
    const { scene, planList: validPlanList } = await form.validateFields();
    // 过滤空的详情项
    confirm({
      title: '是否覆盖当前方案？',
      icon: <ExclamationCircleFilled />,
      content: `本方案 <${scene}> 在当前方案库中已存在，是否将当前内容进行替换`,
      // 禁用自动聚焦到确定按钮，防止回车键触发
      autoFocusButton: null,
      // 禁用键盘ESC键关闭
      keyboard: false,
      // 为确定按钮添加键盘事件处理
      okButtonProps: {
        onKeyDown: (e) => {
          if (e.key === 'Enter') {
            e.preventDefault();
            e.stopPropagation();
          }
        },
      },
      // 为取消按钮添加键盘事件处理
      cancelButtonProps: {
        onKeyDown: (e) => {
          if (e.key === 'Enter') {
            e.preventDefault();
            e.stopPropagation();
          }
        },
      },
      onOk() {
        onSubmit(scene, validPlanList, id);
      },
      onCancel() {
        console.log('Cancel');
      },
    });
  };

  const validateByBE = async (scene: string) => {
    const trimmed = (scene || '').trim();
    if (!trimmed) {
      return undefined;
    }
    const res = await mcpService.searchSchemaExist(trimmed);
    if (res?.data?.code === 0 && res.data.data && res.data.data.id) {
      return res.data.data.id;
    }
    return undefined;
  };

  // 添加防抖处理的提交函数
  const handleSubmit = useMemo(
    () =>
      _.debounce(
        async () => {
          // 表单基础校验（方案名称必填）
          const { scene, planList: validPlanList } =
            await form.validateFields();
          try {
            const id =
              scene && validPlanList.length > 0
                ? await validateByBE(scene)
                : undefined;
            if (id && fromChat) {
              showConfirm(id);
              return;
            }
            // 调用父组件的提交回调（传递校验后的数据）
            await onSubmit(scene, validPlanList);

            // 提交成功后，由父组件控制关闭模态框（避免子组件管理显隐状态）
          } catch (error) {}
        },
        500,
        { leading: false, trailing: true },
      ),
    [form, validateByBE, fromChat, showConfirm, onSubmit],
  );

  return (
    <Modal
      title={title}
      open={visible}
      onCancel={onCancel}
      footer={[
        <Button key="cancel" onClick={onCancel}>
          取消
        </Button>,
        <Button
          key="submit"
          type="primary"
          icon={<SaveOutlined />}
          onClick={handleSubmit}
          loading={false} // 若需loading，可通过Props传递loading状态
        >
          保存
        </Button>,
      ]}
      destroyOnHidden={true}
      maskClosable={false}
      width={600}
    >
      <Form
        form={form}
        layout="vertical"
        style={{
          maxHeight: '50vh',
          overflowY: 'auto',
          padding: '0 8px',
        }}
        initialValues={{
          scene: initialScene,
          planList: Array.isArray(initialPlanList)
            ? initialPlanList
            : [initialPlanList || ''],
        }}
      >
        {/* 方案名称输入框 */}
        <Form.Item
          name="scene"
          label={
            <div>
              <span>方案名称</span>
              <span
                style={{
                  fontSize: '12px',
                  color: '#999',
                  marginTop: '2px',
                  marginLeft: '10px',
                }}
              >
                方案名称影响规划功能，需要与方案详情相关
              </span>
            </div>
          }
          rules={[
            { required: true, message: '请输入方案名称' },

            {
              pattern: /^[-_a-zA-Z0-9\u4e00-\u9fa5]{1,50}$/,
              message:
                '方案名称长度不能超过50个字符，支持中英文、数字、下划线、连字符',
            },
          ]}
        >
          <Input
            placeholder="请输入方案名称"
            disabled={type === 'public'}
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                e.preventDefault();
              }
            }}
            // onChange={(e) => {
            //   // 过滤掉空格字符
            //   let value = e.target.value.replace(/\s/g, '');
            //   if (value.length > 50) {
            //     value = value.slice(0, 50);
            //   }
            //   form.setFieldValue('scene', value);
            // }}
          />
        </Form.Item>

        {/* 方案详情：可增删列表 */}

        <Form.List
          name="planList"
          rules={[
            {
              validator: async (_, names) => {
                if (!names || names.length < 1) {
                  return Promise.reject(new Error('方案详情至少需要填写一项'));
                }
              },
            },
          ]}
        >
          {(fields, { add, remove }, { errors }) => (
            <>
              {fields.map((field, index) => (
                <Form.Item
                  label={index === 0 ? '方案详情' : ''}
                  required={true}
                  key={field.key}
                >
                  <Space
                    className={styles.spaceItemSchemas}
                    direction="horizontal"
                  >
                    <span
                      style={{
                        display: 'inline-block',
                        color: 'rgba(0, 0, 0, 0.45)',
                        marginBottom: '24px',
                      }}
                    >
                      {index + 1}.
                    </span>
                    <Form.Item
                      {...field}
                      key={field.key}
                      validateTrigger={['onChange', 'onBlur']}
                      rules={[
                        {
                          required: true,
                          whitespace: true,
                          message: '请输入详情内容',
                        },
                        {
                          max: 500,
                          message: '最多输入500个字符',
                        },
                      ]}
                    >
                      <TextArea
                        autoSize={true}
                        placeholder={`请输入第${index + 1}项详情`}
                        style={{ width: '100%' }}
                        onKeyDown={(e) => {
                          if (e.key === 'Enter') {
                            e.preventDefault();
                          }
                        }}
                      />
                    </Form.Item>
                    <Button
                      style={{ marginBottom: '24px' }}
                      danger
                      icon={<DeleteOutlined />}
                      size="small"
                      onClick={() => remove(field.name)}
                      disabled={fields.length <= 1}
                    />
                  </Space>
                </Form.Item>
              ))}
              <Form.Item>
                <Button
                  type="dashed"
                  onClick={() => add()}
                  style={{ width: '100%' }}
                  icon={<PlusOutlined />}
                  disabled={fields.length >= 10}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                      e.preventDefault();
                    }
                  }}
                >
                  {fields.length >= 10 ? '最多只能添加10条详情' : '添加详情项'}
                </Button>
                {fields.length >= 10 && (
                  <span
                    style={{
                      color: '#ff4d4f',
                      fontSize: '12px',
                      marginLeft: '8px',
                    }}
                  >
                    已达到最大添加数量限制
                  </span>
                )}
                <Form.ErrorList errors={errors} />
              </Form.Item>
            </>
          )}
        </Form.List>
      </Form>
    </Modal>
  );
};

export default SchemeModal;
