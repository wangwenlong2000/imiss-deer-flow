import { Form, FormInstance, Input, Modal } from 'antd';
import 'antd/dist/reset.css';
import React from 'react';

// 定义表单数据类型
interface FormValues {
  command: string;
  args: string;
  serverId: string;
  type: string;
  url?: string; // SSE 服务器 URL
  // STDIO 服务器不需要 URL
}

interface ModalFormProps {
  editingServer?: string | null;
  modalVisible: boolean;
  onSave: (e: any) => void;
  closeModal: () => void;
  form: FormInstance<FormValues>;
}

const A2AConfigModal: React.FC<ModalFormProps> = ({
  editingServer,
  modalVisible,
  onSave,
  closeModal,
  form,
}) => {
  // 状态管理

  return (
    <Modal
      title={editingServer ? '编辑智能体配置' : '添加智能体配置'}
      open={modalVisible}
      onOk={() => form.validateFields().then((values) => onSave(values))}
      onCancel={closeModal}
      width={600}
      okText="保存"
      cancelText="取消"
      maskClosable={false}
    >
      <Form form={form} layout="vertical">
        <Form.Item
          name="agent_name"
          label="agent名称"
          rules={[
            { required: true, message: '请输入agent的名字' },
            {
              pattern: /^[a-zA-Z0-9_\u4e00-\u9fa5-]+$/,
              message: '智能体名称只能包含字母、数字、下划线、连字符和中文字符',
            },
            { max: 50, message: '智能体名称不能超过50个字符' },
          ]}
        >
          <Input placeholder="请输入agent名字" disabled={!!editingServer} />
        </Form.Item>

        <Form.Item
          name="agent_url"
          label="地址"
          rules={[
            { required: true, message: '请输入地址' },
            { type: 'url', message: '请输入有效的URL' },
          ]}
        >
          <Input placeholder="请输入agent的地址" disabled={!!editingServer} />
        </Form.Item>
      </Form>
    </Modal>
  );
};

export default A2AConfigModal;
