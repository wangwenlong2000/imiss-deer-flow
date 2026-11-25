import { Form, FormInstance, Input, Modal, Select } from 'antd';
import 'antd/dist/reset.css';
import React from 'react';

// 定义表单数据类型
interface FormValues {
  command: string;
  args: string;
  serverId: string;
  type: string;
  url?: string; // SSE 服务器 URL
  headers?: string; // SSE 服务器 headers
  // STDIO 服务器不需要 URL
}

interface ModalFormProps {
  editingServer?: string | null;
  modalVisible: boolean;
  saveServerConfig: (e: any) => void;
  closeModal: () => void;
  form: FormInstance<FormValues>;
}

const McpConfigModal: React.FC<ModalFormProps> = ({
  editingServer,
  modalVisible,
  saveServerConfig,
  closeModal,
  form,
}) => {
  // 状态管理

  const options = [
    { value: 'sse', label: 'sse' },
    { value: 'streamableHttp', label: 'streamableHttp' },
  ];

  const validateHeader = (_: any, value: string, callback: Function) => {
    if (value === null || value === undefined || value.trim() === '') {
      return callback();
    }
    // 检查请求头字符长度，限制1000个字符
    // 保留所有字符（包括空格和换行符）进行长度计数
    if (value.length > 1000) {
      return callback('请求头不能超过1000个字符');
    }
    try {
      const headers = JSON.parse(value);
      if (typeof headers !== 'object' || Array.isArray(headers)) {
        return callback('请求头不是有效的 JSON 格式');
      }
    } catch (e) {
      return callback('请求头不是有效的 JSON 格式');
    }
    return callback();
  };

  return (
    <Modal
      title={editingServer ? '编辑MCP配置' : '添加MCP配置'}
      open={modalVisible}
      onOk={saveServerConfig}
      onCancel={closeModal}
      width={600}
      okText="保存"
      cancelText="取消"
      maskClosable={false}
    >
      <Form form={form} layout="vertical">
        <Form.Item
          name="serverId"
          label="MCP名称"
          rules={[
            { required: true, message: '请输入MCP名称' },
            {
              pattern: /^[a-zA-Z0-9_-]+$/,
              message: 'MCP名称只能包含字母、数字、下划线和连字符',
            },
            { max: 50, message: 'MCP名称不能超过50个字符' },
          ]}
        >
          <Input
            placeholder="输入MCP名称"
            // onChange={(e) => {
            //   // 过滤掉空格字符
            //   let value = e.target.value.replace(/\s/g, '');
            //   if (value.length > 50) {
            //     value = value.slice(0, 50);
            //   }
            //   form.setFieldValue('serverId', value);
            // }}
          />
        </Form.Item>

        <Form.Item
          name="type"
          label="服务器类型"
          rules={[{ required: true, message: '请选择服务器类型' }]}
        >
          <Select placeholder="选择服务器类型" options={options}></Select>
        </Form.Item>

        <Form.Item
          name="url"
          label="服务器URL"
          rules={[
            { required: true, message: '请输入服务器URL' },
            { type: 'url', message: '请输入有效的URL' },
            { max: 200, message: 'URL长度不能超过200字符' },
            {
              pattern: /^[^\u4e00-\u9fa5\u2600-\u26FF\uD800-\uDFFF]*$/,
              message: 'URL不能包含汉字和表情符号',
            },
          ]}
        >
          <Input
            placeholder="输入服务器URL，例如：http://localhost:3000"
            // onChange={(e) => {
            //   // 过滤掉空格字符
            //   let value = e.target.value.replace(/\s/g, '');
            //   if (value.length > 200) {
            //     value = value.slice(0, 200);
            //   }
            //   form.setFieldValue('url', value);
            // }}
          />
        </Form.Item>

        <Form.Item
          name="headers"
          label="请求头"
          rules={[{ validator: validateHeader }]}
        >
          <Input.TextArea
            placeholder='输入JSON格式 如 { "xxx": "xxx" }'
            rows={4} // 初始显示行数
            // onChange={(e) => {
            //   // 过滤掉空格字符
            //   let value = e.target.value.replace(/\s/g, '');
            //   if (value.length > 1000) {
            //     value = value.slice(0, 1000);
            //   }
            //   form.setFieldValue('headers', value);
            // }}
          />
        </Form.Item>
      </Form>
    </Modal>
  );
};

export default McpConfigModal;
