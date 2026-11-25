import chatService, { paramsMethod } from '@/services/chat-service';
import { useQueryClient } from '@tanstack/react-query';
import { Button, Form, Input, Modal, message } from 'antd';
import React, { useEffect, useRef } from 'react';
interface NewSessionModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSubmit?: (data: { name: string; description: string }) => void;
  dialogInfo: {
    name: string;
    description: string;
    dialog_id: string;
  };
}

const NewSessionModal: React.FC<NewSessionModalProps> = ({
  isOpen,
  onClose,
  onSubmit,
  dialogInfo,
}) => {
  const [form] = Form.useForm();
  const nameInputRef = useRef<Input>(null);

  const queryClient = useQueryClient();
  // 当模态框打开时聚焦到名称输入框
  useEffect(() => {
    if (isOpen && nameInputRef.current) {
      nameInputRef.current.focus();
    }

    form.setFieldsValue({
      ...dialogInfo,
    });
  }, [isOpen]);

  const handleCreateChat = async () => {
    const res = await chatService.createDialog({
      name: form.getFieldValue('name').trim(),
      description: form.getFieldValue('description').trim(),
    });
    if (res.data.code === 0) {
      message.success('会话创建成功');
      onClose();
    } else {
      message.error('会话创建失败，请稍后重试');
    }
  };

  const handleEditChatInfo = async () => {
    const res = await paramsMethod.updateConfig(dialogInfo.dialog_id, {
      name: form.getFieldValue('name').trim(),
      description: form.getFieldValue('description').trim(),
    });
    if (res.data.code === 0) {
      message.success('会话修改成功');
      onClose();
      queryClient.invalidateQueries({
        queryKey: ['fetchDialogList'],
      });
    } else {
      message.error('会话修改失败，请稍后重试');
    }
  };

  // 处理表单提交
  const handleSubmit = () => {
    form
      .validateFields()
      .then(async (values) => {
        if (dialogInfo.name) {
          await handleEditChatInfo();
        } else {
          await handleCreateChat();
        }
        onSubmit?.({
          name: values.name.trim(),
          description: values.description.trim(),
        });
        queryClient.invalidateQueries({ queryKey: ['fetchDialogList'] });
        form.resetFields(); // 重置表单
        onClose();
      })
      .catch((info) => {
        // console.log('表单验证失败:', info);
      });
  };

  return (
    <Modal
      title={dialogInfo.name ? '修改会话' : '创建会话'}
      open={isOpen}
      onCancel={onClose}
      maskClosable={false} // 点击遮罩不关闭
      destroyOnHidden={true} // 关闭时销毁组件
      width={500}
      footer={[
        <Button key="cancel" onClick={onClose}>
          取消
        </Button>,
        <Button
          key="submit"
          type="primary"
          onClick={handleSubmit}
          loading={false}
        >
          {dialogInfo.name ? '确定' : '创建'}
        </Button>,
      ]}
    >
      <Form
        form={form}
        layout="vertical"
        initialValues={{ name: '', description: '' }}
      >
        <Form.Item
          name="name"
          label="会话名称"
          rules={[
            { required: true, message: '请输入会话名称' },
            {
              pattern: /^[-_a-zA-Z0-9\u4e00-\u9fa5]{1,50}$/,
              message:
                '会话名称长度不能超过50个字符，支持中英文、数字、下划线、连字符',
            },
          ]}
        >
          <Input
            ref={nameInputRef}
            placeholder="请输入会话名称"
            allowClear
            // onChange={(e) => {
            //   // 过滤掉空格字符
            //   let value = e.target.value.replace(/\s/g, '');
            //   if (value.length > 50) {
            //     value = value.slice(0, 50);
            //   }
            //   form.setFieldValue('name', value);
            // }}
          />
        </Form.Item>
      </Form>
    </Modal>
  );
};

export default NewSessionModal;
