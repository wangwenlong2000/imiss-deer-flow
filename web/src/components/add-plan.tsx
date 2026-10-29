import { useTranslate } from '@/hooks/common-hooks';
import { mcpService } from '@/services/mcp-service';
import { FileAddOutlined } from '@ant-design/icons';
import { Tooltip, message } from 'antd';
import { useState } from 'react';

const addPlanIcon = (data: { data: { plan: string; scene: string } }) => {
  const [copied, setCopied] = useState(false);
  const { t } = useTranslate('common');

  const createSchema = async () => {
    if (!data || !data.data || !data.data.plan) {
      message.error('Invalid data provided for creating schema');
      return;
    }
    const plan =
      data.data.plan.replace(/'/g, '"').replace(/\/n/g, '').trim() || '[]';
    const res = await mcpService.createSchema({
      scene: data.data.scene,
      plan: JSON.parse(plan),
    });
    if (res.data.code === 0) {
      message.success('方案新增成功');
    } else {
      message.error('方案新增失败');
    }
  };

  return (
    <Tooltip title="添加方案库">
      <FileAddOutlined onClick={(e) => createSchema()} />
    </Tooltip>
  );
};

export default addPlanIcon;
