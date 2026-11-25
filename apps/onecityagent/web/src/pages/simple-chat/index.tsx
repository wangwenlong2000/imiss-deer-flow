import { Authorization } from '@/constants/authorization';
import { getAuthorization } from '@/utils/authorization-util';
import { RobotOutlined, SendOutlined, UserOutlined } from '@ant-design/icons';
import {
  Avatar,
  Button,
  Card,
  Input,
  List,
  Select,
  Space,
  Spin,
  Switch,
  Typography,
  message,
} from 'antd';
import React, { useEffect, useRef, useState } from 'react';
import './index.less';

const { TextArea } = Input;
const { Title, Paragraph } = Typography;
const { Option } = Select;

interface Message {
  role: 'user' | 'assistant';
  content: string;
  loading?: boolean;
}

const SimpleChat: React.FC = () => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [loading, setLoading] = useState(false);
  const [intent, setIntent] = useState(false);
  const [planStrategy, setPlanStrategy] = useState('step');

  // 当选择plan_kg策略时，自动启用意图识别
  const handlePlanStrategyChange = (value: string) => {
    setPlanStrategy(value);
    if (value === 'plan_kg' && !intent) {
      setIntent(true);
      message.info('知识图谱规划需要启用意图识别，已自动启用');
    }
  };

  // 当关闭意图识别时，如果当前策略是plan_kg，则自动切换到step
  const handleIntentChange = (checked: boolean) => {
    setIntent(checked);
    if (!checked && planStrategy === 'plan_kg') {
      setPlanStrategy('step');
      message.info('已关闭意图识别，策略已自动切换为步骤规划');
    }
  };
  const messagesEndRef = useRef<HTMLDivElement>(null);

  // 滚动到底部
  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleInputChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setInputValue(e.target.value);
  };

  const handleSendMessage = async () => {
    if (!inputValue.trim()) return;

    // 添加用户消息
    const userMessage: Message = { role: 'user', content: inputValue };
    // 添加一个空的助手消息，用于显示加载状态
    const assistantMessage: Message = {
      role: 'assistant',
      content: '',
      loading: true,
    };

    setMessages([...messages, userMessage, assistantMessage]);
    setInputValue('');
    setLoading(true);

    try {
      const response = await fetch('/v1/chat', {
        method: 'POST',
        headers: {
          [Authorization]: getAuthorization(),
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          prompt: inputValue.trim(),
          plan_strategy: planStrategy,
          intent: intent,
        }),
      });

      const data = await response.json();

      if (response.ok && data.result) {
        // 更新助手消息内容
        setMessages((prev) => {
          const newMessages = [...prev];
          newMessages[newMessages.length - 1] = {
            role: 'assistant',
            content: data.result,
            loading: false,
          };
          return newMessages;
        });
      } else {
        // 处理错误
        message.error(data.error || '请求失败');
        // 移除加载中的消息
        setMessages((prev) => prev.slice(0, prev.length - 1));
      }
    } catch (error) {
      console.error('Error:', error);
      message.error('发送消息失败');
      // 移除加载中的消息
      setMessages((prev) => prev.slice(0, prev.length - 1));
    } finally {
      setLoading(false);
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSendMessage();
    }
  };

  return (
    <div className="simple-chat-container">
      <Card className="chat-card">
        <div className="chat-header">
          <Title level={3} className="chat-title">
            简单聊天
          </Title>
          <div className="chat-settings">
            <Space>
              <span title="启用后，系统会先识别用户意图再回答">意图识别:</span>
              <Switch
                checked={intent}
                onChange={handleIntentChange}
                size="small"
              />
              <span title="选择AI规划回答的方式：步骤规划(按步骤思考)、大纲规划(按大纲组织)、知识图谱规划(需启用意图识别)">
                规划策略:
              </span>
              <Select
                value={planStrategy}
                onChange={handlePlanStrategyChange}
                style={{ width: 120 }}
                size="small"
              >
                <Option value="step" title="AI会按照步骤逐步思考问题">
                  步骤规划
                </Option>
                <Option value="outline" title="AI会按照大纲结构组织回答">
                  大纲规划
                </Option>
                <Option
                  value="plan_kg"
                  title="AI会使用知识图谱方式规划回答，需要启用意图识别"
                >
                  知识图谱规划
                </Option>
              </Select>
            </Space>
          </div>
        </div>

        <div className="messages-container">
          <List
            itemLayout="horizontal"
            dataSource={messages}
            renderItem={(item) => (
              <List.Item className={`message-item ${item.role}`}>
                <List.Item.Meta
                  avatar={
                    <Avatar
                      icon={
                        item.role === 'user' ? (
                          <UserOutlined />
                        ) : (
                          <RobotOutlined />
                        )
                      }
                      className={
                        item.role === 'user'
                          ? 'user-avatar'
                          : 'assistant-avatar'
                      }
                    />
                  }
                  description={
                    <div className="message-content">
                      {item.loading ? (
                        <Spin size="small" />
                      ) : (
                        <Paragraph>{item.content}</Paragraph>
                      )}
                    </div>
                  }
                />
              </List.Item>
            )}
          />
          <div ref={messagesEndRef} />
        </div>

        <div className="input-container">
          <TextArea
            value={inputValue}
            onChange={handleInputChange}
            onKeyPress={handleKeyPress}
            placeholder="输入消息..."
            autoSize={{ minRows: 1, maxRows: 4 }}
            disabled={loading}
          />
          <Button
            type="primary"
            icon={<SendOutlined />}
            onClick={handleSendMessage}
            disabled={!inputValue.trim() || loading}
            className="send-button"
          />
        </div>
      </Card>
    </div>
  );
};

export default SimpleChat;
