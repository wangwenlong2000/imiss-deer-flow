import defaultPng from '@/assets/picture/default.png';
import MessageInput from '@/components/message-input';
import MessageItem from '@/components/message-item';
import { MessageType } from '@/constants/chat';
import {
  useFetchNextDialog,
  useFetchNextDialogList,
  useGetChatSearchParams,
} from '@/hooks/chat-hooks';
import { useFetchUserInfo } from '@/hooks/user-setting-hooks';
import SchemeModal from '@/pages/schemas/Modal';
import { mcpService } from '@/services/mcp-service';
import { useQueryClient } from '@tanstack/react-query';
import { Button, Flex, message, Spin } from 'antd';
import _ from 'lodash';
import { memo, useEffect, useRef, useState } from 'react';
import {
  useCreateConversationBeforeUploadDocument,
  useGetFileIcon,
  useSendButtonDisabled,
} from '../hooks';
import { buildMessageItemReference } from '../utils';
import styles from './index.less';
// 引入全局SSE状态
import { Message, useSseStore } from '@/utils/sseStore';

interface IProps {
  controller: AbortController;
  currentDialog: any; // 兼容原有props，核心数据源为dialogHistory.data
  setLoading: (v: boolean) => void;
  loading: boolean;
  inputValue: string;
  setInputValue: (val: string) => void;
}

const ChatContainer = ({
  inputValue,
  setInputValue,
  controller,
  currentDialog,
  setLoading,
  loading,
}: IProps) => {
  // 1. 基础路由与接口数据
  const { dialogId, conversationId } = useGetChatSearchParams();
  const { data: dialogList, loading: dialogLoading } = useFetchNextDialogList();
  // const { data: conversation } = useFetchNextConversation();
  const dialogHistory = useFetchNextDialog(); // 核心历史对话数据源

  // 2. 组件本地状态
  // const [inputValue, setInputValue] = useState<string>('');
  const [schemeModalVisible, setSchemeModalVisible] = useState(false);
  // 标记是否是首次加载（避免初始化时触发保存）
  const isFirstMount = useRef<boolean>(true);

  // 3. 业务hooks与工具
  const { data: userInfo } = useFetchUserInfo();
  const { createConversationBeforeUploadDocument } =
    useCreateConversationBeforeUploadDocument();
  const queryClient = useQueryClient();
  useGetFileIcon();
  const sendDisabled = useSendButtonDisabled(inputValue);

  const messageListRef = useRef<HTMLDivElement>(null);

  // 4. 全局SSE状态
  const {
    conversationMap,
    sseConnections,
    planningAgents,
    sendLoading,
    initConversation,
    addUserQuestion,
    addAssistantLoading,
    startSSE,
    startDeerFlowSSE,
    batchSaveConversation, // 批量保存方法
    saveConversation, // 单条保存方法（备用）
    registerSseDoneCallback,
    unregisterSseDoneCallback,
  } = useSseStore();

  // 5. 当前对话数据
  const currentConversationList = conversationMap[dialogId] || [];
  const isSseConnected = sseConnections[dialogId]?.isConnected || false;
  const currentAgentData = planningAgents[dialogId] || { plan: [], scene: '' };
  const currentSendLoading = sendLoading[dialogId] || false;

  // 判断当前是否在城市超脑平台页面。
  // 注意：这里必须在发送时读取 window.location，避免 /chat 与 /city-brain 复用同一个 Chat 组件时闭包拿到旧路径。
  const getIsCityBrainPage = () =>
    typeof window !== 'undefined' &&
    window.location.pathname.startsWith('/city-brain');

  // 注册 SSE 完成回调（组件挂载时注册，卸载时移除）
  useEffect(() => {
    if (!dialogId) return;

    // 回调函数：在组件上下文，可安全调用 queryClient
    const handleSseDone = (currentDialogId: string) => {
      if (currentDialogId === dialogId) {
        // 确保是当前对话的回调
        queryClient.invalidateQueries({ queryKey: ['fetchDialogList'] });
      }
    };

    registerSseDoneCallback(dialogId, handleSseDone);

    // 组件卸载时移除回调，避免内存泄漏
    return () => {
      unregisterSseDoneCallback(dialogId);
    };
  }, [
    dialogId,
    queryClient,
    registerSseDoneCallback,
    unregisterSseDoneCallback,
  ]);

  useEffect(() => {
    const scrollContainer = messageListRef.current;
    if (!scrollContainer) return;
    scrollContainer.scrollTo({
      top: scrollContainer.scrollHeight,
    });
  }, [currentConversationList]);

  useEffect(() => {
    if (!dialogId) return;

    if (!isSseConnected && !dialogHistory.loading) {
      let initialData: Message[] = [];

      // 优先用dialogHistory.data
      if (Array.isArray(dialogHistory.data) && dialogHistory.data.length > 0) {
        initialData = dialogHistory.data.map((item: any): Message => {
          const scene = item.scene || '';
          const plan = item.plan || '';
          if (scene || plan) {
            useSseStore.setState((state) => ({
              planningAgents: {
                ...state.planningAgents,
                [dialogId]: { scene, plan },
              },
            }));
          }
          return {
            id:
              item.id ||
              `msg-${Date.now()}-${Math.random().toString(36).slice(2)}`,
            content: item.message === '[]' ? item.reference : item.message,
            role:
              item.message === '[]' ? MessageType.Assistant : MessageType.User,
            timestamp: item.update_time || Date.now(),
            order_id: item.sort_order || 0,
            scene: scene,
            plan: plan,
          };
        });
      }
      // 降级用currentDialog
      else if (Array.isArray(currentDialog) && currentDialog.length > 0) {
        initialData = currentDialog as Message[];
        const lastAssistantMsg = currentDialog.findLast(
          (item: any) =>
            item.role === MessageType.Assistant && item.scene && item.plan,
        );
        if (lastAssistantMsg) {
          useSseStore.setState((state) => ({
            planningAgents: {
              ...state.planningAgents,
              [dialogId]: {
                scene: lastAssistantMsg.scene,
                plan: lastAssistantMsg.plan,
              },
            },
          }));
        }
      }

      initConversation(dialogId, initialData);
    }

    // 首次加载完成标记
    isFirstMount.current = false;
  }, [dialogId, dialogHistory.data, dialogHistory.loading, isSseConnected]);

  useEffect(() => {
    setLoading(isSseConnected || currentSendLoading || dialogHistory.loading);
  }, [isSseConnected, currentSendLoading, dialogHistory.loading, setLoading]);

  const handlePressEnter = () => {
    if (!inputValue.trim() || isSseConnected || !dialogId || !userInfo?.id) {
      if (!inputValue.trim()) message.warning('请输入消息内容');
      if (isSseConnected) message.warning('当前有请求正在处理，请稍后');
      if (!dialogId) message.warning('对话ID无效，无法发送');
      if (!userInfo?.id) message.warning('用户信息未加载完成');
      return;
    }

    addUserQuestion(dialogId, inputValue);
    addAssistantLoading(dialogId);

    const isCityBrainPage = getIsCityBrainPage();
    console.log('[city-brain-debug]', {
      pathname: typeof window !== 'undefined' ? window.location.pathname : '',
      isCityBrainPage,
      dialogId,
    });

    if (isCityBrainPage) {
      startDeerFlowSSE(dialogId, userInfo.id);
    } else {
      startSSE(dialogId, userInfo.id);
    }

    setInputValue('');
  };

  const handleInputChange = (val: string) => {
    setInputValue(val);
  };

  // 使用lodash防抖处理警告消息
  useEffect(() => {
    const debouncedWarning = _.debounce(() => {
      if (inputValue.length > 500) {
        message.warning('输入内容已超过500字，建议精简后发送');
      }
    }, 300); // 300毫秒防抖延迟

    if (inputValue.length > 500) {
      debouncedWarning();
    }

    return () => {
      debouncedWarning.cancel();
    };
  }, [inputValue]);

  const handleManualSave = () => {
    if (!dialogId || currentConversationList.length === 0) {
      message.warning(dialogId ? '无对话数据可保存' : '对话ID无效，无法保存');
      return;
    }
    batchSaveConversation(dialogId);
  };

  const saveSchema = async () => {
    console.log('currentAgentData', currentAgentData);

    setSchemeModalVisible(true);
  };

  const handleCreateSchema = async (
    scene: string,
    plan: string[],
    id?: number,
  ) => {
    // if (
    //   !currentAgentData.plan ||
    //   !currentAgentData.scene ||
    //   !scene.trim() ||
    //   plan.length === 0
    // ) {
    //   message.error('方案数据不完整（需包含有效场景和规划步骤）');
    //   return;
    // }
    let res;

    if (id) {
      res = await mcpService.updateSchema({
        id: id,
        scene,
        plan: plan,
      });
    } else {
      res = await mcpService.createSchema({
        scene: scene.trim(),
        plan: plan.filter((step) => step.trim()),
      });
    }

    if (res.data.code === 0) {
      message.success(`方案${id ? '修改' : '新增'}成功`);
      setSchemeModalVisible(false);
    } else {
      message.error(`方案新增失败：${res.data.msg || '未知错误'}`);
    }
  };

  //处理JSON报错
  const parsePlanData = (planData: string | undefined): any => {
    return planData || [];
    // const processedData =
    //   (planData || '').replace(/'/g, '"').replace(/\/n/g, '').trim() || '[]';
    // try {
    //   return JSON.parse(processedData) || [];
    // } catch (error) {
    //   return processedData;
    // }
  };
  //清空输入框
  const clearInput = () => setInputValue('');
  // ==========================================================================
  // 视图渲染
  // ==========================================================================
  //查找方案名称
  const findSceneInDictionary = (
    currentScene: string,
    dictionaryArray: Array<string>,
    specificValue: string,
  ): string => {
    // 将当前场景值转换为非空字符串

    const sceneValue = currentScene || '';

    // 检查字典数组中是否存在特定值
    const hasSpecificValue = dictionaryArray.some(
      (item) => item === specificValue,
    );

    // 如果存在特定值，则返回空字符串，否则返回当前场景值
    return hasSpecificValue ? '' : sceneValue;
  };
  return (
    <>
      <Flex flex={1} className={styles.chatContainer} vertical>
        {/* 顶部操作按钮区 */}
        <div className={styles.buttonGroup}>
          {/* <Button
            onClick={handleManualSave}
            className={styles.conversionButton}
            type="default"
            size="middle"
            disabled={!dialogId || currentConversationList.length === 0}
          >
            保存对话
          </Button> */}
          <Button
            onClick={saveSchema}
            className={styles.conversionButton}
            type="primary"
            size="middle"
            disabled={
              !Boolean(currentAgentData?.plan.length > 0) ||
              dialogHistory.loading ||
              isSseConnected ||
              currentSendLoading
            }
          >
            保存方案
          </Button>
        </div>

        {/* 消息列表区 */}
        <Flex
          flex={1}
          vertical
          className={styles.messageContainer}
          ref={messageListRef}
        >
          <div className={styles.messageList}>
            <Spin
              spinning={
                dialogHistory.loading || isSseConnected || currentSendLoading
              }
            >
              {currentConversationList.map((message, index) => (
                <MessageItem
                  key={message.id || `msg-${index}-${message.timestamp}`}
                  loading={
                    message.role === MessageType.Assistant &&
                    (isSseConnected || currentSendLoading) &&
                    index === currentConversationList.length - 1
                  }
                  item={message}
                  nickname={userInfo?.nickname || '用户'}
                  avatar={userInfo?.avatar || defaultPng}
                  reference={buildMessageItemReference(
                    {
                      message: currentConversationList,
                      reference: [],
                      // reference: conversation?.reference || [],
                    },
                    message,
                  )}
                  index={index}
                  sendLoading={currentSendLoading}
                  schemaData={currentAgentData}
                />
              ))}
            </Spin>
          </div>
        </Flex>

        {/* 消息输入框 */}
        <MessageInput
          disabled={
            !dialogId ||
            dialogList?.find((i) => i.dialog_id === dialogId)?.finished ||
            currentSendLoading
          }
          sendDisabled={sendDisabled || isSseConnected || currentSendLoading}
          sendLoading={currentSendLoading}
          value={inputValue}
          onInputChange={handleInputChange}
          onPressEnter={handlePressEnter}
          conversationId={conversationId}
          createConversationBeforeUploadDocument={
            createConversationBeforeUploadDocument
          }
          placeholder="请输入消息内容..."
        />
      </Flex>

      {/* 保存方案模态框 */}
      <SchemeModal
        visible={schemeModalVisible}
        title="保存方案"
        initialScene={findSceneInDictionary(
          currentAgentData.scene || '',
          ['其他场景'],
          currentAgentData.scene,
        )}
        initialPlanList={parsePlanData(currentAgentData.plan)}
        fromChat={true}
        onCancel={() => setSchemeModalVisible(false)}
        onSubmit={handleCreateSchema}
      />
    </>
  );
};

export default memo(ChatContainer);
