import { MessageType } from '@/constants/chat';
import chatService, { paramsMethod } from '@/services/chat-service';
import { SseClient } from '@/utils/sse';
import { getPlatformAuthorization } from '@/utils/authorization-util';
import { message } from 'antd';
import { create } from 'zustand';

// 消息类型定义 - 新增type属性
export interface Message {
  id: string;
  content: string;
  role: MessageType;
  timestamp: number;
  order_id: number;
  scene?: string;
  plan?: string;
  type?: string; // 新增type属性，用于存储parsedData.role
}

// SSE状态类型定义
interface SseState {
  // 核心状态
  conversationMap: Record<string, Message[]>;
  sseConnections: Record<string, { isConnected: boolean; client?: SseClient }>;
  planningAgents: Record<string, { plan: string; scene: string }>;
  tempState: Record<
    string,
    {
      currentId: string;
      currentQuestion: string;
      order: number;
      loadingId?: string;
    } // 记录加载状态消息ID
  >;
  sendLoading: Record<string, boolean>;

  // 回调管理
  sseDoneCallbacks: Record<string, (dialogId: string) => void>;

  // 方法
  initConversation: (dialogId: string, initialData: Message[]) => void;
  addUserQuestion: (dialogId: string, content: string) => void;
  addAssistantLoading: (dialogId: string) => void;
  startSSE: (dialogId: string, userId: string) => void;
  startDeerFlowSSE: (dialogId: string, userId: string) => void;
  updateSseData: (dialogId: string, parsedData: any) => void;
  closeSSE: (dialogId: string) => void;
  saveConversation: (dialogId: string, item: Message) => Promise<void>;
  batchSaveConversation: (dialogId: string) => Promise<void>;
  updatePlanningAgent: (
    dialogId: string,
    data: { plan: string; scene: string },
  ) => void;
  resetTempState: (dialogId: string) => void;
  registerSseDoneCallback: (
    dialogId: string,
    callback: (dialogId: string) => void,
  ) => void;
  unregisterSseDoneCallback: (dialogId: string) => void;
}

export const useSseStore = create<SseState>((set, get) => ({
  // 初始化状态
  conversationMap: {},
  sseConnections: {},
  planningAgents: {},
  tempState: {},
  sendLoading: {},
  sseDoneCallbacks: {},

  // 初始化对话数据
  initConversation: (dialogId, initialData) => {
    const { conversationMap, tempState, sendLoading, planningAgents } = get();
    if (!conversationMap[dialogId]) {
      set({
        conversationMap: { ...conversationMap, [dialogId]: initialData },
        tempState: {
          ...tempState,
          [dialogId]: {
            currentId: '',
            currentQuestion: '',
            order: initialData.length
              ? Math.max(...initialData.map((item) => item.order_id)) + 1
              : 0,
          },
        },
        sendLoading: { ...sendLoading, [dialogId]: false },
        planningAgents: {
          ...planningAgents,
          [dialogId]: planningAgents[dialogId] || { plan: '', scene: '' },
        },
      });
    }
  },

  // 添加用户问题
  addUserQuestion: (dialogId, content) => {
    const { conversationMap, tempState } = get();
    const currentDialogData = conversationMap[dialogId] || [];
    const { order } = tempState[dialogId] || { order: 0 };

    const newQuestion: Message = {
      id: `user-${Date.now()}`,
      content,
      role: MessageType.User,
      timestamp: Date.now(),
      order_id: order,
      type: '我', // 设置用户消息类型
    };

    // 将新问题添加到对话数组
    const updatedDialogData = [...currentDialogData, newQuestion];

    set({
      conversationMap: {
        ...conversationMap,
        [dialogId]: updatedDialogData,
      },
      tempState: {
        ...tempState,
        [dialogId]: {
          ...tempState[dialogId],
          currentQuestion: content,
          order: order + 1,
        },
      },
    });

    get().saveConversation(dialogId, newQuestion);
  },

  // 添加助手加载状态
  addAssistantLoading: (dialogId) => {
    const { conversationMap, tempState, sendLoading } = get();
    const currentDialogData = conversationMap[dialogId] || [];
    const { order } = tempState[dialogId] || { order: 0 };
    const loadingId = `loading-${Date.now()}`;

    // 将加载状态消息添加到对话数组
    const updatedDialogData = [...currentDialogData];

    set({
      conversationMap: {
        ...conversationMap,
        [dialogId]: updatedDialogData,
      },
      tempState: {
        ...tempState,
        [dialogId]: {
          ...tempState[dialogId],
          currentId: '',
          order: order + 1,
          loadingId, // 保存加载状态消息ID，用于后续移除
        },
      },
      sendLoading: { ...sendLoading, [dialogId]: true },
    });
  },

  // 启动SSE连接
  startSSE: (dialogId, _userId) => {
    const { sseConnections, tempState } = get();
    const { currentQuestion } = tempState[dialogId] || { currentQuestion: '' };

    if (sseConnections[dialogId]?.isConnected) {
      get().closeSSE(dialogId);
    }

    // 构建SSE连接URL
    // 构建SSE连接URL
    // 兜底判断：如果当前在城市超脑平台页面，即使调用的是 startSSE，也强制走 /query/deerflow。
    // 这样可以避免页面复用或其他发送入口绕过 chat-container 判断。
    const isCityBrainPage =
      typeof window !== 'undefined' &&
      window.location.pathname.startsWith('/city-brain');
    const queryEndpoint = isCityBrainPage ? '/query/deerflow' : '/query';
    console.log('[city-brain-debug-startSSE]', {
      pathname: typeof window !== 'undefined' ? window.location.pathname : '',
      isCityBrainPage,
      queryEndpoint,
      dialogId,
    });
    const sseUrl = `${queryEndpoint}?query=${encodeURIComponent(currentQuestion)}&conversation_id=${dialogId}`;
    const sseClient = new SseClient(
      sseUrl,
      queryEndpoint === '/query/deerflow'
        ? { Authorization: getPlatformAuthorization() }
        : undefined,
    );

    set({
      sseConnections: {
        ...sseConnections,
        [dialogId]: { isConnected: true, client: sseClient },
      },
    });

    // 建立SSE连接
    sseClient.connect(
      (data) => {
        if (data) {
          const parsedData = typeof data === 'object' ? data : JSON.parse(data);
          get().updateSseData(dialogId, parsedData);

          // SSE完成时触发回调
          if (parsedData.type === '[DONE]') {
            get().closeSSE(dialogId);
            if (parsedData.finished) {
              paramsMethod.finishedConversation(dialogId);
            }

            // 调用注册的回调
            const doneCallback = get().sseDoneCallbacks[dialogId];
            if (doneCallback) {
              doneCallback(dialogId);
            }
          }
        }
      },
      (error) => {
        console.error(`[SSE错误] dialogId=${dialogId}:`, error);
        // message.error(`SSE请求失败: ${error.message || '未知错误'}`);
        message.error(`SSE请求失败 ${error.message || '请联系管理员'}`);
        get().closeSSE(dialogId);
      },
      () => {
        console.log(`[SSE关闭] dialogId=${dialogId}`);
      },
    );
  },


  // 启动城市超脑平台 DeerFlow SSE 连接
  // 保留原 startSSE 走 /query；该方法只给 /city-brain 页面使用，走 /query/deerflow。
  startDeerFlowSSE: (dialogId, _userId) => {
    const { sseConnections, tempState } = get();
    const { currentQuestion } = tempState[dialogId] || { currentQuestion: '' };

    if (sseConnections[dialogId]?.isConnected) {
      get().closeSSE(dialogId);
    }

    const sseUrl = `/query/deerflow?query=${encodeURIComponent(currentQuestion)}&conversation_id=${dialogId}`;
    const sseClient = new SseClient(sseUrl, {
      Authorization: getPlatformAuthorization(),
    });

    set({
      sseConnections: {
        ...sseConnections,
        [dialogId]: { isConnected: true, client: sseClient },
      },
    });

    sseClient.connect(
      (data) => {
        if (data) {
          const parsedData = typeof data === 'object' ? data : JSON.parse(data);
          get().updateSseData(dialogId, parsedData);

          if (parsedData.type === '[DONE]') {
            get().closeSSE(dialogId);
            if (parsedData.finished) {
              paramsMethod.finishedConversation(dialogId);
            }

            const doneCallback = get().sseDoneCallbacks[dialogId];
            if (doneCallback) {
              doneCallback(dialogId);
            }
          }
        }
      },
      (error) => {
        console.error(`[DeerFlow SSE错误] dialogId=${dialogId}:`, error);
        message.error(`城市超脑平台请求失败 ${error.message || '请联系管理员'}`);
        get().closeSSE(dialogId);
      },
      () => {
        console.log(`[DeerFlow SSE关闭] dialogId=${dialogId}`);
      },
    );
  },

  // 更新SSE数据 - 使用push将新内容作为独立消息添加
  updateSseData: (dialogId, parsedData) => {
    const { conversationMap, tempState, planningAgents } = get();
    let currentDialogData = conversationMap[dialogId] || [];
    const { order, loadingId } = tempState[dialogId] || {
      order: 0,
      loadingId: '',
    };

    // 处理智能体规划数据
    let scene = planningAgents[dialogId]?.scene || '';
    let plan = planningAgents[dialogId]?.plan || '';
    if (parsedData.role === '规划智能体') {
      plan = parsedData.content;
      try {
        const starJson = parsedData?.scene_name;
        scene = starJson || '';
      } catch (e) {
        scene = '';
      }
    }
    // if (parsedData.role === '意图识别智能体') {
    //   try {
    //     const starJson = parsedData.content.replace(/'/g, '"');
    //     scene = JSON.parse(starJson)?.scene || '';
    //   } catch (e) {
    //     scene = '';
    //   }
    // }

    // 更新智能体状态
    set({
      planningAgents: {
        ...planningAgents,
        [dialogId]: { scene, plan },
      },
    });

    // 移除加载状态消息（如果存在）
    if (loadingId) {
      currentDialogData = currentDialogData.filter(
        (item) => item.id !== loadingId,
      );
    }

    // 创建新的消息对象
    const newMessage: Message = {
      id: `assistant-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      content: `${parsedData.role}-${Array.isArray(parsedData.content) ? JSON.stringify(parsedData.content) : parsedData.content || ''}`,
      role: MessageType.Assistant,
      timestamp: Date.now(),
      order_id: order,
      type: parsedData.role || '', // 存储parsedData.role到type属性
      scene: scene,
      plan: plan,
    };

    if (parsedData.type !== '[DONE]') {
      // 将新消息push到对话数组
      const updatedDialogData = [...currentDialogData, newMessage];
      // 更新状态
      set({
        conversationMap: {
          ...conversationMap,
          [dialogId]: updatedDialogData,
        },
        tempState: {
          ...tempState,
          [dialogId]: {
            ...tempState[dialogId],
            currentId: newMessage.id,
            order: order + 1,
            loadingId: '', // 清除加载状态ID，已替换为实际消息
          },
        },
      });
      get().saveConversation(dialogId, newMessage);
    }
  },

  // 关闭SSE连接
  closeSSE: (dialogId) => {
    const { sseConnections, sendLoading } = get();
    const connection = sseConnections[dialogId];

    if (connection?.client) {
      connection.client.close();
    }

    set({
      sseConnections: {
        ...sseConnections,
        [dialogId]: { ...connection, isConnected: false },
      },
      sendLoading: { ...sendLoading, [dialogId]: false },
    });

    get().resetTempState(dialogId);
  },

  // 保存单条对话
  saveConversation: async (dialogId, item) => {
    try {
      await chatService.setConversation({
        dialog_id: dialogId,
        sort_order: item.order_id,
        message: item.role === MessageType.User ? item.content : [],
        reference: item.role === MessageType.Assistant ? item.content : [],
        scene: item.role === MessageType.Assistant ? item.scene : undefined,
        plan: item.role === MessageType.Assistant ? item.plan : undefined,
        done: true,
        type: item.type, // 保存type属性
      });
    } catch (error) {
      console.error(
        `[保存对话失败] dialogId=${dialogId}, itemId=${item.id}:`,
        error,
      );
    }
  },

  // 批量保存对话
  batchSaveConversation: async (dialogId) => {
    const { conversationMap, planningAgents } = get();
    const dialogData = conversationMap[dialogId] || [];
    const agentData = planningAgents[dialogId] || { plan: '', scene: '' };

    if (dialogData.length === 0) {
      message.warning('无对话数据可保存');
      return;
    }

    try {
      await Promise.all(
        dialogData.map((item) =>
          get().saveConversation(dialogId, {
            ...item,
            scene:
              item.role === MessageType.Assistant
                ? agentData.scene
                : item.scene,
            plan:
              item.role === MessageType.Assistant ? agentData.plan : item.plan,
          }),
        ),
      );
    } catch (error) {
      console.error(`[批量保存失败] dialogId=${dialogId}:`, error);
      message.error('对话保存失败，请稍后重试');
    }
  },

  // 更新智能体规划数据
  updatePlanningAgent: (dialogId, data) => {
    const { planningAgents } = get();
    set({
      planningAgents: { ...planningAgents, [dialogId]: data },
    });
  },

  // 重置临时状态
  resetTempState: (dialogId) => {
    const { tempState } = get();
    set({
      tempState: {
        ...tempState,
        [dialogId]: {
          ...tempState[dialogId],
          currentId: '',
          currentQuestion: '',
          loadingId: '',
        },
      },
    });
  },

  // 注册SSE完成回调
  registerSseDoneCallback: (dialogId, callback) => {
    const { sseDoneCallbacks } = get();
    set({
      sseDoneCallbacks: { ...sseDoneCallbacks, [dialogId]: callback },
    });
  },

  // 移除SSE完成回调
  unregisterSseDoneCallback: (dialogId) => {
    const { sseDoneCallbacks } = get();
    const newCallbacks = { ...sseDoneCallbacks };
    delete newCallbacks[dialogId];
    set({ sseDoneCallbacks: newCallbacks });
  },
}));

export const resetSseSession = () => {
  Object.values(useSseStore.getState().sseConnections).forEach((connection) => {
    connection.client?.close();
  });
  useSseStore.setState({
    conversationMap: {},
    sseConnections: {},
    planningAgents: {},
    tempState: {},
    sendLoading: {},
    sseDoneCallbacks: {},
  });
};
