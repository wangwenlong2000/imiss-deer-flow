import { ChatSearchParams, MessageType } from '@/constants/chat';
import { fileIconMap } from '@/constants/common';
import {
  useFetchManualConversation,
  useFetchManualDialog,
  useFetchNextConversation,
  useFetchNextConversationList,
  useFetchNextDialog,
  useGetChatSearchParams,
  useRemoveNextConversation,
  useRemoveNextDialog,
  useSetNextDialog,
  useUpdateNextConversation,
} from '@/hooks/chat-hooks';
import {
  useSetModalState,
  useShowDeleteConfirm,
  useTranslate,
} from '@/hooks/common-hooks';
import {
  useRegenerateMessage,
  useSelectDerivedMessages,
  useSendMessageWithSse,
} from '@/hooks/logic-hooks';
import { IConversation, IDialog, Message } from '@/interfaces/database/chat';
import { getFileExtension } from '@/utils';
import { getConversationId } from '@/utils/chat';
import { SseClient } from '@/utils/sse';
import { getPlatformAuthorization } from '@/utils/authorization-util';
import { useMutationState } from '@tanstack/react-query';
import { get } from 'lodash';
import trim from 'lodash/trim';
import { ChangeEventHandler, useCallback, useMemo, useState } from 'react';
import { useSearchParams } from 'umi';
import { v4 as uuid } from 'uuid';
import {
  IClientConversation,
  IMessage,
  VariableTableDataType,
} from './interface';

export type ChatEntryMode = 'event' | 'cityBrain';

export const CHAT_ENTRY_MODE_STORAGE_KEY = 'onecityagent_current_chat_entry_mode';
export const DIALOG_MODE_STORAGE_KEY = 'onecityagent_dialog_mode_map_v1';

const getSafePathname = () => {
  if (typeof window === 'undefined') {
    return '';
  }

  return window.location.pathname || '';
};

export const isCityBrainPage = () => getSafePathname().startsWith('/city-brain');

export const getDialogModeMap = (): Record<string, ChatEntryMode> => {
  if (typeof window === 'undefined') {
    return {};
  }

  try {
    const raw = window.localStorage.getItem(DIALOG_MODE_STORAGE_KEY);
    return raw ? JSON.parse(raw) : {};
  } catch (error) {
    console.warn('[city-brain] failed to read dialog mode map', error);
    return {};
  }
};

export const setDialogMode = (dialogId: string, mode: ChatEntryMode) => {
  if (typeof window === 'undefined' || !dialogId) {
    return;
  }

  try {
    const modeMap = getDialogModeMap();
    modeMap[dialogId] = mode;
    window.localStorage.setItem(DIALOG_MODE_STORAGE_KEY, JSON.stringify(modeMap));
  } catch (error) {
    console.warn('[city-brain] failed to set dialog mode', error);
  }
};

export const removeDialogMode = (dialogId: string) => {
  if (typeof window === 'undefined' || !dialogId) {
    return;
  }

  try {
    const modeMap = getDialogModeMap();
    delete modeMap[dialogId];
    window.localStorage.setItem(DIALOG_MODE_STORAGE_KEY, JSON.stringify(modeMap));
  } catch (error) {
    console.warn('[city-brain] failed to remove dialog mode', error);
  }
};

export const getDialogMode = (dialogId?: string): ChatEntryMode | undefined => {
  if (!dialogId) {
    return undefined;
  }

  return getDialogModeMap()[dialogId];
};

export const getCurrentChatEntryMode = (dialogId?: string): ChatEntryMode => {
  const pathname = getSafePathname();

  // dialogId 标记优先级最高：刷新或历史切换后，即使路径被错误带到 /chat，
  // 只要当前会话标记为 cityBrain，发送仍然必须走 /query/deerflow。
  if (dialogId) {
    const storedMode = getDialogMode(dialogId);
    if (storedMode) {
      return storedMode;
    }
  }

  if (pathname.startsWith('/city-brain')) {
    return 'cityBrain';
  }

  // 当前入口标记优先于 /chat：用于兜底处理同一个 Chat 组件复用时路径状态短暂滞后的情况。
  if (typeof window !== 'undefined') {
    const currentMode = window.localStorage.getItem(CHAT_ENTRY_MODE_STORAGE_KEY);
    if (currentMode === 'cityBrain' || currentMode === 'event') {
      return currentMode;
    }
  }

  if (pathname.startsWith('/chat')) {
    return 'event';
  }

  return 'event';
};

export const getCityBrainQueryUrl = (value: string, dialogId?: string) => {
  const endpoint =
    getCurrentChatEntryMode(dialogId) === 'cityBrain' ? '/query/deerflow' : '/query';

  return `${endpoint}?query=${value}`;
};


export const useSetChatRouteParams = () => {
  const [currentQueryParameters, setSearchParams] = useSearchParams();
  const newQueryParameters: URLSearchParams = useMemo(
    () => new URLSearchParams(currentQueryParameters.toString()),
    [currentQueryParameters],
  );

  const setConversationIsNew = useCallback(
    (value: string) => {
      newQueryParameters.set(ChatSearchParams.isNew, value);
      setSearchParams(newQueryParameters);
    },
    [newQueryParameters, setSearchParams],
  );

  const getConversationIsNew = useCallback(() => {
    return newQueryParameters.get(ChatSearchParams.isNew);
  }, [newQueryParameters]);

  return { setConversationIsNew, getConversationIsNew };
};

export const useSetNewConversationRouteParams = () => {
  const [currentQueryParameters, setSearchParams] = useSearchParams();
  const newQueryParameters: URLSearchParams = useMemo(
    () => new URLSearchParams(currentQueryParameters.toString()),
    [currentQueryParameters],
  );

  const setNewConversationRouteParams = useCallback(
    (conversationId: string, isNew: string) => {
      newQueryParameters.set(ChatSearchParams.ConversationId, conversationId);
      newQueryParameters.set(ChatSearchParams.isNew, isNew);
      setSearchParams(newQueryParameters);
    },
    [newQueryParameters, setSearchParams],
  );

  return { setNewConversationRouteParams };
};

export const useSelectCurrentDialog = () => {
  const data = useMutationState({
    filters: { mutationKey: ['fetchDialog'] },
    select: (mutation) => {
      return get(mutation, 'state.data.data', {});
    },
  });

  return (data.at(-1) ?? {}) as IDialog;
};

export const useSelectPromptConfigParameters = (): VariableTableDataType[] => {
  const { data: currentDialog } = useFetchNextDialog();

  const finalParameters: VariableTableDataType[] = useMemo(() => {
    const parameters = currentDialog?.prompt_config?.parameters ?? [];
    if (!currentDialog.id) {
      // The newly created chat has a default parameter
      return [{ key: uuid(), variable: 'knowledge', optional: false }];
    }
    return parameters.map((x) => ({
      key: uuid(),
      variable: x.key,
      optional: x.optional,
    }));
  }, [currentDialog]);

  return finalParameters;
};

export const useDeleteDialog = () => {
  const showDeleteConfirm = useShowDeleteConfirm();

  const { removeDialog } = useRemoveNextDialog();

  const onRemoveDialog = (
    dialogIds: Array<string>,
    options?: { title?: string },
  ) => {
    showDeleteConfirm({
      title: options?.title,
      onOk: () => removeDialog(dialogIds),
      // 禁用键盘事件，特别是回车键
      keyboard: false,
    });
  };

  return { onRemoveDialog };
};

export const useHandleItemHover = () => {
  const [activated, setActivated] = useState<string>('');

  const handleItemEnter = (id: string) => {
    setActivated(id);
  };

  const handleItemLeave = () => {
    setActivated('');
  };

  return {
    activated,
    handleItemEnter,
    handleItemLeave,
  };
};

export const useEditDialog = () => {
  const [dialog, setDialog] = useState<IDialog>({} as IDialog);
  const { fetchDialog } = useFetchManualDialog();
  const { setDialog: submitDialog, loading } = useSetNextDialog();

  const {
    visible: dialogEditVisible,
    hideModal: hideDialogEditModal,
    showModal: showDialogEditModal,
  } = useSetModalState();

  const hideModal = useCallback(() => {
    setDialog({} as IDialog);
    hideDialogEditModal();
  }, [hideDialogEditModal]);

  const onDialogEditOk = useCallback(
    async (dialog: IDialog) => {
      const ret = await submitDialog(dialog);

      if (ret === 0) {
        hideModal();
      }
    },
    [submitDialog, hideModal],
  );

  const handleShowDialogEditModal = useCallback(
    async (dialogId?: string) => {
      if (dialogId) {
        const ret = await fetchDialog(dialogId);
        if (ret.code === 0) {
          setDialog(ret.data);
        }
      }
      showDialogEditModal();
    },
    [showDialogEditModal, fetchDialog],
  );

  const clearDialog = useCallback(() => {
    setDialog({} as IDialog);
  }, []);

  return {
    dialogSettingLoading: loading,
    initialDialog: dialog,
    onDialogEditOk,
    dialogEditVisible,
    hideDialogEditModal: hideModal,
    showDialogEditModal: handleShowDialogEditModal,
    clearDialog,
  };
};

//#region conversation

// export const useSelectDerivedConversationList = () => {
//   const { t } = useTranslate('chat');

//   const [list, setList] = useState<Array<IConversation>>([]);
//   const { data: currentDialog } = useFetchNextDialog();
//   const { data: conversationList, loading } = useFetchNextConversationList();
//   const { dialogId } = useGetChatSearchParams();
//   const prologue = currentDialog?.prompt_config?.prologue ?? '';
//   const { setNewConversationRouteParams } = useSetNewConversationRouteParams();

//   const addTemporaryConversation = useCallback(() => {
//     const conversationId = getConversationId();
//     setList((pre) => {
//       if (dialogId) {
//         setNewConversationRouteParams(conversationId, 'true');
//         const nextList = [
//           {
//             id: conversationId,
//             name: t('newConversation'),
//             dialog_id: dialogId,
//             is_new: true,
//             message: [
//               {
//                 content: prologue,
//                 role: MessageType.Assistant,
//               },
//             ],
//           } as any,
//           ...conversationList,
//         ];
//         return nextList;
//       }

//       return pre;
//     });
//   }, [conversationList, dialogId, prologue, t, setNewConversationRouteParams]);

//   // When you first enter the page, select the top conversation card

//   useEffect(() => {
//     setList([...conversationList]);
//   }, [conversationList]);

//   return { list, addTemporaryConversation, loading };
// };

export const useOrderId = () => {
  const [order, setOrder] = useState(0);

  const initOrder = (num: number) => {
    setOrder(num);
  };

  const autoIncrement = () => {
    setOrder(order + 1);
  };
};

export const useSelectDerivedConversationList = () => {
  const [conversationName, setConversationName] = useState('');
  const { t } = useTranslate('chat');
  const {
    visible: conversationRenameVisible,
    hideModal: hideConversationRenameModal,
    showModal: showConversationRenameModal,
  } = useSetModalState();
  const [list, setList] = useState<Array<IConversation>>([]);
  const { data: currentDialog } = useFetchNextDialog();
  // window.currentDialog = currentDialog;
  const { data: conversationList, loading } = useFetchNextConversationList();
  const { dialogId } = useGetChatSearchParams();
  const prologue = currentDialog?.prompt_config?.prologue ?? '';
  const { setNewConversationRouteParams } = useSetNewConversationRouteParams();
  const [Ids, setId] = useState('');
  const addTemporaryConversation = useCallback(() => {
    const conversationId = getConversationId();
    setList((pre) => {
      if (dialogId) {
        setNewConversationRouteParams(conversationId, 'true');
        const nextList = [
          {
            id: conversationId,
            name: t('newConversation'),
            dialog_id: dialogId,
            is_new: true,
            message: [
              {
                content: prologue,
                role: MessageType.Assistant,
              },
            ],
          } as any,
          ...conversationList,
        ];
        return nextList;
      }
      return pre;
    });
  }, [conversationList, dialogId, prologue, t, setNewConversationRouteParams]);

  const removeTemporaryConversation = useCallback((id: string) => {
    setList((pre) => pre.filter((conversation) => conversation.id !== id));
  }, []);

  const renameTemporaryConversation = useCallback(
    async (id: string, name: string) => {
      setId(id);
      setConversationName(name);
      showConversationRenameModal();
    },
    [showConversationRenameModal],
  );
  const onConversationRenameOk = useCallback(
    (name: string) => {
      setList((pre) =>
        pre.map((conversation) =>
          conversation.id === Ids
            ? { ...conversation, name: name }
            : conversation,
        ),
      );
      hideConversationRenameModal();
    },
    [Ids],
  );

  // useEffect(() => {
  //   setList([...conversationList]);
  // }, [conversationList]);

  return {
    list,
    loading,
    addTemporaryConversation,
    removeTemporaryConversation,
    renameTemporaryConversation,
    conversationRenameVisible,
    hideConversationRenameModal,
    showConversationRenameModal,
    onConversationRenameOk,
    initialConversationName: conversationName,
  };
};

export const useSetConversation = () => {
  const { dialogId } = useGetChatSearchParams();
  const { updateConversation } = useUpdateNextConversation();

  const setConversation = useCallback(
    async (
      message: string,
      isNew: boolean = false,
      conversationId?: string,
    ) => {
      const data = await updateConversation({
        dialog_id: dialogId,
        name: message,
        is_new: isNew,
        conversation_id: conversationId,
        message: [
          {
            role: MessageType.Assistant,
            content: message,
          },
        ],
      });

      return data;
    },
    [updateConversation, dialogId],
  );

  return { setConversation };
};

export const useSelectNextMessages = () => {
  const {
    ref,
    setDerivedMessages,
    derivedMessages,
    addNewestAnswer,
    addNewestQuestion,
    removeLatestMessage,
    removeMessageById,
    removeMessagesAfterCurrentMessage,
  } = useSelectDerivedMessages();
  const { data: conversation, loading } = useFetchNextConversation();
  const { data: dialog } = useFetchNextDialog();
  const { conversationId, dialogId, isNew } = useGetChatSearchParams();

  const addPrologue = useCallback(() => {
    if (dialogId !== '' && isNew === 'true') {
      const prologue = dialog.prompt_config?.prologue;

      const nextMessage = {
        role: MessageType.Assistant,
        content: prologue,
        id: uuid(),
      } as IMessage;

      setDerivedMessages([nextMessage]);
    }
  }, [isNew, dialog, dialogId, setDerivedMessages]);

  // useEffect(() => {
  //   addPrologue();
  // }, [addPrologue]);

  // useEffect(() => {
  //   if (
  //     conversationId &&
  //     isNew !== 'true' &&
  //     conversation.message?.length > 0
  //   ) {
  //     setDerivedMessages(conversation.message);
  //   }

  //   if (!conversationId) {
  //     setDerivedMessages([]);
  //   }
  // }, [conversation.message, conversationId, setDerivedMessages, isNew]);

  return {
    ref,
    derivedMessages,
    loading,
    addNewestAnswer,
    addNewestQuestion,
    removeLatestMessage,
    removeMessageById,
    removeMessagesAfterCurrentMessage,
  };
};

export const useHandleMessageInputChange = () => {
  const [value, setValue] = useState('');

  const handleInputChange: ChangeEventHandler<HTMLInputElement> = (e) => {
    const value = e.target.value;
    const nextValue = value.replaceAll('\\n', '\n').replaceAll('\\t', '\t');
    setValue(nextValue);
  };

  return {
    handleInputChange,
    value,
    setValue,
  };
};

export const useSendNextMessage = (controller: AbortController) => {
  const { setConversation } = useSetConversation();
  const { conversationId, dialogId, isNew } = useGetChatSearchParams();
  const { handleInputChange, value, setValue } = useHandleMessageInputChange();
  const { data: currentDialog } = useFetchNextDialog();
  const { send, answer, done } = useSendMessageWithSse(getCityBrainQueryUrl(value, dialogId));
  // const { send, answer, done } = useSendMessageWithSse(
  //   api.completeConversation,
  // );
  const {
    ref,
    derivedMessages,
    loading,
    addNewestAnswer,
    addNewestQuestion,
    removeLatestMessage,
    removeMessageById,
    removeMessagesAfterCurrentMessage,
  } = useSelectNextMessages();
  const { setConversationIsNew, getConversationIsNew } =
    useSetChatRouteParams();

  const sendMessage = useCallback(
    async ({
      message,
      currentConversationId,
      messages,
    }: {
      message: Message;
      currentConversationId?: string;
      messages?: Message[];
    }) => {
      const currentId = Math.random().toString();
      addNewestAnswer({
        id: currentId,
        content: '搜索中',
        reference: [],
      });
      const derivedMessage = derivedMessages.find(
        (item) => item.id === `${MessageType.Assistant}_currentId`,
      );
      const sseUrl = getCityBrainQueryUrl(value, dialogId);
      const sseClient = new SseClient(
        sseUrl,
        sseUrl.startsWith('/query/deerflow')
          ? { Authorization: getPlatformAuthorization() }
          : undefined,
      );
      sseClient.connect((res) => {
        const value = JSON.parse(res);
        if (derivedMessage?.content === '搜索中') {
          derivedMessage.content = '';
        }

        derivedMessage.content += value.content;

        if (value.type === '[DONE]') {
          sseClient.close();
        }
      });
    },
    [
      derivedMessages,
      conversationId,
      removeLatestMessage,
      setValue,
      send,
      controller,
      dialogId,
    ],
  );

  const handleSendMessage = useCallback(
    async (message: Message) => {
      const isNew = getConversationIsNew();
      sendMessage({ message });
      // if (isNew !== 'true') {
      //   sendMessage({ message });
      // } else {
      //   const data = await setConversation(
      //     message.content,
      //     true,
      //     conversationId,
      //   );
      //   if (data.code === 0) {
      //     setConversationIsNew('');
      //     const id = data.data.id;
      //     // currentConversationIdRef.current = id;
      //     sendMessage({
      //       message,
      //       currentConversationId: id,
      //       messages: data.data.message,
      //     });
      //   }
      // }
    },
    [
      setConversation,
      sendMessage,
      setConversationIsNew,
      getConversationIsNew,
      conversationId,
    ],
  );

  const { regenerateMessage } = useRegenerateMessage({
    removeMessagesAfterCurrentMessage,
    sendMessage,
    messages: derivedMessages,
  });

  // useEffect(() => {
  //   //  #1289
  //   if (answer.answer && conversationId && isNew !== 'true') {
  //     addNewestAnswer(answer);
  //   }
  // }, [answer, addNewestAnswer, conversationId, isNew]);

  const handlePressEnter = useCallback(
    (documentIds: string[], dialog_id: string) => {
      if (trim(value) === '') return;
      const id = uuid();
      addNewestQuestion({
        content: value,
        doc_ids: documentIds,
        id,
        role: MessageType.User,
      });
      if (done) {
        setValue('');

        // handleSendMessage({
        //   id,
        //   content: value.trim(),
        //   role: MessageType.User,
        //   doc_ids: documentIds,
        // });
      }
    },
    [addNewestQuestion, handleSendMessage, done, setValue, value],
  );

  return {
    handlePressEnter,
    handleInputChange,
    value,
    setValue,
    regenerateMessage,
    sendLoading: !done,
    loading,
    ref,
    derivedMessages,
    removeMessageById,
  };
};

export const useGetFileIcon = () => {
  const getFileIcon = (filename: string) => {
    const ext: string = getFileExtension(filename);
    const iconPath = fileIconMap[ext as keyof typeof fileIconMap];
    return `@/assets/svg/file-icon/${iconPath}`;
  };

  return getFileIcon;
};

export const useDeleteConversation = () => {
  const showDeleteConfirm = useShowDeleteConfirm();
  const { removeConversation } = useRemoveNextConversation();

  const deleteConversation = (conversationIds: Array<string>) => async () => {
    const ret = await removeConversation(conversationIds);

    return ret;
  };

  const onRemoveConversation = (conversationIds: Array<string>) => {
    showDeleteConfirm({ onOk: deleteConversation(conversationIds) });
  };

  return { onRemoveConversation };
};

export const useRenameConversation = () => {
  const [conversation, setConversation] = useState<IClientConversation>(
    {} as IClientConversation,
  );
  const { fetchConversation } = useFetchManualConversation();
  const {
    visible: conversationRenameVisible,
    hideModal: hideConversationRenameModal,
    showModal: showConversationRenameModal,
  } = useSetModalState();
  const { updateConversation, loading } = useUpdateNextConversation();

  const onConversationRenameOk = useCallback(
    async (name: string) => {
      const ret = await updateConversation({
        ...conversation,
        conversation_id: conversation.id,
        name,
        is_new: false,
      });

      if (ret.code === 0) {
        hideConversationRenameModal();
      }
    },
    [updateConversation, conversation, hideConversationRenameModal],
  );

  const handleShowConversationRenameModal = useCallback(
    async (conversationId: string) => {
      const ret = await fetchConversation(conversationId);
      if (ret.code === 0) {
        setConversation(ret.data);
      }
      showConversationRenameModal();
    },
    [showConversationRenameModal, fetchConversation],
  );

  return {
    conversationRenameLoading: loading,
    initialConversationName: conversation.name,
    onConversationRenameOk,
    conversationRenameVisible,
    hideConversationRenameModal,
    showConversationRenameModal: handleShowConversationRenameModal,
  };
};

export const useGetSendButtonDisabled = () => {
  const { dialogId, conversationId } = useGetChatSearchParams();

  return dialogId === '' || conversationId === '';
};

export const useSendButtonDisabled = (value: string) => {
  return trim(value) === '' || value.length > 500;
};

export const useCreateConversationBeforeUploadDocument = () => {
  const { setConversation } = useSetConversation();
  const { dialogId } = useGetChatSearchParams();
  const { getConversationIsNew } = useSetChatRouteParams();

  const createConversationBeforeUploadDocument = useCallback(
    async (message: string) => {
      const isNew = getConversationIsNew();
      if (isNew === 'true') {
        const data = await setConversation(message, true);

        return data;
      }
    },
    [setConversation, getConversationIsNew],
  );

  return {
    createConversationBeforeUploadDocument,
    dialogId,
  };
};
//#endregion
