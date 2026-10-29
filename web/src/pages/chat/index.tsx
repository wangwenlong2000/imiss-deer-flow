import defaultPng from '@/assets/picture/default.png';
import { ReactComponent as ChatAppCube } from '@/assets/picture/shenglue.svg';
import RenameModal from '@/components/rename-modal';
import {
  useClickDialogCard,
  useFetchNextDialogList,
  useGetChatSearchParams,
} from '@/hooks/chat-hooks';
import { useSetModalState, useTranslate } from '@/hooks/common-hooks';
import { useSetSelectedRecord } from '@/hooks/logic-hooks';
import { IDialog } from '@/interfaces/database/chat';
import chatService, { paramsMethod } from '@/services/chat-service';
import { useSseStore } from '@/utils/sseStore';
import { DeleteOutlined, EditOutlined } from '@ant-design/icons';
import { useQueryClient } from '@tanstack/react-query';
import {
  Avatar,
  Button,
  Card,
  Divider,
  Dropdown,
  Flex,
  MenuProps,
  message,
  Modal,
  Space,
  Typography,
} from 'antd';
import { MenuItemProps } from 'antd/lib/menu/MenuItem';
import classNames from 'classnames';
import _ from 'lodash';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { history, useLocation } from 'umi';
import './DrawerStyle.css';
import ChatConfigurationModal from './chat-configuration-modal';
import CreateChat from './chat-configuration-modal/create-chat';
import ChatContainer from './chat-container';
import ChatIdModal from './chat-id-modal';
import {
  CHAT_ENTRY_MODE_STORAGE_KEY,
  getDialogMode,
  setDialogMode,
  useDeleteConversation,
  useDeleteDialog,
  useEditDialog,
  useHandleItemHover,
  useRenameConversation,
  useSelectDerivedConversationList,
} from './hooks';
import type { ChatEntryMode } from './hooks';
import styles from './index.less';
import './style.css';

const { Text } = Typography;

const getStoredCurrentChatEntryMode = (): ChatEntryMode | undefined => {
  if (typeof window === 'undefined') {
    return undefined;
  }

  const storedMode = window.localStorage.getItem(CHAT_ENTRY_MODE_STORAGE_KEY);
  return storedMode === 'cityBrain' || storedMode === 'event'
    ? storedMode
    : undefined;
};

const getChatEntryModeByPathname = (pathname: string): ChatEntryMode => {
  return pathname.startsWith('/city-brain') ? 'cityBrain' : 'event';
};

const getRuntimeChatEntryMode = (pathname?: string): ChatEntryMode => {
  const runtimePathname =
    pathname || (typeof window !== 'undefined' ? window.location.pathname : '');

  if (runtimePathname.startsWith('/city-brain')) {
    return 'cityBrain';
  }

  if (runtimePathname.startsWith('/chat')) {
    return 'event';
  }

  return getStoredCurrentChatEntryMode() || 'event';
};

const getChatPathByEntryMode = (mode: ChatEntryMode) => {
  return mode === 'cityBrain' ? '/city-brain' : '/chat';
};

const normalizeCreatedDialogId = (dialogId?: string) => {
  if (!dialogId) {
    return '';
  }

  // 原始代码会去掉后端新建会话 id 的最后 4 位，这里保持原逻辑，避免影响已有路由。
  return dialogId.length > 4 ? dialogId.slice(0, dialogId.length - 4) : dialogId;
};

const Chat = () => {
  const { data: dialogList, loading: dialogLoading } = useFetchNextDialogList();
  const [loading, setLoading] = useState<boolean>(false);
  const { onRemoveDialog } = useDeleteDialog();
  const { onRemoveConversation } = useDeleteConversation();
  const { handleClickDialog } = useClickDialogCard();
  const { dialogId, conversationId } = useGetChatSearchParams();
  const queryClient = useQueryClient();
  const routeLocation = useLocation();
  const currentChatEntryMode = useMemo(
    () => getRuntimeChatEntryMode(routeLocation.pathname),
    [routeLocation.pathname],
  );
  const {
    list: conversationList,
    addTemporaryConversation,
    loading: conversationLoading,
    removeTemporaryConversation,
    renameTemporaryConversation,
    conversationRenameVisible: conversationRenameVisibleFirst,
    hideConversationRenameModal: hideConversationRenameModalFirst,
    onConversationRenameOk: onConversationRenameOkFirst,
    initialConversationName: initialConversationNameFirst,
  } = useSelectDerivedConversationList();
  const { activated, handleItemEnter, handleItemLeave } = useHandleItemHover();
  const {
    activated: conversationActivated,
    handleItemEnter: handleConversationItemEnter,
    handleItemLeave: handleConversationItemLeave,
  } = useHandleItemHover();
  const {
    conversationRenameLoading,
    initialConversationName,
    onConversationRenameOk,
    conversationRenameVisible,
    hideConversationRenameModal,
    showConversationRenameModal,
  } = useRenameConversation();
  const {
    dialogSettingLoading,
    initialDialog,
    onDialogEditOk,
    dialogEditVisible,
    clearDialog,
    hideDialogEditModal,
    showDialogEditModal,
  } = useEditDialog();
  const { t } = useTranslate('chat');
  const {
    visible: overviewVisible,
    hideModal: hideOverviewModal,
    showModal: showOverviewModal,
  } = useSetModalState();
  const { currentRecord, setRecord } = useSetSelectedRecord<IDialog>();
  const [controller, setController] = useState(new AbortController());
  const [isShowCreateModal, setIsShowCreateModal] = useState(false);
  const [currentDialogInfo, setCurrentDialogInfo] = useState({
    name: '',
    description: '',
    dialog_id: '',
  });
  const [currentDialog, setCurrentDialog] = useState<any>([]);
  const [inputValue, setInputValue] = useState('');
  // const { data } = useFetchNextDialog();
  // 4. 全局SSE状态
  const { closeSSE } = useSseStore();

  const visibleDialogList = useMemo(() => {
    return (dialogList || []).filter((item) => {
      const itemDialogId = item.dialog_id || '';
      const storedMode = getDialogMode(itemDialogId);
      const itemMode = storedMode || 'event';
      return itemMode === currentChatEntryMode;
    });
  }, [dialogList, currentChatEntryMode]);

  useEffect(() => {
    if (typeof window === 'undefined') {
      return;
    }

    window.localStorage.setItem(
      CHAT_ENTRY_MODE_STORAGE_KEY,
      currentChatEntryMode,
    );

    if (dialogId && !getDialogMode(dialogId)) {
      setDialogMode(dialogId, currentChatEntryMode);
    }
  }, [currentChatEntryMode, dialogId]);

  const hasAutoFinishedRef = useRef(false);
  useEffect(() => {
    hasAutoFinishedRef.current = false;
  }, [currentChatEntryMode]);

  useEffect(() => {
    const checkAndFinishConversations = async () => {
      console.log('页面初始化关闭进行中的会话', visibleDialogList);

      // 确保dialogList存在且是数组
      if (!visibleDialogList || !Array.isArray(visibleDialogList)) return;

      for (const item of visibleDialogList) {
        if (Number(item.finished) === 0 && item.dialog_id) {
          try {
            // 正确处理异步操作
            const res = await chatService.listConversation({
              dialogId: item.dialog_id,
            });

            if (res?.data?.code === 0 && res.data.data?.length > 0) {
              // 结束会话并关闭相关SSE连接
              const id = item.dialog_id as string;
              await paramsMethod.finishedConversation(id);
              closeSSE(id);
              console.log(`已自动结束会话: ${item.dialog_id}`);
            }
            // 等待数据重新加载
            await queryClient.refetchQueries({ queryKey: ['fetchDialogList'] });
            // 获取最新的dialogList数据
            await queryClient.getQueryData(['fetchDialogList']);
          } catch (error) {
            console.error(`处理会话 ${item.dialog_id} 时出错:`, error);
            continue;
          }
        }
      }
    };

    if (!visibleDialogList || visibleDialogList.length === 0) return;
    if (hasAutoFinishedRef.current) return; // 已经执行过一次则跳过
    checkAndFinishConversations();
    hasAutoFinishedRef.current = true;
  }, [visibleDialogList, queryClient, closeSSE]);

  const handleCloseCreateModal = () => {
    setIsShowCreateModal(false);
  };

  const handleAppCardEnter = (id: string) => () => {
    handleItemEnter(id);
  };

  const handleCreateChat = useCallback(async () => {
    const res = await chatService.createDialog({
      name: new Date().toLocaleString(),
    });
    if (res.data.code === 0) {
      message.success('会话创建成功');
      const rawDialogId = res.data.data.dialog_id || '';
      const nextDialogId = normalizeCreatedDialogId(rawDialogId);

      // 这里不能再临时读取 window.location.pathname。
      // 城市超脑平台和事件处置复用同一个 Chat 组件时，点击“新建会话”前后，
      // window.location 可能被旧会话或旧路由状态短暂带回 /chat。
      // 因此必须使用当前组件已经计算出来的 currentChatEntryMode，
      // 保证在城市超脑平台里新建会话一定跳回 /city-brain。
      const runtimeMode = currentChatEntryMode;
      const targetPath = getChatPathByEntryMode(runtimeMode);

      setDialogMode(nextDialogId, runtimeMode);
      if (rawDialogId && rawDialogId !== nextDialogId) {
        setDialogMode(rawDialogId, runtimeMode);
      }

      if (typeof window !== 'undefined') {
        window.localStorage.setItem(CHAT_ENTRY_MODE_STORAGE_KEY, runtimeMode);
      }

      history.push(`${targetPath}?dialogId=${nextDialogId}`);
      queryClient.invalidateQueries({
        queryKey: ['fetchDialogList'], // queryKey 放在配置对象中
      });
    } else {
      message.error('会话创建失败，请稍后重试');
    }
  }, [currentChatEntryMode, queryClient]);

  const handleShowChatConfigurationModal = useCallback(
    async (dialogId?: string) => {
      if (dialogId) {
        const dialogInfo = visibleDialogList.find(
          (item) => dialogId === item.dialog_id,
        );
        setCurrentDialogInfo({
          name: dialogInfo?.name || '',
          description: dialogInfo?.description || '',
          dialog_id: dialogId,
        });
      }

      // 判断是否允许创建新会话的逻辑
      const allowCreateChat = async () => {
        if (!visibleDialogList || visibleDialogList.length === 0) {
          return true;
        }
        for (const item of visibleDialogList) {
          if (Number(item.finished) === 0 && item.dialog_id) {
            try {
              const res = await chatService.listConversation({
                dialogId: item.dialog_id,
              });

              if (res.data.code === 0 && res.data.data.length > 0) {
                // 二次确认弹窗
                const id = item.dialog_id as string;
                const confirmed = await new Promise<boolean>((resolve) => {
                  Modal.confirm({
                    title: '确认结束会话？',
                    content: '有正在进行的会话，是否确定要结束？',
                    okText: '确定',
                    cancelText: '取消',
                    keyboard: false,
                    okButtonProps: {
                      autoFocus: false,
                      onKeyDown: (e) => {
                        if (e.key === 'Enter') {
                          e.preventDefault();
                        }
                      },
                    },
                    cancelButtonProps: {
                      onKeyDown: (e) => {
                        if (e.key === 'Enter') {
                          e.preventDefault();
                        }
                      },
                    },
                    onOk: async () => {
                      await paramsMethod.finishedConversation(id);
                      let closeSSEResult = closeSSE(id);
                      console.log('closeSSEResult', closeSSEResult);
                      resolve(true);
                    },
                    onCancel: () => {
                      // 用户取消时 resolve false，不再继续创建会话
                      resolve(false);
                    },
                  });
                });
                if (!confirmed) {
                  // 用户取消了确认，直接拒绝创建会话
                  return false;
                }
              }
            } catch (error) {
              console.error('处理会话时出错:', error);
              continue;
            }
          }
        }
        // 等待数据重新加载
        await queryClient.refetchQueries({ queryKey: ['fetchDialogList'] });
        // 获取最新的dialogList数据
        const newDialogList = await queryClient.getQueryData([
          'fetchDialogList',
        ]);
        const hasUnfinishedItem = (newDialogList as IDialog[]).some((item) => {
          const itemMode = getDialogMode(item.dialog_id || '') || 'event';
          return itemMode === currentChatEntryMode && Number(item.finished) === 0;
        });
        return !hasUnfinishedItem;
      };
      //判断会话是否有数据
      const hasSessionData = async () => {
        for (const item of visibleDialogList) {
          if (Number(item.finished) === 0 && item.dialog_id) {
            try {
              const res = await chatService.listConversation({
                dialogId: item.dialog_id,
              });
              if (res.data.code === 0 && res.data.data.length > 0) {
                return false;
              } else {
                return true;
              }
            } catch (error) {
              console.error('处理会话时出错:', error);
              continue;
            }
          }
        }
      };

      if (await allowCreateChat()) {
        handleCreateChat();
      } else if (await hasSessionData()) {
        message.warning('有未完成的会话，请先完成当前会话');
      }
    },
    [
      visibleDialogList,
      currentChatEntryMode,
      queryClient,
      closeSSE,
      handleCreateChat,
    ],
  );

  const debouncedHandleShowChatConfigurationModal = useMemo(() => {
    return _.debounce(handleShowChatConfigurationModal, 1000, {
      leading: false,
      trailing: true,
    });
  }, [handleShowChatConfigurationModal]);

  const handleRemoveDialog =
    (
      dialogId: string,
      options?: { title?: string },
    ): MenuItemProps['onClick'] =>
    ({ domEvent }) => {
      domEvent.preventDefault();
      domEvent.stopPropagation();
      onRemoveDialog([dialogId], options);
      setInputValue('');
    };

  // const handleShowOverviewModal =
  //   (dialog: IDialog): any =>
  //   (info: any) => {
  //     info?.domEvent?.preventDefault();
  //     info?.domEvent?.stopPropagation();
  //     setRecord(dialog);
  //     showOverviewModal();
  //   };

  // const handleRemoveConversation =
  //   (conversationId: string, items: any): MenuItemProps['onClick'] =>
  //   ({ domEvent }) => {
  //     domEvent.preventDefault();
  //     domEvent.stopPropagation();
  //     if (items.is_new == undefined) {
  //       onRemoveConversation([conversationId]);
  //     } else {
  //       removeTemporaryConversation(conversationId);
  //     }
  //   };

  // const handleShowConversationRenameModal =
  //   (conversationId: string, items: any): MenuItemProps['onClick'] =>
  //   ({ domEvent }) => {
  //     domEvent.preventDefault();
  //     domEvent.stopPropagation();
  //     if (items.is_new == undefined) {
  //       showConversationRenameModal(conversationId);
  //     } else {
  //       renameTemporaryConversation(conversationId, items.name);
  //     }
  //   };
  //会话重命名
  const handleRenameConversation = (dialogId?: string) => {
    if (dialogId) {
      const dialogInfo = visibleDialogList.find((item) => dialogId === item.dialog_id);
      setCurrentDialogInfo({
        name: dialogInfo?.name.replace(/[/:]/g, '-').replace(/\s/g, '_') || '',
        description: dialogInfo?.description || '',
        dialog_id: dialogId,
      });
    }
    setIsShowCreateModal(true);
  };

  const handleDialogCardClick = (dialogId: string) => async () => {
    // 点击历史记录时也必须保持当前入口，不允许城市超脑平台的历史跳回 /chat。
    const runtimeMode = currentChatEntryMode;
    const targetPath = getChatPathByEntryMode(runtimeMode);

    setDialogMode(dialogId, runtimeMode);
    if (typeof window !== 'undefined') {
      window.localStorage.setItem(CHAT_ENTRY_MODE_STORAGE_KEY, runtimeMode);
    }
    history.push(`${targetPath}?dialogId=${dialogId}`);
    // const { data } = await chatService.getChatList({ dialogId });
    // const { data } = useFetchNextDialog();

    // setCurrentDialog(data);
    setInputValue('');
  };

  const buildAppItems = (dialog: IDialog) => {
    const dialogId = dialog.dialog_id;

    // 自定义删除弹窗标题
    const customDeleteTitle = `确认删除会话 “${dialog.name.replace(/[/:]/g, '-').replace(/\s/g, '_') || '' || '未命名会话'}”吗?`;

    const appItems: MenuProps['items'] = [
      {
        key: '2',
        onClick: () => handleRenameConversation(dialogId),
        label: (
          <Space>
            <EditOutlined />
            {t('rename', { keyPrefix: 'common' })}
          </Space>
        ),
      },
      {
        key: '1',
        onClick: handleRemoveDialog(dialogId!, { title: customDeleteTitle }),
        label: (
          <Space>
            <DeleteOutlined />
            {t('delete', { keyPrefix: 'common' })}
          </Space>
        ),
      },
    ];

    return appItems;
  };

  return (
    <Flex className={styles.chatWrapper}>
      <div className={styles.boxWapper}>
        <Flex className={styles.chatAppWrapper}>
          <Flex flex={1} vertical>
            <Button
              type="primary"
              onClick={debouncedHandleShowChatConfigurationModal}
              style={{ height: '0.4rem' }}
            >
              {t('createChat')}
            </Button>
            <Divider
              type={'horizontal'}
              style={{ margin: '0.16rem' }}
            ></Divider>
            <Flex className={styles.chatAppContent} vertical gap={10}>
              {visibleDialogList?.map((x) => (
                <Card
                  key={x.dialog_id}
                  hoverable
                  className={classNames(styles.chatAppCard, {
                    [styles.chatAppCardSelected]: dialogId === x.dialog_id,
                  })}
                  onMouseEnter={handleAppCardEnter(x.dialog_id || '')}
                  onMouseLeave={handleItemLeave}
                  onClick={handleDialogCardClick(x.dialog_id || '')}
                >
                  <Flex justify="space-between" align="center">
                    <Space size={15}>
                      <Avatar
                        src={x.icon ? x.icon : defaultPng}
                        shape={'square'}
                      />
                      <section>
                        <b>
                          <Text
                            ellipsis={{
                              tooltip: x.name
                                .replace(/[/:]/g, '-')
                                .replace(/\s/g, '_'),
                            }}
                            style={{ width: 130 }}
                          >
                            {x.name.replace(/[/:]/g, '-').replace(/\s/g, '_')}
                          </Text>
                        </b>
                        <div>{x.description}</div>
                      </section>
                    </Space>
                    {activated === x.dialog_id && (
                      <section>
                        <Dropdown menu={{ items: buildAppItems(x) }}>
                          <ChatAppCube
                            className={styles.cubeIcon}
                          ></ChatAppCube>
                        </Dropdown>
                      </section>
                    )}
                  </Flex>
                </Card>
              ))}
            </Flex>
          </Flex>
        </Flex>
      </div>

      <CreateChat
        isOpen={isShowCreateModal}
        dialogInfo={currentDialogInfo}
        onClose={handleCloseCreateModal}
        onSubmit={() => {
          handleCloseCreateModal();
        }}
      />
      <ChatContainer
        controller={controller}
        currentDialog={currentDialog}
        setLoading={setLoading}
        loading={loading}
        inputValue={inputValue}
        setInputValue={setInputValue}
      ></ChatContainer>
      {dialogEditVisible && (
        <ChatConfigurationModal
          visible={dialogEditVisible}
          initialDialog={initialDialog}
          showModal={showDialogEditModal}
          hideModal={hideDialogEditModal}
          loading={dialogSettingLoading}
          onOk={onDialogEditOk}
          clearDialog={clearDialog}
        ></ChatConfigurationModal>
      )}
      <RenameModal
        visible={conversationRenameVisible}
        hideModal={hideConversationRenameModal}
        onOk={onConversationRenameOk}
        initialName={initialConversationName}
        loading={conversationRenameLoading}
      ></RenameModal>
      {
        <RenameModal
          visible={conversationRenameVisibleFirst}
          hideModal={hideConversationRenameModalFirst}
          onOk={onConversationRenameOkFirst}
          initialName={initialConversationNameFirst}
          loading={false}
        ></RenameModal>
      }
      {overviewVisible && (
        <ChatIdModal
          visible={overviewVisible}
          hideModal={hideOverviewModal}
          id={currentRecord.id}
          name={currentRecord.name}
          idKey="dialogId"
        ></ChatIdModal>
      )}
    </Flex>
  );
};

export default Chat;
