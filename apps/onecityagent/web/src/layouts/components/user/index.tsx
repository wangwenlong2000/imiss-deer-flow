import defaultPng from '@/assets/picture/default.png';
import { useLogout } from '@/hooks/login-hooks';
import { useFetchUserInfo } from '@/hooks/user-setting-hooks';
import chatService, { paramsMethod } from '@/services/chat-service';
import { useSseStore } from '@/utils/sseStore';
import type { MenuProps } from 'antd';
import { Avatar, Dropdown, Modal, Space } from 'antd';
import React, { useCallback, useState } from 'react';
// import { history } from 'umi';
import { useFetchNextDialogList } from '@/hooks/chat-hooks';

import styles from '../../index.less';

const App: React.FC = () => {
  const { data: userInfo } = useFetchUserInfo();
  const { logout, loading: isLoggingOut } = useLogout();
  const { data: dialogList } = useFetchNextDialogList();
  const [visibles, setVisibles] = useState(false);
  const { closeSSE } = useSseStore();
  // 判断是否允许创建新会话的逻辑
  const allowCreateChat = useCallback(async () => {
    if (!dialogList || dialogList.length === 0) {
      return true;
    }
    for (const item of dialogList) {
      if (Number(item.finished) === 0 && item.dialog_id) {
        try {
          const res = await chatService.listConversation({
            dialogId: item.dialog_id,
          });
          if (res.data.code === 0 && res.data.data.length > 0) {
            const id = item.dialog_id as string;
            await paramsMethod.finishedConversation(id);
            closeSSE(id);
          }
        } catch (error) {
          continue;
        }
      }
    }
  }, [dialogList, closeSSE]);

  // 简化的处理退出登录函数，使用React Query的loading状态防止重复提交
  const handleLogout = useCallback(async () => {
    // 先等待 allowCreateChat 完成，再执行 logout
    await allowCreateChat();
    logout();
  }, [allowCreateChat, logout]);
  const items: MenuProps['items'] = [
    {
      key: '1',
      label: (
        <a
          onClick={() => {
            setVisibles(true);
          }}
        >
          退出
        </a>
      ),
    },
  ];

  return (
    <>
      <div style={{ display: 'flex', alignItems: 'center', cursor: 'pointer' }}>
        <Dropdown menu={{ items }}>
          <Space>
            <Avatar
              size={32}
              className={styles.clickAvailable}
              src={userInfo?.avatar || defaultPng}
            />
            <div style={{ marginLeft: '0.08rem', color: '#333' }}>
              {userInfo.nickname}
            </div>
          </Space>
        </Dropdown>
      </div>
      <Modal
        title={'确定退出账号么?'}
        width={500}
        open={visibles}
        onOk={handleLogout}
        okButtonProps={{
          loading: isLoggingOut, // 当退出请求正在进行时禁用确认按钮
        }}
        onCancel={() => {
          setVisibles(false);
        }}
      ></Modal>
    </>
  );
};

export default App;
