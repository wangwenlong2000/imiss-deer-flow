import { ReactComponent as Chat } from '@/assets/picture/chat.svg';
import { ReactComponent as Conversition } from '@/assets/picture/conversition.svg';
import { ReactComponent as Database } from '@/assets/picture/database.svg';
import { ReactComponent as ChinaMobile } from '@/assets/picture/mobile.svg';
import { ReactComponent as ModelIcon } from '@/assets/picture/model.svg';
import { ReactComponent as PasswordIcon } from '@/assets/picture/password.svg';
import { ReactComponent as ProfileIcon } from '@/assets/picture/profile.svg';
import { ReactComponent as RegisterIcon } from '@/assets/picture/register.svg';
import { useFetchNextDialogList } from '@/hooks/chat-hooks';
import { useTranslate } from '@/hooks/common-hooks';
import { useNavigateWithFromState } from '@/hooks/route-hook';
import { useFetchUserInfo } from '@/hooks/user-setting-hooks';
import chatService, { paramsMethod } from '@/services/chat-service';
import { useSseStore } from '@/utils/sseStore';
import { useQueryClient } from '@tanstack/react-query';
import { Layout, Modal } from 'antd';
import { useCallback, useMemo, useState } from 'react';
import styles from './index.less';

const { Sider } = Layout;

const RagLeft = () => {
  const [currentPath, setCurrentPath] = useState<string>(
    window.location.pathname,
  );
  const navigate = useNavigateWithFromState();
  const { t } = useTranslate('header');
  const queryClient = useQueryClient();

  const { data } = useFetchUserInfo();
  const { data: dialogList, refetch } = useFetchNextDialogList();
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
                  closeSSE(id);
                  // 手动刷新dialogList数据
                  await refetch();
                  queryClient.invalidateQueries({
                    queryKey: ['fetchDialogList'],
                  });
                  resolve(true);
                },
                onCancel: () => {
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
          continue;
        }
      }
    }
  }, [dialogList, closeSSE]);
  const tagsData = useMemo(() => {
    const routes = [
      { path: '/chat', name: '事件处置', icon: Chat },
      { path: '/city-brain', name: t('cityBrain'), icon: ModelIcon },
      { path: '/schemes', name: t('schemeLibrary'), icon: Database },
      { path: '/user-setting/mcp', name: t('mcp'), icon: Conversition },
      // { path: '/user-setting/model', name: t('model'), icon: ModelIcon },
      { path: '/user-setting/profile', name: t('profile'), icon: ProfileIcon },
      {
        path: '/user-setting/password',
        name: '密码',
        icon: PasswordIcon,
      },
    ];
    if (data?.role !== '33fa821cd48611efa18a52036cfc6ce5') {
      routes.push({
        path: '/user-setting/register',
        name: '注册用户',
        icon: RegisterIcon,
      });
    }
    return routes;
  }, [t]);

  const handleChange = useCallback(
    async (path: string) => {
      console.log(path);
      const canCreate = await allowCreateChat();
      // 只有当allowCreateChat返回true时才执行跳转
      if (canCreate !== false) {
        setCurrentPath(path);
        navigate(path);
      }
    },
    [navigate, allowCreateChat],
  );

  return (
    <Sider
      width={280}
      style={{
        backgroundColor: 'white',
        borderRight: '1px solid #e8e8e8',
        borderRadius: '0.1rem',
        flex: '0 0 280px', // 防止 flex 收缩/增长
        minWidth: 280,
        maxWidth: 280,

        // boxShadow: '0// width:'300px'
      }}
    >
      <div className={styles.sidebarContainer}>
        <div className={styles.leftBarlogo}>
          <ChinaMobile></ChinaMobile>
          <div style={{ marginLeft: '0.08rem', fontSize: '17px' }}>
            城市治理多智能体协同引擎
          </div>
        </div>
        <div style={{ marginTop: '0.16rem' }}>
          {tagsData.map((item) => (
            <div
              key={item.name}
              className={`${styles.sidebarButton} ${
                currentPath === item.path ? styles.active : ''
              }`}
              onClick={() => handleChange(item.path)}
              title={item.name}
            >
              <div className="iconWrapper" style={{ pointerEvents: 'none' }}>
                <item.icon
                  style={{ width: '20px', height: '20px' }}
                  fill={
                    currentPath === item.path ? '#2083EA' : 'rgba(0,0,0,0.87)'
                  }
                />
              </div>
              <div className="label">{item.name}</div>
            </div>
          ))}
        </div>
      </div>
    </Sider>
  );
};

export default RagLeft;
