import { useTranslate } from '@/hooks/common-hooks';
// import { useLogout } from '@/hooks/login-hooks';
import { useSecondPathName } from '@/hooks/route-hook';
import {
  useFetchSystemVersion,
  useFetchUserInfo,
} from '@/hooks/user-setting-hooks';
import type { MenuProps } from 'antd';
import { Flex, Menu } from 'antd';
import React, { useEffect, useMemo } from 'react';
import { useNavigate } from 'umi';
import {
  UserSettingBaseKey,
  UserSettingIconMap,
  UserSettingRouteKey,
} from '../constants';
import styles from './index.less';

type MenuItem = Required<MenuProps>['items'][number];

const SideBar = () => {
  const { data: userInfo } = useFetchUserInfo();
  const navigate = useNavigate();
  const pathName = useSecondPathName();
  // const { logout } = useLogout();
  const { t } = useTranslate('setting');
  const { version, fetchSystemVersion } = useFetchSystemVersion();

  useEffect(() => {
    // 已屏蔽对系统版本的请求
    // 原代码已被注释
    /*
    if (location.host !== Domain) {
      fetchSystemVersion();
    }
    */
  }, []);

  function getItem(
    label: string,
    key: React.Key,
    icon?: React.ReactNode,
    children?: MenuItem[],
    type?: 'group',
  ): MenuItem {
    return {
      key,
      icon,
      children,
      label: (
        <Flex justify={'space-between'}>
          {t(label)}
          {/* <span className={styles.version}>
            {label === 'system' && version}
          </span> */}
        </Flex>
      ),
      type,
    } as MenuItem;
  }

  const items: MenuItem[] = Object.values(UserSettingRouteKey)
    .map((value) => {
      // 确保 userInfo 存在，并且 userInfo.role（或其他字段）可用
      if (userInfo?.nickname !== 'admin' && value === 'register') {
        return null; // 跳过该项
      }
      return getItem(value, value, UserSettingIconMap[value]);
    })
    .filter(Boolean);

  const handleMenuClick: MenuProps['onClick'] = ({ key }) => {
    // if (key === UserSettingRouteKey.Logout) {
    //   // logout();
    // } else {
    navigate(`/${UserSettingBaseKey}/${key}`);
    // }
  };

  const selectedKeys = useMemo(() => {
    return [pathName];
  }, [pathName]);

  return (
    <section className={styles.sideBarWrapper}>
      <Menu
        selectedKeys={selectedKeys}
        mode="inline"
        items={items}
        onClick={handleMenuClick}
        style={{
          width: '3.2rem',
          height: '100%',
          padding: '0.16rem',
          borderRadius: '0.16rem',
        }}
      />
    </section>
  );
};

export default SideBar;
