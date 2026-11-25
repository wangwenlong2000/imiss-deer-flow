import { useTranslate } from '@/hooks/common-hooks';
import { MenuProps, Space } from 'antd';
import User from '../user';

import { LanguageList, LanguageMap } from '@/constants/common';
import { useChangeLanguage } from '@/hooks/logic-hooks';
import { useFetchUserInfo } from '@/hooks/user-setting-hooks';
import styled from './index.less';

// const Circle = ({ children, ...restProps }: React.PropsWithChildren) => {
//   return (
//     <div {...restProps} className={styled.circle}>
//       {children}
//     </div>
//   );
// };

// const handleGithubCLick = () => {
//   window.open('https://github.com/infiniflow/ragflow', 'target');
// };

const RightToolBar = () => {
  const { t } = useTranslate('common');
  const changeLanguage = useChangeLanguage();

  const { data: userInfo } = useFetchUserInfo();

  const handleItemClick: MenuProps['onClick'] = ({ key }) => {
    changeLanguage(key);
  };

  const items: MenuProps['items'] = LanguageList.map((x) => ({
    key: x,
    label: <span>{LanguageMap[x as keyof typeof LanguageMap]}</span>,
  })).reduce<MenuProps['items']>((pre, cur) => {
    return [...pre!, { type: 'divider' }, cur];
  }, []);

  return (
    <div className={styled.toolbarWrapper}>
      <Space wrap size={16}>
        {/* <Dropdown menu={{ items, onClick: handleItemClick }} placement="bottom">
          <Space className={styled.language}>
            <b>{t(camelCase(userInfo.language))}</b>
            <DownOutlined />
          </Space>
        </Dropdown> */}
        {/* <Circle>
          <GithubOutlined onClick={handleGithubCLick} />
          tys github链接跳转
        </Circle> */}
        {/* <Circle>
          <MonIcon />
        </Circle> */}
        <User></User>
      </Space>
    </div>
  );
};

export default RightToolBar;
