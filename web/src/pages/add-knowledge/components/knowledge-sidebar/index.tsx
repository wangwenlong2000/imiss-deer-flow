import { ReactComponent as ConfigurationIcon } from '@/assets/picture/peizhi.svg';
import { ReactComponent as DatasetIcon } from '@/assets/picture/shujuji.svg';
import { useFetchKnowledgeBaseConfiguration } from '@/hooks/knowledge-hooks';
import {
  useGetKnowledgeSearchParams,
  useSecondPathName,
} from '@/hooks/route-hook';
import { getWidth } from '@/utils';
import { Menu, MenuProps } from 'antd';
import { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'umi';
import { KnowledgeRouteKey } from '../../constant';

import styles from './index.less';

const KnowledgeSidebar = () => {
  let navigate = useNavigate();
  const activeKey = useSecondPathName();
  const { knowledgeId } = useGetKnowledgeSearchParams();

  const [windowWidth, setWindowWidth] = useState(getWidth());
  const [collapsed, setCollapsed] = useState(false);
  const { t } = useTranslation();
  const { data: knowledgeDetails } = useFetchKnowledgeBaseConfiguration();

  const handleSelect: MenuProps['onSelect'] = (e) => {
    navigate(`/knowledge/${e.key}?id=${knowledgeId}`);
  };

  type MenuItem = Required<MenuProps>['items'][number];

  const getItem = useCallback(
    (
      label: string,
      key: React.Key,
      icon?: React.ReactNode,
      disabled?: boolean,
      children?: MenuItem[],
      type?: 'group',
    ): MenuItem => {
      return {
        key,
        icon,
        children,
        label: t(`knowledgeDetails.${label}`),
        type,
        disabled,
      } as MenuItem;
    },
    [t],
  );

  const items: MenuItem[] = useMemo(() => {
    return [
      getItem(
        KnowledgeRouteKey.Dataset, // TODO: Change icon color when selected
        KnowledgeRouteKey.Dataset,
        <DatasetIcon
          fill={activeKey === 'dataset' ? '#2083EA' : 'raba(0,0,0,0.87)'}
        />,
      ),
      // getItem(
      //   KnowledgeRouteKey.Testing,
      //   KnowledgeRouteKey.Testing,
      //   <TestingIcon />,
      // ),
      getItem(
        KnowledgeRouteKey.Configuration,
        KnowledgeRouteKey.Configuration,
        <ConfigurationIcon
          fill={activeKey === 'dataset' ? 'raba(0,0,0,0.87)' : '#2083EA'}
        />,
      ),
    ];
  }, [getItem, activeKey]);

  // useEffect(() => {
  //   if (windowWidth.width > 957) {
  //     setCollapsed(false);
  //   } else {
  //     setCollapsed(true);
  //   }
  // }, [windowWidth.width]);

  // 标记一下
  // useEffect(() => {
  //   const widthSize = () => {
  //     const width = getWidth();

  //     setWindowWidth(width);
  //   };
  //   window.addEventListener('resize', widthSize);
  //   return () => {
  //     window.removeEventListener('resize', widthSize);
  //   };
  // }, []);

  return (
    <div className={styles.sidebarWrapper}>
      {/* <div className={styles.sidebarTop}>
        <Space size={8} direction="vertical">
          <Avatar size={64} src={knowledgeDetails.avatar} />
          <div className={styles.knowledgeTitle}>{knowledgeDetails.name}</div>
        </Space>
        <p className={styles.knowledgeDescription}>
          {knowledgeDetails.description}
        </p>
      </div>
      <div className={styles.divider}></div> */}
      <div className={styles.menuWrapper}>
        <Menu
          selectedKeys={[activeKey]}
          // mode="inline"
          // className={classNames(styles.menu, {
          //   [styles.defaultWidth]: windowWidth.width > 957,
          //   [styles.minWidth]: windowWidth.width <= 957,
          // })}
          // inlineCollapsed={collapsed}
          mode="horizontal"
          items={items}
          onSelect={handleSelect}
          style={{
            background: 'transparent',
            fontSize: '0.14rem',
            height: '0.48rem',
            lineHeight: '0.48rem',
          }}
        />
      </div>
    </div>
  );
};

export default KnowledgeSidebar;
