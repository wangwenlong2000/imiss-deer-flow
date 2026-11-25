import { Layout, theme } from 'antd';
import React from 'react';
import { Outlet } from 'umi';
import '../locales/config';
import Header from './components/header';
import LeftBar from './components/leftBar';

import styles from './index.less';
// import { transform } from 'lodash';

const { Content } = Layout;

const App: React.FC = () => {
  const {
    token: { colorBgContainer, borderRadiusLG },
  } = theme.useToken();

  return (
    <Layout className={styles.layout}>
      {/* <Divider orientationMargin={0} className={styles.divider} /> */}
      <LeftBar></LeftBar>
      <Layout style={{ marginLeft: '0.24rem', background: 'transparent' }}>
        <Header></Header>
        <Content
          style={{
            // minHeight: 280,
            // background: colorBgContainer,
            // borderRadius: borderRadiusLG,
            // overflow: 'auto',
            // display: 'flex',
            borderRadius: '0.1rem',
            background: 'rgba(255, 255, 255, 0.60)',
            boxShadow: '0px 2px 3px 0px rgba(0, 0, 0, 0.10)',
            padding: '0.32rem',
          }}
        >
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  );
};

export default App;
