import { useSetModalState } from '@/hooks/common-hooks';
import { Layout } from 'antd';
import { useState } from 'react';
import { ReactFlowProvider } from 'reactflow';
import FlowCanvas from './canvas';
import Sider from './flow-sider';
import FlowHeader from './header';
import { useCopyPaste, useFetchDataOnMount } from './hooks';

const { Content } = Layout;

function RagFlow() {
  const [collapsed, setCollapsed] = useState(false);
  const {
    visible: chatDrawerVisible,
    hideModal: hideChatDrawer,
    showModal: showChatDrawer,
  } = useSetModalState();

  useFetchDataOnMount();
  useCopyPaste();

  return (
    <Layout style={{ background: 'transparent', height: '100%' }}>
      <ReactFlowProvider>
        <FlowHeader
          showChatDrawer={showChatDrawer}
          chatDrawerVisible={chatDrawerVisible}
        ></FlowHeader>
        <Layout style={{ background: 'transparent' }}>
          <Sider setCollapsed={setCollapsed} collapsed={collapsed}></Sider>
          <Content
            style={{
              marginLeft: '0.24rem',
              background: '#EEF0F3',
              borderRadius: '0.1rem',
            }}
          >
            <FlowCanvas
              drawerVisible={chatDrawerVisible}
              hideDrawer={hideChatDrawer}
            ></FlowCanvas>
          </Content>
        </Layout>
      </ReactFlowProvider>
    </Layout>
  );
}

export default RagFlow;
