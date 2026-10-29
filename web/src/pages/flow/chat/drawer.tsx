import { useFetchFlow } from '@/hooks/flow-hooks';
import { IModalProps } from '@/interfaces/common';
import { Drawer } from 'antd';
import { getDrawerWidth } from '../utils';
import FlowChatBox from './box';

const ChatDrawer = ({ visible, hideModal }: IModalProps<any>) => {
  const { data } = useFetchFlow();
  const W = getDrawerWidth();
  const getContainer = () => document.body; //()=>document.querySelector('.chat-drawer')
  return (
    <Drawer
      title={data.title}
      placement="right"
      onClose={hideModal}
      open={visible}
      getContainer={getContainer}
      width={W}
      mask={false}
      // zIndex={10000}
    >
      <FlowChatBox></FlowChatBox>
    </Drawer>
  );
};

export default ChatDrawer;
