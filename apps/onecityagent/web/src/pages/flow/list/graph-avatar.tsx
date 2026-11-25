import { ReactComponent as NothingIcon } from '@/assets/svg/nothing.svg';
import { Avatar } from 'antd';

const GraphAvatar = ({ avatar }: { avatar?: string | null }) => {
  return (
    <div>
      {avatar ? (
        <Avatar
          style={{ borderRadius: '4px' }}
          size={48}
          icon={<NothingIcon />}
          src={avatar}
        />
      ) : (
        <NothingIcon />
      )}
    </div>
  );
};

export default GraphAvatar;
