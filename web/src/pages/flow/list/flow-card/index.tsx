import { formatDate } from '@/utils/date';
// import { CalendarOutlined } from '@ant-design/icons';
import { Card, Modal, Typography } from 'antd';
import { useNavigate } from 'umi';

// import OperateDropdown from '@/components/operate-dropdown';
import { ReactComponent as Delete } from '@/assets/picture/delete.svg';
import { ReactComponent as Time } from '@/assets/picture/time.svg';
import { useDeleteFlow } from '@/hooks/flow-hooks';
import { IFlow } from '@/interfaces/database/flow';
import { useCallback, useState } from 'react';
import GraphAvatar from '../graph-avatar';
import styles from './index.less';

interface IProps {
  item: IFlow;
}

const FlowCard = ({ item }: IProps) => {
  const navigate = useNavigate();
  const { deleteFlow } = useDeleteFlow();

  const removeFlow = useCallback(() => {
    return deleteFlow([item.id]);
  }, [deleteFlow, item]);

  const handleCardClick = () => {
    navigate(`/flow/${item.id}`);
  };
  const [visible, setVisible] = useState<boolean>(false);

  return (
    <>
      <Card className={styles.card} onClick={handleCardClick}>
        <div className={styles.container}>
          <div>
            <div className={styles.content}>
              <GraphAvatar avatar={item.avatar}></GraphAvatar>
              {/* <OperateDropdown deleteItem={removeFlow}></OperateDropdown> */}
            </div>
          </div>
          <div className={styles.contentRight}>
            <div className={styles.titleWrapper}>
              <Typography.Title
                className={styles.title}
                ellipsis={{ tooltip: item.title }}
              >
                {item.title}
              </Typography.Title>
              {/* <p>{item.description}</p> */}
            </div>
            <div className={styles.footer}>
              <div className={styles.bottom}>
                <div className={styles.bottomLeft}>
                  {/* <CalendarOutlined className={styles.leftIcon} /> */}
                  <div style={{ display: 'flex', alignItems: 'center' }}>
                    <Time />
                    <span className={styles.rightText}>
                      {formatDate(item.update_time)}
                    </span>
                  </div>
                  <div>
                    <Delete
                      onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        setVisible(true);
                        // removeFlow();
                      }}
                    />
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </Card>
      <Modal
        title={'确定删除么?'}
        width={500}
        open={visible}
        onOk={removeFlow}
        onCancel={() => setVisible(false)}
        destroyOnHidden
      ></Modal>
    </>
  );
};

export default FlowCard;
