import { KnowledgeRouteKey } from '@/constants/knowledge';
import { IKnowledge } from '@/interfaces/database/knowledge';
import { formatDate } from '@/utils/date';
// import {
//   CalendarOutlined,
//   FileTextOutlined,
//   UserOutlined,
//   DeleteOutlined
// } from '@ant-design/icons';
import { Badge, Card, Space, Tooltip } from 'antd';
// import classNames from 'classnames';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'umi';

import { ReactComponent as Book } from '@/assets/picture/book.svg';
import { ReactComponent as Delete } from '@/assets/picture/delete.svg';
import { ReactComponent as File } from '@/assets/picture/file-paste.svg';
import { ReactComponent as Time } from '@/assets/picture/time.svg';
import { useShowDeleteConfirm } from '@/hooks/common-hooks';
import { useDeleteKnowledge } from '@/hooks/knowledge-hooks';
import { useFetchUserInfo } from '@/hooks/user-setting-hooks';
import styles from './index.less';

interface IProps {
  item: IKnowledge;
}

const KnowledgeCard = ({ item }: IProps) => {
  const showDeleteConfirm = useShowDeleteConfirm();
  const navigate = useNavigate();
  const { t } = useTranslation();
  const { data: userInfo } = useFetchUserInfo();

  const { deleteKnowledge } = useDeleteKnowledge();

  const removeKnowledge = async () => {
    return deleteKnowledge(item.id);
  };

  const handleCardClick = () => {
    navigate(`/knowledge/${KnowledgeRouteKey.Dataset}?id=${item.id}`, {
      state: { from: 'list' },
    });
  };

  return (
    <Badge.Ribbon
      text={item.nickname}
      color={
        userInfo.nickname === item.nickname
          ? 'linear-gradient(90deg, rgba(50, 89, 238, 0.50) 0%, #30B5FA 100%)'
          : 'linear-gradient(90deg, rgba(193, 50, 238, 0.50) 0%, #6E30FA 100%)'
      }
      style={{ height: '0.24rem', lineHeight: '0.24rem' }}
      // className={classNames(styles.ribbon, {
      //   [styles.hideRibbon]: item.permission !== 'team',
      // })}
    >
      <Card className={styles.card} onClick={handleCardClick}>
        <div className={styles.container}>
          <div className={styles.content}>
            <Book></Book>
            {/* <OperateDropdown deleteItem={removeKnowledge}></OperateDropdown> */}
          </div>
          <div className={styles.titleWrapper}>
            <div className={styles.title}>{item.name}</div>
            {/* <p>{item.description}</p> */}
          </div>
          <div className={styles.footer}>
            <div className={styles.footerTop}>
              <div className={styles.bottomLeft}>
                <File className={styles.leftIcon} />
                <span className={styles.rightText}>
                  <Space>
                    {item.doc_num}
                    {t('knowledgeList.doc')}
                  </Space>
                </span>
              </div>
            </div>
            <div className={styles.bottom}>
              <div className={styles.bottomLeft}>
                <Time className={styles.leftIcon} />
                <span className={styles.rightText}>
                  {formatDate(item.update_time)}
                </span>
              </div>
              {userInfo.nickname === item.nickname && (
                <Tooltip placement="top" title={'删除'} arrow={true}>
                  <Delete
                    onClick={(e) => {
                      e.preventDefault();
                      e.stopPropagation();
                      showDeleteConfirm({ onOk: removeKnowledge });
                    }}
                  />
                </Tooltip>
              )}

              {/* <Avatar.Group size={25}>
                <Avatar src="https://api.dicebear.com/7.x/miniavs/svg?seed=1" />
                <a href="https://ant.design">
                  <Avatar style={{ backgroundColor: '#f56a00' }}>K</Avatar>
                </a>
                <Tooltip title="Ant User" placement="top">
                  <Avatar
                    style={{ backgroundColor: '#87d068' }}
                    icon={<UserOutlined />}
                  />
                </Tooltip>
                <Avatar
                  style={{ backgroundColor: '#1677ff' }}
                  icon={<AntDesignOutlined />}
                />
              </Avatar.Group> */}
            </div>
          </div>
        </div>
      </Card>
    </Badge.Ribbon>
  );
};

export default KnowledgeCard;
