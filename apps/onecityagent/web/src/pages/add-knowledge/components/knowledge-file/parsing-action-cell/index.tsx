import { ReactComponent as DeleteOutlined } from '@/assets/picture/actiondelete.svg';
import { ReactComponent as DownloadOutlined } from '@/assets/picture/actiondownload.svg';
import { ReactComponent as EditOutlined } from '@/assets/picture/actionedit.svg';
import { ReactComponent as ToolOutlined } from '@/assets/picture/actionsetting.svg';
import { useShowDeleteConfirm, useTranslate } from '@/hooks/common-hooks';
import { useRemoveNextDocument } from '@/hooks/document-hooks';
import { IDocumentInfo } from '@/interfaces/database/document';
import { api_host } from '@/utils/api';
import { downloadFile } from '@/utils/file-util';
import { Button, Space, Tooltip } from 'antd';
import { isParserRunning } from '../utils';

import styles from './index.less';

interface IProps {
  record: IDocumentInfo;
  setCurrentRecord: (record: IDocumentInfo) => void;
  showRenameModal: () => void;
  showChangeParserModal: () => void;
}

const ParsingActionCell = ({
  record,
  setCurrentRecord,
  showRenameModal,
  showChangeParserModal,
}: IProps) => {
  const documentId = record.id;
  const isRunning = isParserRunning(record.run);
  const { t } = useTranslate('knowledgeDetails');
  const { removeDocument } = useRemoveNextDocument();
  const showDeleteConfirm = useShowDeleteConfirm();

  const onRmDocument = () => {
    if (!isRunning) {
      showDeleteConfirm({ onOk: () => removeDocument([documentId]) });
    }
  };

  const onDownloadDocument = () => {
    downloadFile({
      url: `${api_host}/document/get/${documentId}`,
      filename: record.name,
    });
  };

  const setRecord = () => {
    setCurrentRecord(record);
  };

  const onShowRenameModal = () => {
    setRecord();
    showRenameModal();
  };
  const onShowChangeParserModal = () => {
    setRecord();
    showChangeParserModal();
  };

  // const chunkItems: MenuProps['items'] = [
  //   {
  //     key: '1',
  //     label: (
  //       <div>
  //         <Button type="link" onClick={onShowChangeParserModal}>
  //           {t('chunkMethod')}
  //         </Button>
  //       </div>
  //     ),
  //   },
  // ];

  return (
    <Space size={0}>
      {/* <Dropdown
        menu={{ items: chunkItems }}
        trigger={['click']}
        disabled={isRunning}
      > */}
      <Tooltip title={t('chunkMethod', { keyPrefix: 'knowledgeDetails' })}>
        <Button
          type="text"
          disabled={isRunning}
          className={styles.iconButton}
          onClick={onShowChangeParserModal}
        >
          <ToolOutlined />
        </Button>
      </Tooltip>
      {/* </Dropdown> */}
      <Tooltip title={t('rename', { keyPrefix: 'common' })}>
        <Button
          type="text"
          disabled={isRunning}
          onClick={onShowRenameModal}
          className={styles.iconButton}
        >
          <EditOutlined />
        </Button>
      </Tooltip>
      <Tooltip title={t('delete', { keyPrefix: 'common' })}>
        <Button
          type="text"
          disabled={isRunning}
          onClick={onRmDocument}
          className={styles.iconButton}
        >
          <DeleteOutlined />
        </Button>
      </Tooltip>
      <Tooltip title={t('download', { keyPrefix: 'common' })}>
        <Button
          type="text"
          disabled={isRunning}
          onClick={onDownloadDocument}
          className={styles.iconButton}
        >
          <DownloadOutlined />
        </Button>
      </Tooltip>
    </Space>
  );
};

export default ParsingActionCell;
