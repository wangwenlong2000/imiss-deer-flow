import { ReactComponent as Frame } from '@/assets/picture/Frame.svg';
import defaultPng from '@/assets/picture/default.png';
import { MessageType } from '@/constants/chat';
import { useSetModalState } from '@/hooks/common-hooks';
import { IReference } from '@/interfaces/database/chat';
import { IChunk } from '@/interfaces/database/knowledge';
import classNames from 'classnames';
import { memo, useCallback, useEffect, useMemo, useState } from 'react';

import {
  useFetchDocumentInfosByIds,
  useFetchDocumentThumbnailsByIds,
} from '@/hooks/document-hooks';
import { IRegenerateMessage, IRemoveMessageById } from '@/hooks/logic-hooks';
import { IMessage } from '@/pages/chat/interface';
import MarkdownContent from '@/pages/chat/markdown-content';
import { getExtension, isImage } from '@/utils/document-util';
import { Avatar, Button, Flex, List, Space, Typography } from 'antd';
import FileIcon from '../file-icon';
import IndentedTreeModal from '../indented-tree/modal';
import NewDocumentLink from '../new-document-link';
import { AssistantGroupButton, UserGroupButton } from './group-button';
import styles from './index.less';
import './style.css';

const { Text } = Typography;

interface IProps extends Partial<IRemoveMessageById>, IRegenerateMessage {
  item: IMessage;
  reference: IReference;
  loading?: boolean;
  sendLoading?: boolean;
  nickname?: string;
  avatar?: string;
  clickDocumentButton?: (documentId: string, chunk: IChunk) => void;
  index: number;
  showLikeButton?: boolean;
  schemaData: {
    scene: string;
    plan: string;
  };
}

const MessageItem = ({
  item,
  reference,
  loading = false,
  avatar = '',
  sendLoading = false,
  clickDocumentButton,
  index,
  removeMessageById,
  regenerateMessage,
  showLikeButton = true,
  schemaData,
}: IProps) => {
  const isAssistant = item.role === MessageType.Assistant;
  const isUser = item.role === MessageType.User;
  const { data: documentList, setDocumentIds } = useFetchDocumentInfosByIds();
  const { data: documentThumbnails, setDocumentIds: setIds } =
    useFetchDocumentThumbnailsByIds();
  const { visible, hideModal, showModal } = useSetModalState();
  const [clickedDocumentId, setClickedDocumentId] = useState('');

  const referenceDocumentList = useMemo(() => {
    return reference?.doc_aggs ?? [];
  }, [reference?.doc_aggs]);

  const handleUserDocumentClick = useCallback(
    (id: string) => () => {
      setClickedDocumentId(id);
      showModal();
    },
    [showModal],
  );

  const handleRegenerateMessage = useCallback(() => {
    regenerateMessage?.(item);
  }, [regenerateMessage, item]);

  useEffect(() => {
    const ids = item?.doc_ids ?? [];
    if (ids.length) {
      setDocumentIds(ids);
      const documentIds = ids.filter((x) => !(x in documentThumbnails));
      if (documentIds.length) {
        setIds(documentIds);
      }
    }
  }, [item.doc_ids, setDocumentIds, setIds, documentThumbnails]);

  return (
    <div
      className={classNames(styles.messageItem, {
        [styles.messageItemLeft]: item.role === MessageType.Assistant,
        [styles.messageItemRight]: item.role === MessageType.User,
      })}
    >
      <section
        className={classNames(styles.messageItemSection, {
          [styles.messageItemSectionLeft]: item.role === MessageType.Assistant,
          [styles.messageItemSectionRight]: item.role === MessageType.User,
        })}
      >
        <div
          className={classNames(styles.messageItemContent, {
            [styles.messageItemContentReverse]: item.role === MessageType.User,
          })}
        >
          {item.role === MessageType.User ? (
            <Avatar size={40} src={avatar ?? defaultPng} />
          ) : (
            <Frame></Frame>
          )}
          {
            item.role ? (
              <Flex vertical gap={8} flex={1}>
                <Space>
                  {isAssistant ? (
                    <b>{item.content?.slice(0, item.content?.indexOf('-'))}</b>
                  ) : (
                    <></>
                  )}
                  {isAssistant ? (
                    index !== 0 && (
                      <AssistantGroupButton
                        messageId={item.id}
                        content={
                          item.content?.slice(item.content?.indexOf('-') + 1) ||
                          item.content
                        }
                        prompt={item.prompt}
                        showLikeButton={showLikeButton}
                        audioBinary={item.audio_binary}
                        schemaData={schemaData}
                      ></AssistantGroupButton>
                    )
                  ) : (
                    <UserGroupButton
                      content={item.content}
                      messageId={item.id}
                      removeMessageById={removeMessageById}
                      regenerateMessage={
                        regenerateMessage && handleRegenerateMessage
                      }
                      sendLoading={sendLoading}
                    ></UserGroupButton>
                  )}

                  {/* <b>{isAssistant ? '' : nickname}</b> */}
                </Space>

                <div
                  className={
                    isAssistant ? styles.messageText : styles.messageUserText
                  }
                >
                  <MarkdownContent
                    loading={loading}
                    content={
                      item.role === MessageType.Assistant
                        ? item.content?.slice(item.content?.indexOf('-') + 1) ||
                          item.content
                        : item.content
                    }
                    reference={reference}
                    clickDocumentButton={clickDocumentButton}
                  ></MarkdownContent>
                </div>
                {isAssistant && referenceDocumentList.length > 0 && (
                  <List
                    bordered
                    dataSource={referenceDocumentList}
                    renderItem={(item) => {
                      return (
                        <List.Item>
                          <Flex gap={'small'} align="center">
                            <FileIcon
                              id={item.doc_id}
                              name={item.doc_name}
                            ></FileIcon>

                            <NewDocumentLink
                              documentId={item.doc_id}
                              documentName={item.doc_name}
                              prefix="document"
                            >
                              {item.doc_name}
                            </NewDocumentLink>
                          </Flex>
                        </List.Item>
                      );
                    }}
                  />
                )}
                {isUser && documentList.length > 0 && (
                  <List
                    bordered
                    dataSource={documentList}
                    renderItem={(item) => {
                      // TODO:
                      const fileThumbnail =
                        documentThumbnails[item.id] ||
                        documentThumbnails[item.id];
                      const fileExtension = getExtension(item.name);
                      return (
                        <List.Item>
                          <Flex gap={'small'} align="center">
                            <FileIcon id={item.id} name={item.name}></FileIcon>

                            {isImage(fileExtension) ? (
                              <NewDocumentLink
                                documentId={item.id}
                                documentName={item.name}
                                prefix="document"
                              >
                                {item.name}
                              </NewDocumentLink>
                            ) : (
                              <Button
                                type={'text'}
                                onClick={handleUserDocumentClick(item.id)}
                              >
                                <Text
                                  style={{ maxWidth: '40vw' }}
                                  ellipsis={{ tooltip: item.name }}
                                >
                                  {item.name}
                                </Text>
                              </Button>
                            )}
                          </Flex>
                        </List.Item>
                      );
                    }}
                  />
                )}
              </Flex>
            ) : (
              <></>
            ) // 占位，保持对齐
          }
        </div>
      </section>
      {visible && (
        <IndentedTreeModal
          visible={visible}
          hideModal={hideModal}
          documentId={clickedDocumentId}
        ></IndentedTreeModal>
      )}
    </div>
  );
};

export default memo(MessageItem);
