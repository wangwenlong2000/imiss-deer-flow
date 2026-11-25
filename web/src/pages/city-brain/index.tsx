import { ReactComponent as Frame } from '@/assets/picture/Frame.svg';
import defaultPng from '@/assets/picture/default.png';
import { ReactComponent as ChatAppCube } from '@/assets/picture/shenglue.svg';
import { useFetchUserInfo } from '@/hooks/user-setting-hooks';
import { getPlatformAuthorization } from '@/utils/authorization-util';
import { buildDeerFlowDownloadUrl, isDeerFlowArtifactUrl } from '@/utils/session';
import { SseClient } from '@/utils/sse';
import { CopyOutlined, DeleteOutlined, EditOutlined, PaperClipOutlined } from '@ant-design/icons';
import {
  Avatar,
  Button,
  Card,
  Divider,
  Drawer,
  Dropdown,
  Flex,
  Input,
  message,
  Modal,
  Space,
  Spin,
  Typography,
} from 'antd';
import type { MenuProps } from 'antd';
import classNames from 'classnames';
import type { ChangeEvent, ReactNode } from 'react';
import { useEffect, useMemo, useRef, useState } from 'react';
import { history, useLocation } from 'umi';
import styles from './index.less';

const { Text } = Typography;
const { TextArea } = Input;

const CITY_BRAIN_DIALOGS_STORAGE_KEY = 'onecityagent_city_brain_dialogs_v1';
const CITY_BRAIN_MESSAGES_STORAGE_KEY = 'onecityagent_city_brain_messages_v1';
const CITY_BRAIN_STEP_FEEDBACK_CONTENT = '当前步骤已完成，系统将继续执行下一步任务。';

interface AuthenticatedComplianceIdentity {
  user_id: string;
  org_id: string;
  role_code: string;
  identity_class: string;
  roles: string[];
  permissions: string[];
  identity_mapping_configured: boolean;
}

interface IdentityPanelState {
  status: 'loading' | 'ready' | 'error';
  identity?: AuthenticatedComplianceIdentity;
}

interface ComplianceResult {
  gate: 'InputGate' | 'ContextGate' | 'OutputGate' | string;
  scene: string;
  violation_types: string[];
  actions: string[];
  audit_ref?: string;
  basis: string[];
  final_disposition: string;
}

interface CityBrainDialog {
  dialog_id: string;
  name: string;
  created_at: number;
  updated_at: number;
  finished?: boolean;
  /** 当前移动侧会话绑定的 DeerFlow/LangGraph 真实 UUID thread_id。 */
  deerflow_thread_id?: string;
}

interface CityBrainMessage {
  id: string;
  role: 'user' | 'agent';
  content: string;
  timestamp: number;
  order_id: number;
  agentRole?: string;
  scene?: string;
  plan?: string | string[];
  metadata?: Record<string, any>;
  complianceResult?: ComplianceResult;
}

type CityBrainMessageMap = Record<string, CityBrainMessage[]>;

const safeJsonParse = <T,>(raw: string | null, fallback: T): T => {
  if (!raw) return fallback;
  try {
    return JSON.parse(raw) as T;
  } catch (error) {
    console.error('[city-brain] localStorage JSON parse failed:', error);
    return fallback;
  }
};

const isCityBrainDialogId = (dialogId?: string) => {
  return Boolean(dialogId && dialogId.startsWith('citybrain-'));
};

const sanitizeDialogName = (name?: string) => {
  return (name || '').replace(/[/:]/g, '-').replace(/\s/g, '_');
};

const cityBrainStorageKey = (key: string, userId: string) => `${key}:${userId}`;

const loadCityBrainDialogs = (userId: string): CityBrainDialog[] => {
  if (typeof window === 'undefined' || !userId) return [];
  const dialogs = safeJsonParse<CityBrainDialog[]>(
    window.localStorage.getItem(cityBrainStorageKey(CITY_BRAIN_DIALOGS_STORAGE_KEY, userId)),
    [],
  );

  const dialogMap = new Map<string, CityBrainDialog>();
  dialogs.forEach((item) => {
    if (isCityBrainDialogId(item.dialog_id)) {
      dialogMap.set(item.dialog_id, item);
    }
  });

  return Array.from(dialogMap.values()).sort(
    (a, b) => (b.updated_at || 0) - (a.updated_at || 0),
  );
};

const saveCityBrainDialogs = (userId: string, dialogs: CityBrainDialog[]) => {
  if (typeof window === 'undefined' || !userId) return;
  const safeDialogs = dialogs.filter((item) => isCityBrainDialogId(item.dialog_id));
  window.localStorage.setItem(
    cityBrainStorageKey(CITY_BRAIN_DIALOGS_STORAGE_KEY, userId),
    JSON.stringify(safeDialogs),
  );
};

const loadCityBrainMessageMap = (userId: string): CityBrainMessageMap => {
  if (typeof window === 'undefined' || !userId) return {};
  const messageMap = safeJsonParse<CityBrainMessageMap>(
    window.localStorage.getItem(cityBrainStorageKey(CITY_BRAIN_MESSAGES_STORAGE_KEY, userId)),
    {},
  );

  return Object.fromEntries(
    Object.entries(messageMap).filter(([dialogId]) => isCityBrainDialogId(dialogId)),
  );
};

const saveCityBrainMessageMap = (userId: string, messageMap: CityBrainMessageMap) => {
  if (typeof window === 'undefined' || !userId) return;
  const safeMessageMap = Object.fromEntries(
    Object.entries(messageMap).filter(([dialogId]) => isCityBrainDialogId(dialogId)),
  );
  window.localStorage.setItem(
    cityBrainStorageKey(CITY_BRAIN_MESSAGES_STORAGE_KEY, userId),
    JSON.stringify(safeMessageMap),
  );
};

const getDialogIdFromSearch = (search: string) => {
  return new URLSearchParams(search).get('dialogId') || '';
};

const createCityBrainDialogId = () => {
  return `citybrain-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
};

const isDeerFlowUuidThreadId = (value?: string) => {
  return Boolean(
    value &&
      /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
        String(value).trim(),
      ),
  );
};

const extractDeerFlowThreadIdFromText = (text?: string) => {
  const rawText = String(text || '');
  const match = rawText.match(/thread_id\s*[=:：]\s*([0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})/i);
  return match?.[1] || '';
};

const getKnownDeerFlowThreadId = (
  dialog?: CityBrainDialog,
  dialogMessages: CityBrainMessage[] = [],
  attachments: UploadedAttachment[] = [],
) => {
  if (isDeerFlowUuidThreadId(dialog?.deerflow_thread_id)) {
    return String(dialog?.deerflow_thread_id);
  }

  const attachmentThread = attachments.find((item) => isDeerFlowUuidThreadId(item.thread_id))?.thread_id;
  if (isDeerFlowUuidThreadId(attachmentThread)) {
    return String(attachmentThread);
  }

  for (let index = dialogMessages.length - 1; index >= 0; index -= 1) {
    const threadId = extractDeerFlowThreadIdFromText(dialogMessages[index]?.content);
    if (isDeerFlowUuidThreadId(threadId)) {
      return threadId;
    }
  }

  return '';
};

const formatDialogName = (timestamp: number) => {
  return new Date(timestamp).toLocaleString();
};

const normalizeContent = (content: unknown) => {
  if (Array.isArray(content)) {
    return JSON.stringify(content);
  }
  if (typeof content === 'object' && content !== null) {
    return JSON.stringify(content, null, 2);
  }
  return String(content ?? '');
};

const buildDeerFlowSseUrl = (
  query: string,
  dialogId: string,
  attachments: UploadedAttachment[] = [],
  deerflowThreadId?: string,
) => {
  const params = new URLSearchParams({
    query,
    conversation_id: dialogId,
  });

  if (isDeerFlowUuidThreadId(deerflowThreadId)) {
    params.set('thread_id', String(deerflowThreadId));
  }

  const safeAttachments = attachments
    .filter((item) => item.filename || item.virtual_path || item.artifact_url || item.url)
    .map((item) => ({
      filename: item.filename || item.original_filename || '',
      size: item.size,
      virtual_path: item.virtual_path || '',
      artifact_url: item.artifact_url || item.url || '',
      markdown_virtual_path: item.markdown_virtual_path || '',
      markdown_artifact_url: item.markdown_artifact_url || item.preview_url || '',
      mime_type: item.mime_type || '',
      thread_id: item.thread_id || deerflowThreadId || '',
    }));

  if (safeAttachments.length > 0) {
    params.set('attachments', JSON.stringify(safeAttachments));
  }

  return `/query/deerflow?${params.toString()}`;
};

const trimUrlTail = (url: string) => {
  const match = url.match(/^(.*?)([，。；、,.);）\]]*)$/);
  return {
    href: match?.[1] || url,
    tail: match?.[2] || '',
  };
};

const getFileNameFromUrl = (url: string) => {
  try {
    const parsedUrl = new URL(url);
    return decodeURIComponent(parsedUrl.pathname.split('/').pop() || url);
  } catch (error) {
    return url;
  }
};


const getFileExtension = (filenameOrUrl?: string) => {
  const raw = String(filenameOrUrl || '').split('?')[0].split('#')[0];
  const name = raw.split('/').pop() || raw;
  if (!name.includes('.')) return 'FILE';
  return name.split('.').pop()?.toUpperCase() || 'FILE';
};

const formatFileSize = (size?: number) => {
  if (!size || Number.isNaN(size)) return '';
  if (size < 1024) return `${size} B`;
  if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
  return `${(size / 1024 / 1024).toFixed(1)} MB`;
};

const parseCsvRows = (content: string, maxRows = 80) => {
  const rows: string[][] = [];
  let current = '';
  let row: string[] = [];
  let inQuotes = false;

  for (let index = 0; index < content.length; index += 1) {
    const char = content[index];
    const nextChar = content[index + 1];

    if (char === '"' && inQuotes && nextChar === '"') {
      current += '"';
      index += 1;
      continue;
    }

    if (char === '"') {
      inQuotes = !inQuotes;
      continue;
    }

    if (char === ',' && !inQuotes) {
      row.push(current);
      current = '';
      continue;
    }

    if ((char === '\n' || char === '\r') && !inQuotes) {
      if (char === '\r' && nextChar === '\n') {
        index += 1;
      }
      row.push(current);
      rows.push(row);
      row = [];
      current = '';
      if (rows.length >= maxRows) break;
      continue;
    }

    current += char;
  }

  if (rows.length < maxRows && (current || row.length)) {
    row.push(current);
    rows.push(row);
  }

  return rows.filter((item) => item.some((cell) => cell.trim()));
};

const normalizeArtifactUrl = (url: string) => {
  const rawUrl = String(url || '').trim();
  if (!rawUrl) return '';

  if (/^https?:\/\//i.test(rawUrl) || rawUrl.startsWith('data:')) {
    return rawUrl;
  }

  // DeerFlow 的相对 artifact 链接由后端根据部署配置改写为浏览器可访问地址。
  // 前端不拼接固定主机名或端口，避免不同部署环境误连到其他实例。
  if (rawUrl.startsWith('/api/')) {
    return rawUrl;
  }

  return rawUrl;
};

const getCleanUrl = (url: string) => normalizeArtifactUrl(trimUrlTail(String(url || '').trim()).href);

const isImageUrl = (url?: string) => {
  const cleanUrl = getCleanUrl(String(url || ''));
  return /\.(png|jpe?g|gif|webp|svg|bmp)(\?|#|$)/i.test(cleanUrl);
};

const isDownloadUrl = (text: string) => {
  const rawText = text.trim();
  return /^(https?:\/\/|\/api\/)[^\s]+$/.test(rawText);
};

const AuthenticatedArtifactImage = ({ url, alt, onPreview }: {
  url: string;
  alt: string;
  onPreview?: () => void;
}) => {
  const [source, setSource] = useState('');
  const [failed, setFailed] = useState(false);
  const authorization = getPlatformAuthorization();
  const conversationId = getDialogIdFromSearch(window.location.search);

  useEffect(() => {
    const controller = new AbortController();
    let objectUrl = '';
    if (!conversationId || !authorization) {
      setFailed(true);
      return;
    }
    void fetch(buildDeerFlowDownloadUrl(url, conversationId, window.location.origin), {
      headers: { Authorization: authorization },
      signal: controller.signal,
    }).then(async (response) => {
      if (!response.ok) throw new Error('图片读取失败');
      const blob = await response.blob();
      if (controller.signal.aborted) return;
      objectUrl = URL.createObjectURL(blob);
      setSource(objectUrl);
    }).catch(() => {
      if (!controller.signal.aborted) setFailed(true);
    });
    return () => {
      controller.abort();
      if (objectUrl) URL.revokeObjectURL(objectUrl);
    };
  }, [url, conversationId, authorization]);

  return source
    ? <img className={styles.markdownImage} src={source} alt={alt} onClick={onPreview} />
    : <span>{failed ? '图片暂不可用，请通过文件卡片重试' : '正在加载图片…'}</span>;
};

const renderReportImage = (src: string, alt?: string, key?: string | number, options: RenderCityBrainContentOptions = {}) => {
  const { href, tail } = trimUrlTail(src);
  const imageUrl = normalizeArtifactUrl(href);
  const caption = (alt || '').trim();
  const shouldShowCaption = Boolean(caption && !/^original$/i.test(caption));
  if (isDeerFlowArtifactUrl(imageUrl, window.location.origin)) {
    return (
      <span className={styles.markdownImageBlock} key={key ?? imageUrl}>
        <AuthenticatedArtifactImage
          url={imageUrl}
          alt={caption || '图片'}
          onPreview={() => options.onDownloadFile?.({ url: imageUrl, name: getFileNameFromUrl(imageUrl) })}
        />
        {shouldShowCaption && <span className={styles.markdownImageCaption}>{caption}</span>}
        {tail}
      </span>
    );
  }

  return (
    <span className={styles.markdownImageBlock} key={key ?? imageUrl}>
      <a
        href={imageUrl}
        target="_blank"
        rel="noopener noreferrer"
        className={styles.markdownImageLink}
        title={imageUrl}
        onClick={(event) => {
          event.stopPropagation();
        }}
      >
        <img className={styles.markdownImage} src={imageUrl} alt={caption || '图片'} loading="lazy" />
      </a>
      {shouldShowCaption && <span className={styles.markdownImageCaption}>{caption}</span>}
      {tail}
    </span>
  );
};

const parseMarkdownLink = (text: string) => {
  const match = text.match(/^(!?)\[([^\]]*)\]\(([^)]+)\)$/);
  if (!match) return null;

  const rawHref = match[3].trim().replace(/^<|>$/g, '').split(/\s+/)[0];

  return {
    isExplicitImage: Boolean(match[1]),
    label: match[2] || '',
    href: rawHref,
  };
};

const renderDownloadLink = (href: string, label?: string, key?: string | number, options: RenderCityBrainContentOptions = {}) => {
  const { href: cleanHref, tail } = trimUrlTail(href);
  const normalizedHref = normalizeArtifactUrl(cleanHref);
  const displayText = label?.trim() || getFileNameFromUrl(normalizedHref);

  return (
    <span key={key ?? normalizedHref}>
      <a
        href={normalizedHref}
        target="_blank"
        rel="noopener noreferrer"
        title={normalizedHref}
        className={styles.downloadLink}
        onClick={(event) => {
          event.stopPropagation();
          if (isDeerFlowArtifactUrl(normalizedHref, window.location.origin)) {
            event.preventDefault();
            options.onDownloadFile?.({ url: normalizedHref, name: displayText });
          }
        }}
      >
        {displayText}
      </a>
      {tail}
    </span>
  );
};

const renderInlineText = (line: string, keyPrefix: string, options: RenderCityBrainContentOptions = {}) => {
  const tokenReg = /(!?\[[^\]]*?\]\([^)]+?\)|https?:\/\/[^\s]+|\/api\/threads\/[^\s]+|`[^`]+`|\*\*[^*]+\*\*)/g;
  const parts = line.split(tokenReg);

  return parts.map((part, index) => {
    if (!part) return null;

    const markdownLink = parseMarkdownLink(part);
    if (markdownLink) {
      if (markdownLink.isExplicitImage || isImageUrl(markdownLink.href)) {
        return renderReportImage(markdownLink.href, markdownLink.label, `${keyPrefix}-image-link-${index}`, options);
      }

      return renderDownloadLink(markdownLink.href, markdownLink.label, `${keyPrefix}-md-link-${index}`, options);
    }

    if (isDownloadUrl(part)) {
      if (isImageUrl(part)) {
        return renderReportImage(part, undefined, `${keyPrefix}-image-url-${index}`, options);
      }

      return renderDownloadLink(part, undefined, `${keyPrefix}-url-${index}`, options);
    }

    if (/^`[^`]+`$/.test(part)) {
      return (
        <code key={`${keyPrefix}-code-${index}`} className={styles.inlineCode}>
          {part.slice(1, -1)}
        </code>
      );
    }

    if (/^\*\*[^*]+\*\*$/.test(part)) {
      return <strong key={`${keyPrefix}-strong-${index}`}>{part.slice(2, -2)}</strong>;
    }

    return <span key={`${keyPrefix}-text-${index}`}>{part}</span>;
  });
};

const renderImageOnlyLine = (line: string, key: string, options: RenderCityBrainContentOptions = {}) => {
  const trimmed = line.trim();

  const markdownLink = parseMarkdownLink(trimmed);
  if (markdownLink && (markdownLink.isExplicitImage || isImageUrl(markdownLink.href))) {
    return renderReportImage(markdownLink.href, markdownLink.label, key, options);
  }

  const htmlImageMatch = trimmed.match(/<img[^>]+src=["']([^"']+)["'][^>]*>/i);
  if (htmlImageMatch && isImageUrl(htmlImageMatch[1])) {
    return renderReportImage(htmlImageMatch[1], '图片', key, options);
  }

  if (isDownloadUrl(trimmed) && isImageUrl(trimmed)) {
    return renderReportImage(trimmed, undefined, key, options);
  }

  return null;
};


interface DownloadFileItem {
  name: string;
  url: string;
  previewUrl?: string;
}

interface UploadedAttachment {
  filename: string;
  original_filename?: string;
  size?: number;
  mime_type?: string;
  path?: string;
  virtual_path?: string;
  artifact_url?: string;
  markdown_file?: string;
  markdown_path?: string;
  markdown_virtual_path?: string;
  markdown_artifact_url?: string;
  url?: string;
  preview_url?: string;
  thread_id?: string;
}

interface FilePreviewState {
  filename: string;
  url: string;
  fileType: string;
  previewType: 'markdown' | 'csv' | 'text' | 'unsupported';
  content: string;
  truncated?: boolean;
  error?: string;
}

interface RenderCityBrainContentOptions {
  onPreviewFile?: (file: DownloadFileItem) => void;
  onDownloadFile?: (file: DownloadFileItem) => void;
}

interface ContentSection {
  type: 'markdown' | 'files';
  text?: string;
  title?: string;
  files?: DownloadFileItem[];
}

const parseCityBrainContentSections = (content?: string): ContentSection[] => {
  const text = String(content || '');
  const lines = text.split('\n');
  const sections: ContentSection[] = [];
  const markdownBuffer: string[] = [];

  const flushMarkdown = () => {
    const markdownText = markdownBuffer.join('\n').trimEnd();
    if (markdownText) {
      sections.push({ type: 'markdown', text: markdownText });
    }
    markdownBuffer.length = 0;
  };

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    const isFileBlockStart = /已生成结果文件/.test(line);

    if (!isFileBlockStart) {
      markdownBuffer.push(line);
      continue;
    }

    const files: DownloadFileItem[] = [];
    const title = line.trim() || '已生成结果文件：';
    let cursor = index + 1;

    while (cursor < lines.length) {
      const currentLine = lines[cursor];
      const currentTrimmed = currentLine.trim();
      const bulletMatch = currentTrimmed.match(/^[•*-]\s*(.+?)\s*$/);
      const currentIsUrl = isDownloadUrl(currentTrimmed);

      if (!currentTrimmed) {
        cursor += 1;
        continue;
      }

      if (bulletMatch) {
        const possibleFileName = bulletMatch[1].trim();
        const nextTrimmed = (lines[cursor + 1] || '').trim();
        const inlineUrlMatch = possibleFileName.match(/(https?:\/\/[^\s]+)$/);

        if (isDownloadUrl(nextTrimmed)) {
          files.push({ name: possibleFileName, url: nextTrimmed });
          cursor += 2;
          continue;
        }

        if (inlineUrlMatch) {
          const url = inlineUrlMatch[1];
          const name = possibleFileName.replace(url, '').trim() || getFileNameFromUrl(url);
          files.push({ name, url });
          cursor += 1;
          continue;
        }
      }

      if (currentIsUrl) {
        files.push({ name: getFileNameFromUrl(currentTrimmed), url: currentTrimmed });
        cursor += 1;
        continue;
      }

      break;
    }

    if (files.length > 0) {
      flushMarkdown();
      sections.push({ type: 'files', title, files });
      index = cursor - 1;
    } else {
      markdownBuffer.push(line);
    }
  }

  flushMarkdown();
  return sections;
};

const isMarkdownTableStart = (lines: string[], index: number) => {
  const currentLine = lines[index]?.trim() || '';
  const nextLine = lines[index + 1]?.trim() || '';
  return currentLine.includes('|') && /^\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|?$/.test(nextLine);
};

const splitTableRow = (line: string) => {
  return line
    .trim()
    .replace(/^\|/, '')
    .replace(/\|$/, '')
    .split('|')
    .map((item) => item.trim());
};

const renderMarkdownTable = (lines: string[], startIndex: number, keyPrefix: string, options: RenderCityBrainContentOptions = {}) => {
  const header = splitTableRow(lines[startIndex]);
  const bodyRows: string[][] = [];
  let cursor = startIndex + 2;

  while (cursor < lines.length && lines[cursor].includes('|') && lines[cursor].trim()) {
    bodyRows.push(splitTableRow(lines[cursor]));
    cursor += 1;
  }

  return {
    nextIndex: cursor,
    node: (
      <div className={styles.markdownTableWrapper} key={`${keyPrefix}-table-${startIndex}`}>
        <table className={styles.markdownTable}>
          <thead>
            <tr>
              {header.map((cell, cellIndex) => (
                <th key={`${keyPrefix}-th-${cellIndex}`}>
                  {renderInlineText(cell, `${keyPrefix}-th-${cellIndex}`, options)}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {bodyRows.map((row, rowIndex) => (
              <tr key={`${keyPrefix}-tr-${rowIndex}`}>
                {row.map((cell, cellIndex) => (
                  <td key={`${keyPrefix}-td-${rowIndex}-${cellIndex}`}>
                    {renderInlineText(cell, `${keyPrefix}-td-${rowIndex}-${cellIndex}`, options)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    ),
  };
};

const renderMarkdownLikeText = (text?: string, options: RenderCityBrainContentOptions = {}) => {
  const lines = String(text || '').split('\n');
  const nodes: ReactNode[] = [];
  let index = 0;

  while (index < lines.length) {
    const line = lines[index];
    const trimmed = line.trim();

    if (!trimmed) {
      nodes.push(<div key={`blank-${index}`} className={styles.markdownBlankLine} />);
      index += 1;
      continue;
    }

    const imageLineNode = renderImageOnlyLine(trimmed, `image-line-${index}`, options);
    if (imageLineNode) {
      nodes.push(imageLineNode);
      index += 1;
      continue;
    }

    if (isMarkdownTableStart(lines, index)) {
      const tableResult = renderMarkdownTable(lines, index, `table-${index}`, options);
      nodes.push(tableResult.node);
      index = tableResult.nextIndex;
      continue;
    }

    const headingMatch = trimmed.match(/^(#{1,4})\s+(.+)$/);
    if (headingMatch) {
      const level = headingMatch[1].length;
      const headingContent = renderInlineText(headingMatch[2], `heading-${index}`, options);

      if (level === 1) {
        nodes.push(
          <h3 key={`heading-${index}`} className={styles.markdownHeading}>
            {headingContent}
          </h3>,
        );
      } else if (level === 2) {
        nodes.push(
          <h4 key={`heading-${index}`} className={styles.markdownHeading}>
            {headingContent}
          </h4>,
        );
      } else {
        nodes.push(
          <h5 key={`heading-${index}`} className={styles.markdownHeading}>
            {headingContent}
          </h5>,
        );
      }

      index += 1;
      continue;
    }

    const bulletMatch = trimmed.match(/^[-*•]\s+(.+)$/);
    if (bulletMatch) {
      nodes.push(
        <div key={`bullet-${index}`} className={styles.markdownListItem}>
          <span className={styles.markdownListMarker}>•</span>
          <span>{renderInlineText(bulletMatch[1], `bullet-${index}`, options)}</span>
        </div>,
      );
      index += 1;
      continue;
    }

    const orderedMatch = trimmed.match(/^(\d+)[.、]\s+(.+)$/);
    if (orderedMatch) {
      nodes.push(
        <div key={`ordered-${index}`} className={styles.markdownListItem}>
          <span className={styles.markdownListMarker}>{orderedMatch[1]}.</span>
          <span>{renderInlineText(orderedMatch[2], `ordered-${index}`, options)}</span>
        </div>,
      );
      index += 1;
      continue;
    }

    nodes.push(
      <div key={`paragraph-${index}`} className={styles.markdownParagraph}>
        {renderInlineText(line, `paragraph-${index}`, options)}
      </div>,
    );
    index += 1;
  }

  return nodes;
};

const renderFileListBlock = (
  section: ContentSection,
  sectionIndex: number,
  options: RenderCityBrainContentOptions = {},
) => {
  const files = section.files || [];

  return (
    <div className={styles.fileListBlock} key={`files-${sectionIndex}`}>
      <div className={styles.fileListTitle}>{section.title || '已生成结果文件：'}</div>
      <div className={styles.fileListGrid}>
        {files.map((file, fileIndex) => {
          const fileName = file.name || getFileNameFromUrl(file.url);
          const extension = getFileExtension(fileName);

          return (
            <div
              key={`${file.url}-${fileIndex}`}
              title={file.url}
              className={styles.fileCard}
              role="button"
              tabIndex={0}
              onClick={(event) => {
                event.stopPropagation();
                options.onPreviewFile?.(file);
              }}
              onKeyDown={(event) => {
                if (event.key === 'Enter') {
                  event.preventDefault();
                  options.onPreviewFile?.(file);
                }
              }}
            >
              <span className={styles.fileIcon}>{extension || 'FILE'}</span>
              <span className={styles.fileName}>{fileName}</span>
              <button
                type="button"
                className={styles.fileDownloadAction}
                onClick={(event) => {
                  event.stopPropagation();
                  options.onDownloadFile?.(file);
                }}
              >
                下载
              </button>
            </div>
          );
        })}
      </div>
    </div>
  );
};

const renderCityBrainContent = (
  content?: string,
  options: RenderCityBrainContentOptions = {},
) => {
  const sections = parseCityBrainContentSections(content);

  return sections.map((section, sectionIndex) => {
    if (section.type === 'files') {
      return renderFileListBlock(section, sectionIndex, options);
    }

    return (
      <div className={styles.markdownBody} key={`markdown-${sectionIndex}`}>
        {renderMarkdownLikeText(section.text, options)}
      </div>
    );
  });
};

const normalizeComplianceResult = (value: any): ComplianceResult | null => {
  if (!value || typeof value !== 'object') return null;
  const gate = String(value.gate || '');
  if (!['InputGate', 'ContextGate', 'OutputGate'].includes(gate)) return null;
  return {
    gate,
    scene: String(value.scene || '_unknown'),
    violation_types: Array.isArray(value.violation_types)
      ? value.violation_types.map(String)
      : [],
    actions: Array.isArray(value.actions) ? value.actions.map(String) : [],
    audit_ref: value.audit_ref ? String(value.audit_ref) : undefined,
    basis: Array.isArray(value.basis) ? value.basis.map(String) : [],
    final_disposition: String(value.final_disposition || 'allow'),
  };
};

const complianceResultKey = (result: ComplianceResult) => {
  return [
    result.gate,
    result.scene,
    result.audit_ref || '',
    result.violation_types.join(','),
    result.actions.join(','),
    result.final_disposition,
  ].join('|');
};

const COMPLIANCE_GATE_LABELS: Record<string, string> = {
  InputGate: '输入闸门',
  ContextGate: '上下文闸门',
  OutputGate: '输出闸门',
};

const COMPLIANCE_ACTION_LABELS: Record<string, string> = {
  allow: '放行',
  warn: '警告',
  report: '上报留痕',
  role_check: '权限核验',
  desensitize: '脱敏',
  aggregate: '聚合',
  rewrite: '改写',
  manual_review: '人工复核',
  block_storage: '阻止存储',
  refuse: '拒绝',
};

const COMPLIANCE_SCENE_LABELS: Record<string, string> = {
  self_use: '个人自用',
  internal_org: '组织内部',
  cross_org: '跨组织协作',
  public_release: '公开发布',
  research_anon: '科研匿名化',
  _unknown: '未能确认场景',
};

const COMPLIANCE_IDENTITY_LABELS: Record<string, string> = {
  ordinary_user: '个人自查用户',
  internal_operator: '同组织工作人员',
  cross_org_operator: '跨组织协作人员',
  publisher: '公开发布操作人员',
  researcher: '科研人员（匿名化数据）',
};

const COMPLIANCE_PERMISSION_LABELS: Record<string, string> = {
  analysis: '分析',
  upload: '上传',
  export: '导出',
  share: '分享',
  public_release: '公开发布',
  research: '科研',
};

const formatComplianceScene = (scene: string) => {
  const label = COMPLIANCE_SCENE_LABELS[scene] || '未识别场景';
  return `${label}（${scene || '_unknown'}）`;
};

const CityBrainComplianceMessage = ({ item }: { item: CityBrainMessage }) => {
  const result = item.complianceResult;
  if (!result) return null;

  const blocked = ['refuse', 'manual_review'].includes(result.final_disposition);
  const warning = !blocked && result.final_disposition !== 'allow';
  const dispositionLabel = COMPLIANCE_ACTION_LABELS[result.final_disposition] || result.final_disposition;

  return (
    <div
      className={classNames(styles.complianceCard, {
        [styles.complianceCardBlocked]: blocked,
        [styles.complianceCardWarning]: warning,
      })}
    >
      <div className={styles.complianceCardHeader}>
        <b>{blocked ? '合规处置提示' : '合规检查结果'}</b>
        <span className={styles.complianceDisposition}>{dispositionLabel}</span>
      </div>
      <div className={styles.complianceGrid}>
        <span>检查闸门</span>
        <b>{COMPLIANCE_GATE_LABELS[result.gate] || result.gate}</b>
        <span>矩阵场景</span>
        <b>{formatComplianceScene(result.scene)}</b>
        <span>风险类型</span>
        <b>{result.violation_types.length ? result.violation_types.join('、') : '未检出'}</b>
        <span>处置动作</span>
        <b>
          {result.actions.length
            ? result.actions.map((action) => COMPLIANCE_ACTION_LABELS[action] || action).join('、')
            : dispositionLabel}
        </b>
        <span>审计编号</span>
        <b>{result.audit_ref || '—'}</b>
      </div>
      {result.basis.length > 0 && (
        <div className={styles.complianceBasis}>
          <span>合规依据</span>
          <ul>
            {result.basis.map((basis, index) => (
              <li key={`${basis}-${index}`}>{basis}</li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
};

const CityBrainAgentMessage = ({
  item,
  onPreviewFile,
  onDownloadFile,
}: {
  item: CityBrainMessage;
  onPreviewFile?: (file: DownloadFileItem) => void;
  onDownloadFile?: (file: DownloadFileItem) => void;
}) => {
  if (item.complianceResult) {
    return <CityBrainComplianceMessage item={item} />;
  }

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(item.content || '');
      message.success('已添加到剪贴板');
    } catch (error) {
      message.error('复制失败，请手动选择复制');
    }
  };

  return (
    <div className={styles.agentMessageRow}>
      <div className={styles.agentIconWrapper}>
        <Frame />
      </div>
      <div className={styles.agentContentWrapper}>
        <Space size={8} className={styles.agentTitleRow}>
          <b>{item.agentRole || '系统'}</b>
          <Button size="small" icon={<CopyOutlined />} onClick={handleCopy} />
        </Space>
        <div
          className={classNames(styles.agentBubble, {
            [styles.complianceRefusalBubble]: item.metadata?.source_event === 'compliance_retract',
          })}
        >
          {renderCityBrainContent(item.content, { onPreviewFile, onDownloadFile })}
        </div>
      </div>
    </div>
  );
};

const CityBrainUserMessage = ({
  item,
  avatar,
}: {
  item: CityBrainMessage;
  avatar?: string;
}) => {
  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(item.content || '');
      message.success('已添加到剪贴板');
    } catch (error) {
      message.error('复制失败，请手动选择复制');
    }
  };

  return (
    <div className={styles.userMessageRow}>
      <div className={styles.userContentWrapper}>
        <Button size="small" icon={<CopyOutlined />} onClick={handleCopy} />
        <div>
          <div className={styles.userBubble}>{item.content}</div>
        </div>
      </div>
      <Avatar src={avatar || defaultPng} />
    </div>
  );
};

const renderPreviewContent = (previewFile: FilePreviewState | null) => {
  if (!previewFile) {
    return <div className={styles.previewEmpty}>请选择文件进行预览。</div>;
  }

  if (previewFile.error) {
    return <div className={styles.previewError}>{previewFile.error}</div>;
  }

  if (previewFile.previewType === 'unsupported') {
    return (
      <div className={styles.previewUnsupported}>
        <div>{previewFile.content || '该类型文件暂不支持在线预览，请下载后查看。'}</div>
      </div>
    );
  }

  if (previewFile.previewType === 'csv') {
    const rows = parseCsvRows(previewFile.content);
    const header = rows[0] || [];
    const bodyRows = rows.slice(1);

    return (
      <div className={styles.previewTableWrapper}>
        <table className={styles.previewTable}>
          <thead>
            <tr>
              {header.map((cell, index) => (
                <th key={`preview-th-${index}`}>{cell}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {bodyRows.map((row, rowIndex) => (
              <tr key={`preview-row-${rowIndex}`}>
                {header.map((_, cellIndex) => (
                  <td key={`preview-cell-${rowIndex}-${cellIndex}`}>{row[cellIndex] || ''}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
        {previewFile.truncated && (
          <div className={styles.previewHint}>内容过长，当前仅展示部分内容，请下载完整文件查看。</div>
        )}
      </div>
    );
  }

  if (previewFile.previewType === 'markdown') {
    return (
      <div className={styles.previewMarkdownBody}>
        {renderMarkdownLikeText(previewFile.content)}
        {previewFile.truncated && (
          <div className={styles.previewHint}>内容过长，当前仅展示部分内容，请下载完整文件查看。</div>
        )}
      </div>
    );
  }

  return <pre className={styles.previewTextBody}>{previewFile.content}</pre>;
};

const CityBrain = () => {
  const { data: userInfo } = useFetchUserInfo();
  const userId = String(userInfo?.id || '');
  const authorization = getPlatformAuthorization();
  const routeLocation = useLocation();
  const [dialogs, setDialogs] = useState<CityBrainDialog[]>([]);
  const [currentDialogId, setCurrentDialogId] = useState('');
  const [messages, setMessages] = useState<CityBrainMessage[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [sendLoading, setSendLoading] = useState(false);
  const [runStatus, setRunStatus] = useState<'submitting' | 'pending' | 'running' | null>(null);
  const [uploadLoading, setUploadLoading] = useState(false);
  const [uploadedAttachments, setUploadedAttachments] = useState<UploadedAttachment[]>([]);
  const [previewVisible, setPreviewVisible] = useState(false);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewFile, setPreviewFile] = useState<FilePreviewState | null>(null);
  const [activatedDialogId, setActivatedDialogId] = useState('');
  const [renameVisible, setRenameVisible] = useState(false);
  const [renamingDialogId, setRenamingDialogId] = useState('');
  const [renameValue, setRenameValue] = useState('');
  const [identityPanel, setIdentityPanel] = useState<IdentityPanelState>({
    status: 'loading',
  });
  const messageListRef = useRef<HTMLDivElement>(null);
  const eventSourceRef = useRef<SseClient | null>(null);
  const uploadInputRef = useRef<HTMLInputElement>(null);
  const activeSessionRef = useRef(true);
  const storageUserId = identityPanel.status === 'ready' && identityPanel.identity
    ? JSON.stringify([identityPanel.identity.org_id, identityPanel.identity.user_id])
    : '';
  const isCurrentSession = () =>
    activeSessionRef.current && getPlatformAuthorization() === authorization;

  useEffect(() => {
    if (!userId || !authorization) {
      setIdentityPanel({ status: 'loading' });
      return;
    }

    const controller = new AbortController();
    setIdentityPanel({ status: 'loading' });
    fetch('/query/deerflow/identity', {
      headers: { Authorization: authorization },
      signal: controller.signal,
    })
      .then(async (response) => {
        const result = await response.json();
        if (
          !response.ok || result?.code !== 0 || !result?.data ||
          String(result.data.user_id) !== userId || !result.data.org_id
        ) {
          throw new Error('authenticated identity unavailable');
        }
        if (!isCurrentSession()) return;
        setIdentityPanel({ status: 'ready', identity: result.data });
      })
      .catch(() => {
        if (!controller.signal.aborted && isCurrentSession()) {
          setIdentityPanel({ status: 'error' });
        }
      });

    return () => controller.abort();
  }, [userId, authorization]);

  const currentDialog = useMemo(
    () => dialogs.find((item) => item.dialog_id === currentDialogId),
    [dialogs, currentDialogId],
  );

  const hasPlan = useMemo(() => {
    return messages.some((item) => item.agentRole === '规划智能体');
  }, [messages]);

  const persistMessages = (dialogId: string, nextMessages: CityBrainMessage[]) => {
    if (!isCurrentSession() || !storageUserId || !isCityBrainDialogId(dialogId)) return;
    const messageMap = loadCityBrainMessageMap(storageUserId);
    messageMap[dialogId] = nextMessages;
    saveCityBrainMessageMap(storageUserId, messageMap);
  };

  const updateDialogs = (updater: (prev: CityBrainDialog[]) => CityBrainDialog[]) => {
    if (!isCurrentSession()) return;
    setDialogs((prev) => {
      const next = updater(prev)
        .filter((item) => isCityBrainDialogId(item.dialog_id))
        .sort((a, b) => (b.updated_at || 0) - (a.updated_at || 0));
      saveCityBrainDialogs(storageUserId, next);
      return next;
    });
  };

  const appendMessage = (dialogId: string, item: CityBrainMessage) => {
    if (!isCityBrainDialogId(dialogId)) return;
    setMessages((prev) => {
      const next = [...prev, item];
      persistMessages(dialogId, next);
      return next;
    });
  };

  const touchDialog = (dialogId: string, finished?: boolean) => {
    if (!isCityBrainDialogId(dialogId)) return;
    updateDialogs((prev) =>
      prev.map((item) =>
        item.dialog_id === dialogId
          ? {
              ...item,
              updated_at: Date.now(),
              finished: typeof finished === 'boolean' ? finished : item.finished,
            }
          : item,
      ),
    );
  };


  const rememberDeerFlowThreadId = (dialogId: string, threadId?: string) => {
    if (!isCityBrainDialogId(dialogId) || !isDeerFlowUuidThreadId(threadId)) return;
    updateDialogs((prev) =>
      prev.map((item) => {
        if (item.dialog_id !== dialogId) return item;
        if (item.deerflow_thread_id && item.deerflow_thread_id !== threadId) {
          console.warn(
            '[city-brain] ignore unexpected DeerFlow thread switch',
            item.deerflow_thread_id,
            threadId,
          );
          return item;
        }
        return {
          ...item,
          deerflow_thread_id: String(threadId),
          updated_at: Date.now(),
        };
      }),
    );
  };

  useEffect(() => {
    if (!storageUserId) {
      setDialogs([]);
      setCurrentDialogId('');
      setMessages([]);
      return;
    }

    const storedDialogs = loadCityBrainDialogs(storageUserId);
    const urlDialogId = getDialogIdFromSearch(routeLocation.search || '');
    const messageMap = loadCityBrainMessageMap(storageUserId);

    setDialogs(storedDialogs);

    if (urlDialogId && !isCityBrainDialogId(urlDialogId)) {
      setCurrentDialogId('');
      setMessages([]);
      setInputValue('');
      history.replace('/city-brain');
      return;
    }

    if (urlDialogId && storedDialogs.some((item) => item.dialog_id === urlDialogId)) {
      setCurrentDialogId(urlDialogId);
      setMessages(messageMap[urlDialogId] || []);
    } else {
      setCurrentDialogId('');
      setMessages([]);
      setInputValue('');
      if (urlDialogId) {
        history.replace('/city-brain');
      }
    }
  }, [routeLocation.search, storageUserId]);

  useEffect(() => {
    activeSessionRef.current = true;
    return () => {
      activeSessionRef.current = false;
      eventSourceRef.current?.close();
      eventSourceRef.current = null;
    };
  }, []);

  useEffect(() => {
    const scrollContainer = messageListRef.current;
    if (!scrollContainer) return;
    scrollContainer.scrollTo({ top: scrollContainer.scrollHeight });
  }, [messages]);

  const handleCreateDialog = async () => {
    if (!storageUserId || !isCurrentSession()) {
      message.warning('请等待登录身份验证完成后重试');
      return;
    }
    if (sendLoading) {
      message.warning('当前有请求正在处理，请稍后');
      return;
    }

    const now = Date.now();
    const dialogId = createCityBrainDialogId();
    const dialogName = formatDialogName(now);
    try {
      const response = await fetch('/query/deerflow/dialog', {
        method: 'POST',
        headers: {
          Authorization: authorization,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ conversation_id: dialogId, name: dialogName }),
      });
      const result = await response.json();
      if (!isCurrentSession()) return;
      if (!response.ok || result.code !== 0) {
        throw new Error(result.message || '会话创建失败');
      }
    } catch (error: any) {
      if (!isCurrentSession()) return;
      message.error(error?.message || '会话创建失败');
      return;
    }

    const newDialog: CityBrainDialog = {
      dialog_id: dialogId,
      name: dialogName,
      created_at: now,
      updated_at: now,
      finished: true,
    };

    updateDialogs((prev) => [newDialog, ...prev]);
    const messageMap = loadCityBrainMessageMap(storageUserId);
    messageMap[dialogId] = [];
    saveCityBrainMessageMap(storageUserId, messageMap);

    setCurrentDialogId(dialogId);
    setMessages([]);
    setInputValue('');
    setUploadedAttachments([]);
    history.push(`/city-brain?dialogId=${dialogId}`);
    message.success('会话创建成功');
  };

  const handleSelectDialog = (dialogId: string) => {
    if (!isCityBrainDialogId(dialogId)) {
      history.replace('/city-brain');
      return;
    }

    if (sendLoading && dialogId !== currentDialogId) {
      message.warning('当前有请求正在处理，请稍后再切换会话');
      return;
    }

    const messageMap = loadCityBrainMessageMap(storageUserId);
    setCurrentDialogId(dialogId);
    setMessages(messageMap[dialogId] || []);
    setInputValue('');
    setUploadedAttachments([]);
    history.push(`/city-brain?dialogId=${dialogId}`);
  };

  const removeCityBrainDialog = (dialogId: string) => {
    if (!isCityBrainDialogId(dialogId)) return;

    updateDialogs((prev) => prev.filter((item) => item.dialog_id !== dialogId));
    const messageMap = loadCityBrainMessageMap(storageUserId);
    delete messageMap[dialogId];
    saveCityBrainMessageMap(storageUserId, messageMap);

    if (dialogId === currentDialogId) {
      eventSourceRef.current?.close();
      eventSourceRef.current = null;
      setSendLoading(false);
      setCurrentDialogId('');
      setMessages([]);
      setInputValue('');
      history.push('/city-brain');
    }
  };

  const handleDeleteDialog = (dialog: CityBrainDialog) => {
    if (sendLoading && dialog.dialog_id === currentDialogId) {
      message.warning('当前会话正在处理请求，暂不能删除');
      return;
    }

    const dialogName = sanitizeDialogName(dialog.name) || '未命名会话';
    Modal.confirm({
      title: `确认删除会话 “${dialogName}” 吗?`,
      okText: '是',
      cancelText: '否',
      keyboard: false,
      okButtonProps: {
        danger: true,
        autoFocus: false,
        onKeyDown: (event) => {
          if (event.key === 'Enter') {
            event.preventDefault();
          }
        },
      },
      cancelButtonProps: {
        onKeyDown: (event) => {
          if (event.key === 'Enter') {
            event.preventDefault();
          }
        },
      },
      onOk: () => removeCityBrainDialog(dialog.dialog_id),
    });
  };

  const handleOpenRenameModal = (dialog: CityBrainDialog) => {
    setRenamingDialogId(dialog.dialog_id);
    setRenameValue(sanitizeDialogName(dialog.name) || dialog.name || '');
    setRenameVisible(true);
  };

  const handleRenameOk = () => {
    const nextName = renameValue.trim();
    if (!renamingDialogId) {
      setRenameVisible(false);
      return;
    }
    if (!nextName) {
      message.warning('请输入会话名称');
      return;
    }

    updateDialogs((prev) =>
      prev.map((item) =>
        item.dialog_id === renamingDialogId
          ? {
              ...item,
              name: nextName,
              updated_at: Date.now(),
            }
          : item,
      ),
    );
    setRenameVisible(false);
    setRenamingDialogId('');
    setRenameValue('');
    message.success('重命名成功');
  };

  const handleRenameCancel = () => {
    setRenameVisible(false);
    setRenamingDialogId('');
    setRenameValue('');
  };

  const buildDialogMenuItems = (dialog: CityBrainDialog): MenuProps['items'] => {
    return [
      {
        key: 'rename',
        onClick: ({ domEvent }) => {
          domEvent.preventDefault();
          domEvent.stopPropagation();
          handleOpenRenameModal(dialog);
        },
        label: (
          <Space>
            <EditOutlined />
            重命名
          </Space>
        ),
      },
      {
        key: 'delete',
        onClick: ({ domEvent }) => {
          domEvent.preventDefault();
          domEvent.stopPropagation();
          handleDeleteDialog(dialog);
        },
        label: (
          <Space>
            <DeleteOutlined />
            删除
          </Space>
        ),
      },
    ];
  };

  const handleUploadButtonClick = () => {
    if (!currentDialogId) {
      message.warning('请先新建城市超脑平台会话');
      return;
    }
    if (sendLoading || uploadLoading) {
      message.warning('当前有请求正在处理，请稍后');
      return;
    }
    uploadInputRef.current?.click();
  };

  const handleUploadFiles = async (event: ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(event.target.files || []);
    event.target.value = '';

    if (!files.length || !storageUserId || !isCurrentSession()) return;
    if (!currentDialogId) {
      message.warning('请先新建城市超脑平台会话');
      return;
    }

    const knownThreadId = getKnownDeerFlowThreadId(currentDialog, messages, uploadedAttachments);
    const formData = new FormData();
    files.forEach((file) => formData.append('files', file));
    formData.append('conversation_id', currentDialogId);
    if (knownThreadId) {
      formData.append('thread_id', knownThreadId);
    }

    setUploadLoading(true);
    try {
      const params = new URLSearchParams({
        conversation_id: currentDialogId,
      });
      if (knownThreadId) {
        params.set('thread_id', knownThreadId);
      }
      const response = await fetch(`/query/deerflow/upload?${params.toString()}`, {
        method: 'POST',
        headers: { Authorization: authorization },
        body: formData,
      });
      const result = await response.json();
      if (!isCurrentSession()) return;
      if (!response.ok || result.code !== 0) {
        throw new Error(result.message || '文件上传失败');
      }

      const nextFiles = (result.data?.files || []) as UploadedAttachment[];
      if (!nextFiles.length) {
        throw new Error('上传接口未返回有效文件信息');
      }

      const responseThreadId = result.data?.thread_id || nextFiles.find((item) => item.thread_id)?.thread_id || knownThreadId;
      if (isDeerFlowUuidThreadId(responseThreadId)) {
        rememberDeerFlowThreadId(currentDialogId, responseThreadId);
      }

      const normalizedNextFiles = nextFiles.map((item) => ({
        ...item,
        thread_id: item.thread_id || responseThreadId || knownThreadId || '',
      }));

      setUploadedAttachments((prev) => {
        const map = new Map<string, UploadedAttachment>();
        [...prev, ...normalizedNextFiles].forEach((item) => {
          const key = item.virtual_path || item.artifact_url || item.url || item.filename;
          if (key) map.set(key, item);
        });
        return Array.from(map.values());
      });
      message.success(`已上传 ${nextFiles.length} 个文件`);
    } catch (error: any) {
      if (!isCurrentSession()) return;
      console.error('[city-brain] upload failed:', error);
      message.error(error?.message || '文件上传失败');
    } finally {
      if (isCurrentSession()) setUploadLoading(false);
    }
  };

  const handleRemoveAttachment = (file: UploadedAttachment) => {
    const key = file.virtual_path || file.artifact_url || file.url || file.filename;
    setUploadedAttachments((prev) =>
      prev.filter((item) => (item.virtual_path || item.artifact_url || item.url || item.filename) !== key),
    );
  };

  const handleDownloadFile = async (url: string, filename: string) => {
    if (!url || !storageUserId || !isCurrentSession()) return;

    try {
      const downloadUrl = buildDeerFlowDownloadUrl(url, currentDialogId, window.location.origin);
      const response = await fetch(downloadUrl, {
        headers: { Authorization: authorization },
      });
      if (!response.ok) {
        throw new Error(`文件下载失败（HTTP ${response.status}）`);
      }
      const blob = await response.blob();
      if (!isCurrentSession()) return;
      const objectUrl = URL.createObjectURL(blob);
      const anchor = document.createElement('a');
      anchor.href = objectUrl;
      anchor.download = filename || getFileNameFromUrl(url);
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      URL.revokeObjectURL(objectUrl);
    } catch (error: any) {
      if (!isCurrentSession()) return;
      message.error(error?.message || '文件下载失败');
    }
  };

  const handleDownloadFileItem = (file: DownloadFileItem) => {
    void handleDownloadFile(file.url, file.name || getFileNameFromUrl(file.url));
  };

  const handlePreviewFile = async (file: DownloadFileItem) => {
    if (!storageUserId || !isCurrentSession()) return;
    const fileName = file.name || getFileNameFromUrl(file.url);
    const previewUrl = file.previewUrl || file.url;
    const fileType = getFileExtension(previewUrl || fileName).toLowerCase();

    setPreviewVisible(true);
    setPreviewLoading(true);
    setPreviewFile({
      filename: fileName,
      url: file.url,
      fileType,
      previewType: 'text',
      content: '',
    });

    if (['xlsx', 'xls'].includes(fileType)) {
      setPreviewFile({
        filename: fileName,
        url: file.url,
        fileType,
        previewType: 'unsupported',
        content: 'Excel 文件暂不支持在线预览，请下载后查看。',
      });
      setPreviewLoading(false);
      return;
    }

    try {
      const params = new URLSearchParams({
        url: previewUrl,
        filename: fileName,
        conversation_id: currentDialogId,
      });
      const response = await fetch(`/query/deerflow/file/preview?${params.toString()}`, {
        headers: { Authorization: authorization },
      });
      const result = await response.json();
      if (!isCurrentSession()) return;
      if (!response.ok || result.code !== 0) {
        throw new Error(result.message || '文件预览失败');
      }

      const data = result.data || {};
      setPreviewFile({
        filename: data.filename || fileName,
        url: data.url || file.url,
        fileType: data.file_type || fileType,
        previewType: data.preview_type || 'text',
        content: data.content || '',
        truncated: data.truncated,
      });
    } catch (error: any) {
      if (!isCurrentSession()) return;
      console.error('[city-brain] preview failed:', error);
      setPreviewFile({
        filename: fileName,
        url: file.url,
        fileType,
        previewType: 'unsupported',
        content: '',
        error: error?.message || '文件预览失败，请下载后查看。',
      });
    } finally {
      if (isCurrentSession()) setPreviewLoading(false);
    }
  };

  const handleSend = () => {
    if (!storageUserId || !isCurrentSession()) {
      message.warning('请等待登录身份验证完成后重试');
      return;
    }
    const query = inputValue.trim();
    if (!currentDialogId) {
      message.warning('请先新建城市超脑平台会话');
      return;
    }
    if (!isCityBrainDialogId(currentDialogId)) {
      message.warning('当前会话不属于城市超脑平台，请重新新建会话');
      history.replace('/city-brain');
      return;
    }
    if (!query) {
      message.warning('请输入消息内容');
      return;
    }
    if (query.length > 500) {
      message.warning('输入内容已超过500字，建议精简后发送');
      return;
    }
    if (sendLoading) {
      message.warning('当前有请求正在处理，请稍后');
      return;
    }

    const activeDialogId = currentDialogId;
    const activeAttachments = uploadedAttachments;
    const activeThreadId = getKnownDeerFlowThreadId(currentDialog, messages, activeAttachments);
    const orderBase = messages.length;
    const attachmentNames = activeAttachments
      .map((item) => item.filename || item.original_filename)
      .filter(Boolean);
    const userMessage: CityBrainMessage = {
      id: `user-${Date.now()}`,
      role: 'user',
      content: attachmentNames.length
        ? `${query}\n\n已上传文件：\n${attachmentNames.map((name) => `• ${name}`).join('\\n')}`
        : query,
      timestamp: Date.now(),
      order_id: orderBase,
    };

    appendMessage(activeDialogId, userMessage);
    touchDialog(activeDialogId, false);
    setInputValue('');
    setUploadedAttachments([]);
    setSendLoading(true);
    setRunStatus('submitting');

    eventSourceRef.current?.close();
    const sseUrl = buildDeerFlowSseUrl(
      query,
      activeDialogId,
      activeAttachments,
      activeThreadId,
    );
    const eventSource = new SseClient(sseUrl, {
      Authorization: authorization,
    });
    eventSourceRef.current = eventSource;

    const upsertComplianceResults = (rawResults: any[]) => {
      const normalizedResults = rawResults
        .map(normalizeComplianceResult)
        .filter((item): item is ComplianceResult => Boolean(item));
      if (!normalizedResults.length) return;

      setMessages((prev) => {
        let next = [...prev];
        const shouldRetract = normalizedResults.some(
          (result) =>
            result.gate === 'OutputGate' &&
            ['refuse', 'manual_review'].includes(result.final_disposition),
        );

        // OutputGate 明确拒绝时，撤下本轮可能已经提前展示的最终回答；
        // DeerFlow 随后发送的 compliance_retract 安全提示仍会正常展示。
        if (shouldRetract) {
          next = next.filter(
            (item) =>
              !(
                item.role === 'agent' &&
                item.timestamp >= userMessage.timestamp &&
                item.agentRole === '反馈智能体' &&
                !item.complianceResult &&
                item.metadata?.source_event !== 'compliance_retract'
              ),
          );
        }

        const existingKeys = new Set(
          next
            .map((item) => item.complianceResult)
            .filter((item): item is ComplianceResult => Boolean(item))
            .map(complianceResultKey),
        );

        normalizedResults.forEach((result, index) => {
          const key = complianceResultKey(result);
          if (existingKeys.has(key)) return;
          existingKeys.add(key);
          const timestamp = Date.now() + index;
          next.push({
            id: `compliance-${result.audit_ref || timestamp}-${result.gate}`,
            role: 'agent',
            agentRole: '合规检测',
            content: `${COMPLIANCE_GATE_LABELS[result.gate] || result.gate}：${result.final_disposition}`,
            timestamp,
            order_id: timestamp,
            complianceResult: result,
            metadata: { source: 'deerflow', source_event: 'compliance' },
          });
        });

        persistMessages(activeDialogId, next);
        return next;
      });
      touchDialog(activeDialogId, false);
    };

    eventSource.connect((eventData) => {
      if (!isCurrentSession()) return;
      const rawData = typeof eventData === 'string' ? eventData : eventData?.data;
      if (!rawData) return;

      let parsedData: any;
      try {
        parsedData = JSON.parse(rawData);
      } catch (error) {
        console.error('[city-brain] SSE parse error:', rawData, error);
        return;
      }

      if (parsedData.type === '[DONE]') {
        if (Array.isArray(parsedData.compliance_results)) {
          upsertComplianceResults(parsedData.compliance_results);
        }
        eventSource.close();
        eventSourceRef.current = null;
        setSendLoading(false);
        setRunStatus(null);
        touchDialog(activeDialogId, true);
        return;
      }

      const reportedStatus = parsedData.status || parsedData.data?.status;
      if (reportedStatus === 'pending' || reportedStatus === 'queued') {
        setRunStatus('pending');
      } else if (reportedStatus === 'running') {
        setRunStatus('running');
      }

      if (parsedData.type === 'compliance') {
        upsertComplianceResults([
          parsedData.compliance_result || parsedData.data || parsedData,
        ]);
        return;
      }

      const parsedThreadId =
        parsedData.thread_id ||
        parsedData.metadata?.thread_id ||
        parsedData.data?.thread_id ||
        extractDeerFlowThreadIdFromText(parsedData.content);
      if (isDeerFlowUuidThreadId(parsedThreadId)) {
        rememberDeerFlowThreadId(activeDialogId, parsedThreadId);
      }

      const agentRole = parsedData.role || parsedData.type || '系统';
      const content = normalizeContent(parsedData.content);
      if (parsedData.type === 'error') {
        setRunStatus(null);
        message.error(content || '任务未能完成，请稍后重试');
      }
      const now = Date.now();
      const agentMessage: CityBrainMessage = {
        id: `agent-${now}-${Math.random().toString(36).slice(2, 9)}`,
        role: 'agent',
        agentRole,
        content,
        timestamp: now,
        order_id: now,
        scene: parsedData.scene_name || '',
        plan: agentRole === '规划智能体' ? parsedData.content : undefined,
        metadata: parsedData.metadata || {},
      };

      setMessages((prev) => {
        const next = [...prev];
        const lastMessage = next[next.length - 1];
        const shouldInsertStepFeedback =
          agentRole === '执行智能体' &&
          lastMessage?.role === 'agent' &&
          lastMessage.agentRole === '执行智能体';

        if (shouldInsertStepFeedback) {
          next.push({
            id: `feedback-step-${now}-${Math.random().toString(36).slice(2, 9)}`,
            role: 'agent',
            agentRole: '反馈智能体',
            content: CITY_BRAIN_STEP_FEEDBACK_CONTENT,
            timestamp: now,
            order_id: now,
          });
        }

        next.push(agentMessage);
        persistMessages(activeDialogId, next);
        return next;
      });
      touchDialog(activeDialogId, false);
    }, (error) => {
      if (!isCurrentSession()) return;
      console.error('[city-brain] SSE error:', error);
      eventSource.close();
      eventSourceRef.current = null;
      setSendLoading(false);
      setRunStatus(null);
      touchDialog(activeDialogId, true);
      message.error(error?.message || '城市超脑平台请求失败，请稍后重试');
    });
  };

  const handleSavePlan = () => {
    if (!hasPlan) {
      message.warning('当前会话暂无规划内容，无法保存方案');
      return;
    }
    message.info('城市超脑平台方案保存功能后续可接入独立后端接口');
  };

  return (
    <Flex className={styles.chatWrapper}>
      <div className={styles.boxWapper}>
        <Flex className={styles.chatAppWrapper}>
          <Flex flex={1} vertical>
            <Button
              type="primary"
              onClick={handleCreateDialog}
              style={{ height: '0.4rem' }}
            >
              新建会话
            </Button>
            <Divider type="horizontal" style={{ margin: '0.16rem' }} />
            <Flex className={styles.chatAppContent} vertical gap={10}>
              {dialogs.map((dialog) => (
                <Card
                  key={dialog.dialog_id}
                  hoverable
                  className={classNames(styles.chatAppCard, {
                    [styles.chatAppCardSelected]:
                      currentDialogId === dialog.dialog_id,
                  })}
                  onMouseEnter={() => setActivatedDialogId(dialog.dialog_id)}
                  onMouseLeave={() => setActivatedDialogId('')}
                  onClick={() => handleSelectDialog(dialog.dialog_id)}
                >
                  <Flex justify="space-between" align="center">
                    <Space size={15}>
                      <Avatar src={defaultPng} shape="square" />
                      <section>
                        <b>
                          <Text
                            ellipsis={{ tooltip: sanitizeDialogName(dialog.name) }}
                            style={{ width: 130 }}
                          >
                            {sanitizeDialogName(dialog.name)}
                          </Text>
                        </b>
                      </section>
                    </Space>
                    {activatedDialogId === dialog.dialog_id && (
                      <section
                        onClick={(event) => {
                          event.preventDefault();
                          event.stopPropagation();
                        }}
                      >
                        <Dropdown menu={{ items: buildDialogMenuItems(dialog) }}>
                          <ChatAppCube className={styles.cubeIcon} />
                        </Dropdown>
                      </section>
                    )}
                  </Flex>
                </Card>
              ))}
            </Flex>
          </Flex>
        </Flex>

        <div className={styles.complianceIdentityPanel}>
          <div className={styles.complianceIdentityControl}>
            <span className={styles.complianceIdentityTitle}>当前账号与合规身份</span>
          </div>
          <div className={styles.complianceIdentityGrid}>
            <span>登录账号</span>
            <b>
              {userInfo?.nickname || '—'}
              {userInfo?.email ? `（${userInfo.email}）` : ''}
            </b>
            <span>服务端身份</span>
            <b>
              {identityPanel.status === 'ready' && identityPanel.identity
                ? `${COMPLIANCE_IDENTITY_LABELS[identityPanel.identity.identity_class] || '未识别身份'}（${identityPanel.identity.identity_class}）`
                : identityPanel.status === 'error'
                  ? '身份读取失败'
                  : '读取中…'}
            </b>
            <span>OneCity 角色</span>
            <b>
              {identityPanel.status === 'ready'
                ? identityPanel.identity?.role_code || '未配置角色代码'
                : '—'}
            </b>
            <span>允许操作</span>
            <b>
              {identityPanel.status === 'ready' && identityPanel.identity?.permissions.length
                ? identityPanel.identity.permissions
                    .map((permission) => COMPLIANCE_PERMISSION_LABELS[permission] || permission)
                    .join('、')
                : identityPanel.status === 'ready'
                  ? '未配置'
                  : '—'}
            </b>
          </div>
          {identityPanel.status === 'ready' && identityPanel.identity && !identityPanel.identity.identity_mapping_configured && (
            <div className={styles.complianceIdentityNotice}>
              当前 OneCity 角色未配置显式身份映射，服务端按“个人自查用户”默认身份处理；这不代表其他合规场景已获授权。
            </div>
          )}
          {identityPanel.status === 'error' && (
            <div className={styles.complianceIdentityNotice}>
              无法读取服务端 IAM 身份。请确认当前账号已登录，且 OneCityAgent 授权桥接可用。
            </div>
          )}
          <div className={styles.complianceIdentityDescription}>
            登录认证确认账号；资源授权确认当前账号能否访问会话。合规矩阵场景由服务端结合操作、目标受众和资源归属计算，下面的检查卡展示各闸门实际命中的场景与处置动作。客户端不能自行声明身份或场景。
          </div>
        </div>
      </div>

      <Flex flex={1} className={styles.cityBrainContainer} vertical>
        <div className={styles.buttonGroup}>
          <Button
            onClick={handleSavePlan}
            className={styles.conversionButton}
            type="primary"
            size="middle"
            disabled={!hasPlan || sendLoading}
          >
            保存方案
          </Button>
        </div>

        <Flex
          flex={1}
          vertical
          className={styles.messageContainer}
          ref={messageListRef}
        >
          <Spin
            spinning={sendLoading}
            tip={runStatus === 'pending' ? '任务排队中，轮到后自动执行' : runStatus === 'running' ? '正在执行任务' : '正在提交任务'}
          >
            <div className={styles.messageList}>
              {messages.map((item) =>
                item.role === 'user' ? (
                  <CityBrainUserMessage
                    key={item.id}
                    item={item}
                    avatar={userInfo?.avatar || defaultPng}
                  />
                ) : (
                  <CityBrainAgentMessage
                    key={item.id}
                    item={item}
                    onPreviewFile={handlePreviewFile}
                    onDownloadFile={handleDownloadFileItem}
                  />
                ),
              )}
              {!currentDialog && (
                <div className={styles.emptyHint}>
                  请点击左侧“新建会话”，开始城市超脑平台对话。
                </div>
              )}
            </div>
          </Spin>
        </Flex>

        <div className={styles.inputWrapper}>
          <div className={styles.inputMainArea}>
            {uploadedAttachments.length > 0 && (
              <div className={styles.uploadedFileList}>
                {uploadedAttachments.map((file) => {
                  const fileName = file.filename || file.original_filename || '未命名文件';
                  const key = file.virtual_path || file.artifact_url || file.url || fileName;
                  return (
                    <div className={styles.uploadedFileTag} key={key} title={fileName}>
                      <span className={styles.uploadedFileIcon}>{getFileExtension(fileName)}</span>
                      <span className={styles.uploadedFileName}>{fileName}</span>
                      {file.size ? <span className={styles.uploadedFileSize}>{formatFileSize(file.size)}</span> : null}
                      <Button
                        type="text"
                        size="small"
                        icon={<DeleteOutlined />}
                        onClick={() => handleRemoveAttachment(file)}
                      />
                    </div>
                  );
                })}
              </div>
            )}
            <TextArea
              value={inputValue}
              onChange={(event) => setInputValue(event.target.value)}
              onPressEnter={(event) => {
                if (!event.shiftKey) {
                  event.preventDefault();
                  handleSend();
                }
              }}
              disabled={!currentDialogId || sendLoading}
              autoSize={{ minRows: 1, maxRows: 4 }}
              placeholder="消息概要助手..."
            />
          </div>
          <input
            ref={uploadInputRef}
            type="file"
            multiple
            style={{ display: 'none' }}
            onChange={handleUploadFiles}
          />
          <Button
            icon={<PaperClipOutlined />}
            onClick={handleUploadButtonClick}
            disabled={!currentDialogId || sendLoading}
            loading={uploadLoading}
          >
            上传
          </Button>
          <Button
            type="primary"
            onClick={handleSend}
            disabled={!currentDialogId || !inputValue.trim() || sendLoading || uploadLoading}
            loading={sendLoading}
          >
            发送
          </Button>
        </div>
      </Flex>

      <Drawer
        title={previewFile?.filename || '文件预览'}
        open={previewVisible}
        width="42%"
        onClose={() => setPreviewVisible(false)}
        extra={
          previewFile?.url ? (
            <Button
              onClick={() => void handleDownloadFile(previewFile.url, previewFile.filename)}
              type="primary"
            >
              下载
            </Button>
          ) : null
        }
      >
        <Spin spinning={previewLoading}>
          {renderPreviewContent(previewFile)}
        </Spin>
      </Drawer>

      <Modal
        title="重命名会话"
        open={renameVisible}
        onOk={handleRenameOk}
        onCancel={handleRenameCancel}
        okText="确定"
        cancelText="取消"
        destroyOnClose
      >
        <Input
          value={renameValue}
          maxLength={60}
          placeholder="请输入会话名称"
          onChange={(event) => setRenameValue(event.target.value)}
          onPressEnter={(event) => {
            event.preventDefault();
            handleRenameOk();
          }}
        />
      </Modal>
    </Flex>
  );
};

export default CityBrain;
